/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.iceberg;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMergeSink;
import com.facebook.presto.spi.ConnectorMergeTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.LocationProvider;

import javax.inject.Inject;

import java.util.Optional;

import static com.facebook.presto.iceberg.IcebergUtil.getLocationProvider;
import static com.facebook.presto.iceberg.IcebergUtil.getShallowWrappedIcebergTable;
import static com.facebook.presto.iceberg.PartitionSpecConverter.toIcebergPartitionSpec;
import static com.facebook.presto.iceberg.SchemaConverter.toIcebergSchema;
import static java.util.Objects.requireNonNull;

public class IcebergPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final PageIndexerFactory pageIndexerFactory;
    private final int maxOpenPartitions;
    private final SortParameters sortParameters;

    @Inject
    public IcebergPageSinkProvider(
            HdfsEnvironment hdfsEnvironment,
            JsonCodec<CommitTaskData> jsonCodec,
            IcebergFileWriterFactory fileWriterFactory,
            PageIndexerFactory pageIndexerFactory,
            IcebergConfig icebergConfig,
            SortParameters sortParameters)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        requireNonNull(icebergConfig, "icebergConfig is null");
        this.maxOpenPartitions = icebergConfig.getMaxPartitionsPerWriter();
        this.sortParameters = sortParameters;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, PageSinkContext pageSinkContext)
    {
        return createPageSink(session, (IcebergWritableTableHandle) outputTableHandle);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, PageSinkContext pageSinkContext)
    {
        return createPageSink(session, (IcebergWritableTableHandle) insertTableHandle);
    }

    private ConnectorPageSink createPageSink(ConnectorSession session, IcebergWritableTableHandle tableHandle)
    {
        HdfsContext hdfsContext = new HdfsContext(session, tableHandle.getSchemaName(), tableHandle.getTableName().getTableName());
        Schema schema = toIcebergSchema(tableHandle.getSchema());
        PartitionSpec partitionSpec = toIcebergPartitionSpec(tableHandle.getPartitionSpec()).toUnbound().bind(schema);
        LocationProvider locationProvider = getLocationProvider(new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName().getTableName()),
                tableHandle.getOutputPath(), tableHandle.getStorageProperties());
        Table table = getShallowWrappedIcebergTable(schema, partitionSpec, tableHandle.getStorageProperties(), Optional.empty());
        return new IcebergPageSink(
                table,
                locationProvider,
                fileWriterFactory,
                pageIndexerFactory,
                hdfsEnvironment,
                hdfsContext,
                tableHandle.getInputColumns(),
                jsonCodec,
                session,
                tableHandle.getFileFormat(),
                maxOpenPartitions,
                tableHandle.getSortOrder(),
                sortParameters);
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle)
    {
        IcebergMergeTableHandle merge = (IcebergMergeTableHandle) mergeHandle;
        IcebergWritableTableHandle tableHandle = merge.getInsertTableHandle();
        SchemaTableName schemaTableName = new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName().getTableName());
        LocationProvider locationProvider = getLocationProvider(schemaTableName, tableHandle.getOutputPath(), tableHandle.getStorageProperties());

        Schema schema = toIcebergSchema(tableHandle.getSchema());
        PartitionSpec partitionSpec = toIcebergPartitionSpec(tableHandle.getPartitionSpec()).toUnbound().bind(schema);
        // TODO #20578: In Trino, they use "Map<Integer, PartitionSpec>" instead of "PartitionSpec". Should Presto do the same?
        //  Map<Integer, PartitionSpec> partitionsSpecs = transformValues(tableHandle.getPartitionsSpecsAsJson(), json -> PartitionSpecParser.fromJson(schema, json));

        ConnectorPageSink pageSink = createPageSink(session, tableHandle);

        return new IcebergMergeSink(
                locationProvider,
                fileWriterFactory,
                hdfsEnvironment,
//                fileIoProvider, // TODO #20578: Seems that it is only necessary to support Iceberg using AVRO file format. Presto does not have that support, so it looks like we can ignore this parameter for now.
                jsonCodec,
                session,
                tableHandle.getFileFormat(),
                tableHandle.getStorageProperties(),
                schema,
                partitionSpec,
                pageSink,
                tableHandle.getInputColumns().size());
    }
}
