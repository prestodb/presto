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
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.io.LocationProvider;

import javax.inject.Inject;

import static com.facebook.presto.iceberg.IcebergUtil.getLocationProvider;
import static java.util.Objects.requireNonNull;

public class IcebergPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final PageIndexerFactory pageIndexerFactory;

    @Inject
    public IcebergPageSinkProvider(
            HdfsEnvironment hdfsEnvironment,
            JsonCodec<CommitTaskData> jsonCodec,
            IcebergFileWriterFactory fileWriterFactory,
            PageIndexerFactory pageIndexerFactory)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
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
        HdfsContext hdfsContext = new HdfsContext(session, tableHandle.getSchemaName(), tableHandle.getTableName());
        Schema schema = SchemaParser.fromJson(tableHandle.getSchemaAsJson());
        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, tableHandle.getPartitionSpecAsJson());
        LocationProvider locationProvider = getLocationProvider(new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName()),
                tableHandle.getOutputPath(), tableHandle.getStorageProperties());
        return new IcebergPageSink(
                schema,
                partitionSpec,
                locationProvider,
                fileWriterFactory,
                pageIndexerFactory,
                hdfsEnvironment,
                hdfsContext,
                tableHandle.getInputColumns(),
                jsonCodec,
                session,
                tableHandle.getFileFormat());
    }
}
