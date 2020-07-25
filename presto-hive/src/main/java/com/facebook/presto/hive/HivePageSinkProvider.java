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
package com.facebook.presto.hive;

import com.facebook.airlift.event.client.EventClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePageSinkMetadataProvider;
import com.facebook.presto.hive.metastore.SortingColumn;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSinkProperties;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;
import java.util.OptionalInt;
import java.util.Set;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.hive.metastore.CachingHiveMetastore.memoizeMetastore;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class HivePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final Set<HiveFileWriterFactory> fileWriterFactories;
    private final HdfsEnvironment hdfsEnvironment;
    private final PageSorter pageSorter;
    private final ExtendedHiveMetastore metastore;
    private final PageIndexerFactory pageIndexerFactory;
    private final TypeManager typeManager;
    private final int maxOpenPartitions;
    private final int maxOpenSortFiles;
    private final DataSize writerSortBufferSize;
    private final boolean immutablePartitions;
    private final LocationService locationService;
    private final ListeningExecutorService writeVerificationExecutor;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final NodeManager nodeManager;
    private final EventClient eventClient;
    private final HiveSessionProperties hiveSessionProperties;
    private final HiveWriterStats hiveWriterStats;
    private final OrcFileWriterFactory orcFileWriterFactory;
    private final long perTransactionMetastoreCacheMaximumSize;

    @Inject
    public HivePageSinkProvider(
            Set<HiveFileWriterFactory> fileWriterFactories,
            HdfsEnvironment hdfsEnvironment,
            PageSorter pageSorter,
            ExtendedHiveMetastore metastore,
            PageIndexerFactory pageIndexerFactory,
            TypeManager typeManager,
            HiveClientConfig hiveClientConfig,
            MetastoreClientConfig metastoreClientConfig,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            NodeManager nodeManager,
            EventClient eventClient,
            HiveSessionProperties hiveSessionProperties,
            HiveWriterStats hiveWriterStats,
            OrcFileWriterFactory orcFileWriterFactory)
    {
        this.fileWriterFactories = ImmutableSet.copyOf(requireNonNull(fileWriterFactories, "fileWriterFactories is null"));
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        // TODO: this metastore should not have global cache
        // As a temporary workaround, always disable cache on the workers
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.maxOpenPartitions = hiveClientConfig.getMaxPartitionsPerWriter();
        this.maxOpenSortFiles = hiveClientConfig.getMaxOpenSortFiles();
        this.writerSortBufferSize = requireNonNull(hiveClientConfig.getWriterSortBufferSize(), "writerSortBufferSize is null");
        this.immutablePartitions = hiveClientConfig.isImmutablePartitions();
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.writeVerificationExecutor = listeningDecorator(newFixedThreadPool(hiveClientConfig.getWriteValidationThreads(), daemonThreadsNamed("hive-write-validation-%s")));
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.eventClient = requireNonNull(eventClient, "eventClient is null");
        this.hiveSessionProperties = requireNonNull(hiveSessionProperties, "hiveSessionProperties is null");
        this.hiveWriterStats = requireNonNull(hiveWriterStats, "stats is null");
        this.orcFileWriterFactory = requireNonNull(orcFileWriterFactory, "orcFileWriterFactory is null");
        this.perTransactionMetastoreCacheMaximumSize = metastoreClientConfig.getPerTransactionMetastoreCacheMaximumSize();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorOutputTableHandle tableHandle, PageSinkProperties pageSinkProperties)
    {
        HiveWritableTableHandle handle = (HiveOutputTableHandle) tableHandle;
        return createPageSink(handle, true, session, pageSinkProperties.isCommitRequired());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorInsertTableHandle tableHandle, PageSinkProperties pageSinkProperties)
    {
        HiveInsertTableHandle handle = (HiveInsertTableHandle) tableHandle;
        return createPageSink(handle, false, session, pageSinkProperties.isCommitRequired());
    }

    private ConnectorPageSink createPageSink(HiveWritableTableHandle handle, boolean isCreateTable, ConnectorSession session, boolean commitRequired)
    {
        OptionalInt bucketCount = OptionalInt.empty();
        List<SortingColumn> sortedBy;

        if (handle.getBucketProperty().isPresent()) {
            bucketCount = OptionalInt.of(handle.getBucketProperty().get().getBucketCount());
            sortedBy = handle.getBucketProperty().get().getSortedBy();
        }
        else {
            sortedBy = handle.getPreferredOrderingColumns();
        }

        HiveWriterFactory writerFactory = new HiveWriterFactory(
                fileWriterFactories,
                handle.getSchemaName(),
                handle.getTableName(),
                isCreateTable,
                handle.getInputColumns(),
                handle.getTableStorageFormat(),
                handle.getPartitionStorageFormat(),
                handle.getCompressionCodec(),
                bucketCount,
                sortedBy,
                handle.getLocationHandle(),
                locationService,
                handle.getFilePrefix(),
                // The scope of metastore cache is within a single HivePageSink object
                // TODO: Extend metastore cache scope to the entire transaction
                new HivePageSinkMetadataProvider(handle.getPageSinkMetadata(), memoizeMetastore(metastore, perTransactionMetastoreCacheMaximumSize)),
                typeManager,
                hdfsEnvironment,
                pageSorter,
                writerSortBufferSize,
                maxOpenSortFiles,
                immutablePartitions,
                session,
                nodeManager,
                eventClient,
                hiveSessionProperties,
                hiveWriterStats,
                orcFileWriterFactory,
                commitRequired,
                handle.getEncryptionInformation());

        return new HivePageSink(
                writerFactory,
                handle.getInputColumns(),
                handle.getBucketProperty(),
                pageIndexerFactory,
                typeManager,
                hdfsEnvironment,
                maxOpenPartitions,
                writeVerificationExecutor,
                partitionUpdateCodec,
                session);
    }
}
