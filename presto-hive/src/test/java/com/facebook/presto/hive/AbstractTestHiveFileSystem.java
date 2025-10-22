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

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.hive.AbstractTestHiveClient.HiveTransaction;
import com.facebook.presto.hive.AbstractTestHiveClient.Transaction;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.datasink.OutputStreamDataSinkFactory;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePartitionMutator;
import com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.MetastoreOperationResult;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.thrift.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.HiveCluster;
import com.facebook.presto.hive.metastore.thrift.TestingHiveCluster;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastoreConfig;
import com.facebook.presto.hive.statistics.QuickStatsProvider;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingContext;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.hive.AbstractTestHiveClient.TEST_HIVE_PAGE_SINK_CONTEXT;
import static com.facebook.presto.hive.AbstractTestHiveClient.createTableProperties;
import static com.facebook.presto.hive.AbstractTestHiveClient.filterNonHiddenColumnHandles;
import static com.facebook.presto.hive.AbstractTestHiveClient.filterNonHiddenColumnMetadata;
import static com.facebook.presto.hive.AbstractTestHiveClient.getAllSplits;
import static com.facebook.presto.hive.HiveFileSystemTestUtils.getTableHandle;
import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.HiveTestUtils.DO_NOTHING_DIRECTORY_LISTER;
import static com.facebook.presto.hive.HiveTestUtils.FILTER_STATS_CALCULATOR_SERVICE;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_RESOLUTION;
import static com.facebook.presto.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static com.facebook.presto.hive.HiveTestUtils.PAGE_SORTER;
import static com.facebook.presto.hive.HiveTestUtils.ROW_EXPRESSION_SERVICE;
import static com.facebook.presto.hive.HiveTestUtils.getAllSessionProperties;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveAggregatedPageSourceFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveBatchPageSourceFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveFileWriterFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveRecordCursorProvider;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveSelectivePageSourceFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultOrcFileWriterFactory;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultS3HiveRecordCursorProvider;
import static com.facebook.presto.hive.HiveTestUtils.getTypes;
import static com.facebook.presto.hive.metastore.MetastoreOperationResult.EMPTY_RESULT;
import static com.facebook.presto.hive.metastore.NoopMetastoreCacheStats.NOOP_METASTORE_CACHE_STATS;
import static com.facebook.presto.spi.SplitContext.NON_CACHEABLE;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestHiveFileSystem
{
    protected static final HdfsContext TESTING_CONTEXT = new HdfsContext(new ConnectorIdentity("test", Optional.empty(), Optional.empty()));
    public static final SplitSchedulingContext SPLIT_SCHEDULING_CONTEXT = new SplitSchedulingContext(UNGROUPED_SCHEDULING, false, WarningCollector.NOOP);

    protected String database;
    protected SchemaTableName table;
    protected SchemaTableName temporaryCreateTable;

    protected HdfsEnvironment hdfsEnvironment;
    protected LocationService locationService;
    protected TestingHiveMetastore metastoreClient;
    protected HiveMetadataFactory metadataFactory;
    protected HiveTransactionManager transactionManager;
    protected ConnectorSplitManager splitManager;
    protected ConnectorPageSinkProvider pageSinkProvider;
    protected ConnectorPageSourceProvider pageSourceProvider;

    private ExecutorService executor;
    private HiveClientConfig config;
    private CacheConfig cacheConfig;
    private MetastoreClientConfig metastoreClientConfig;
    private ThriftHiveMetastoreConfig thriftHiveMetastoreConfig;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    protected abstract Path getBasePath();

    protected void setup(String host, int port, String databaseName, BiFunction<HiveClientConfig, MetastoreClientConfig, HdfsConfiguration> hdfsConfigurationProvider, boolean s3SelectPushdownEnabled)
    {
        database = databaseName;
        table = new SchemaTableName(database, "presto_test_external_fs");

        String random = UUID.randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
        temporaryCreateTable = new SchemaTableName(database, "tmp_presto_test_create_" + random);

        config = new HiveClientConfig().setS3SelectPushdownEnabled(s3SelectPushdownEnabled);
        cacheConfig = new CacheConfig();
        metastoreClientConfig = new MetastoreClientConfig();
        thriftHiveMetastoreConfig = new ThriftHiveMetastoreConfig();

        String proxy = System.getProperty("hive.metastore.thrift.client.socks-proxy");
        if (proxy != null) {
            metastoreClientConfig.setMetastoreSocksProxy(HostAndPort.fromString(proxy));
        }

        HiveCluster hiveCluster = new TestingHiveCluster(metastoreClientConfig, thriftHiveMetastoreConfig, host, port, new HiveCommonClientConfig());
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));
        HivePartitionManager hivePartitionManager = new HivePartitionManager(FUNCTION_AND_TYPE_MANAGER, config);

        HdfsConfiguration hdfsConfiguration = hdfsConfigurationProvider.apply(config, metastoreClientConfig);

        hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        ColumnConverterProvider columnConverterProvider = HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER;
        metastoreClient = new TestingHiveMetastore(
                new BridgingHiveMetastore(new ThriftHiveMetastore(hiveCluster, metastoreClientConfig, hdfsEnvironment), new HivePartitionMutator()),
                executor,
                metastoreClientConfig,
                getBasePath(),
                hdfsEnvironment);
        locationService = new HiveLocationService(hdfsEnvironment);
        metadataFactory = new HiveMetadataFactory(
                config,
                metastoreClientConfig,
                metastoreClient,
                hdfsEnvironment,
                hivePartitionManager,
                newDirectExecutorService(),
                FUNCTION_AND_TYPE_MANAGER,
                locationService,
                FUNCTION_RESOLUTION,
                ROW_EXPRESSION_SERVICE,
                FILTER_STATS_CALCULATOR_SERVICE,
                new TableParameterCodec(),
                HiveTestUtils.PARTITION_UPDATE_CODEC,
                HiveTestUtils.PARTITION_UPDATE_SMILE_CODEC,
                new HiveTypeTranslator(),
                new HiveStagingFileCommitter(hdfsEnvironment, listeningDecorator(executor)),
                new HiveZeroRowFileCreator(hdfsEnvironment, new OutputStreamDataSinkFactory(), listeningDecorator(executor)),
                new NodeVersion("test_version"),
                new HivePartitionObjectBuilder(),
                new HiveEncryptionInformationProvider(ImmutableList.of()),
                new HivePartitionStats(),
                columnConverterProvider,
                new QuickStatsProvider(metastoreClient, HDFS_ENVIRONMENT, DO_NOTHING_DIRECTORY_LISTER, new HiveClientConfig(), new NamenodeStats(), ImmutableList.of()),
                new HiveTableWritabilityChecker(config));

        transactionManager = new HiveTransactionManager();
        splitManager = new HiveSplitManager(
                transactionManager,
                new NamenodeStats(),
                hdfsEnvironment,
                new CachingDirectoryLister(new HadoopDirectoryLister(), new HiveClientConfig()),
                new BoundedExecutor(executor, config.getMaxSplitIteratorThreads()),
                new HiveCoercionPolicy(FUNCTION_AND_TYPE_MANAGER),
                new CounterStat(),
                config.getMaxOutstandingSplits(),
                config.getMaxOutstandingSplitsSize(),
                config.getMinPartitionBatchSize(),
                config.getMaxPartitionBatchSize(),
                config.getSplitLoaderConcurrency(),
                config.getRecursiveDirWalkerEnabled(),
                new ConfigBasedCacheQuotaRequirementProvider(cacheConfig),
                new HiveEncryptionInformationProvider(ImmutableSet.of()),
                new HivePartitionSkippabilityChecker());
        pageSinkProvider = new HivePageSinkProvider(
                getDefaultHiveFileWriterFactories(config, metastoreClientConfig),
                hdfsEnvironment,
                PAGE_SORTER,
                metastoreClient,
                new GroupByHashPageIndexerFactory(new JoinCompiler(MetadataManager.createTestMetadataManager())),
                FUNCTION_AND_TYPE_MANAGER,
                config,
                metastoreClientConfig,
                new SortingFileWriterConfig(),
                locationService,
                HiveTestUtils.PARTITION_UPDATE_CODEC,
                HiveTestUtils.PARTITION_UPDATE_SMILE_CODEC,
                new TestingNodeManager("fake-environment"),
                new HiveEventClient(),
                new HiveSessionProperties(config, new OrcFileWriterConfig(), new ParquetFileWriterConfig(), new CacheConfig()),
                new HiveWriterStats(),
                getDefaultOrcFileWriterFactory(config, metastoreClientConfig),
                columnConverterProvider);
        Set<HiveRecordCursorProvider> recordCursorProviderSet = s3SelectPushdownEnabled ?
                getDefaultS3HiveRecordCursorProvider(config, metastoreClientConfig) :
                getDefaultHiveRecordCursorProvider(config, metastoreClientConfig);
        pageSourceProvider = new HivePageSourceProvider(
                config,
                hdfsEnvironment,
                recordCursorProviderSet,
                getDefaultHiveBatchPageSourceFactories(config, metastoreClientConfig),
                getDefaultHiveSelectivePageSourceFactories(config, metastoreClientConfig),
                getDefaultHiveAggregatedPageSourceFactories(config, metastoreClientConfig),
                FUNCTION_AND_TYPE_MANAGER,
                ROW_EXPRESSION_SERVICE);
    }

    protected ConnectorSession newSession()
    {
        return new TestingConnectorSession(getAllSessionProperties(config, new HiveCommonClientConfig()));
    }

    protected Transaction newTransaction()
    {
        return new HiveTransaction(transactionManager, metadataFactory.get());
    }

    protected MaterializedResult readTable(SchemaTableName tableName)
            throws IOException
    {
        return HiveFileSystemTestUtils.readTable(tableName, transactionManager, config, metadataFactory, pageSourceProvider, splitManager);
    }

    protected MaterializedResult filterTable(SchemaTableName tableName, List<ColumnHandle> projectedColumns)
            throws IOException
    {
        return HiveFileSystemTestUtils.filterTable(tableName, projectedColumns, transactionManager, config, metadataFactory, pageSourceProvider, splitManager);
    }

    @Test
    public void testGetRecords()
            throws Exception
    {
        assertEqualsIgnoreOrder(
                readTable(table),
                MaterializedResult.resultBuilder(newSession(), BIGINT)
                        .row(70000L).row(8000L).row(300L) // test1.csv
                        .row(70000L).row(8000L).row(300L) // test1.csv.gz
                        .row(70000L).row(8000L).row(300L) // test1.csv.bz2
                        .row(70000L).row(8000L).row(300L) // test1.csv.lz4
                        .build());
    }

    @Test
    public void testGetFileStatus()
            throws Exception
    {
        Path basePath = getBasePath();
        Path tablePath = new Path(basePath, "presto_test_external_fs");
        Path filePath = new Path(tablePath, "test1.csv");
        FileSystem fs = hdfsEnvironment.getFileSystem(TESTING_CONTEXT, basePath);

        assertTrue(fs.getFileStatus(basePath).isDirectory());
        assertTrue(fs.getFileStatus(tablePath).isDirectory());
        assertFalse(fs.getFileStatus(filePath).isDirectory());
        assertFalse(fs.exists(new Path(basePath, "foo")));
    }

    @Test
    public void testRename()
            throws Exception
    {
        Path basePath = new Path(getBasePath(), UUID.randomUUID().toString());
        FileSystem fs = hdfsEnvironment.getFileSystem(TESTING_CONTEXT, basePath);
        assertFalse(fs.exists(basePath));

        // create file foo.txt
        Path path = new Path(basePath, "foo.txt");
        assertTrue(fs.createNewFile(path));
        assertTrue(fs.exists(path));

        // rename foo.txt to bar.txt when bar does not exist
        Path newPath = new Path(basePath, "bar.txt");
        assertFalse(fs.exists(newPath));
        assertTrue(fs.rename(path, newPath));
        assertFalse(fs.exists(path));
        assertTrue(fs.exists(newPath));

        // rename foo.txt to foo.txt when foo.txt does not exist
        assertFalse(fs.rename(path, path));

        // create file foo.txt and rename to existing bar.txt
        assertTrue(fs.createNewFile(path));
        assertFalse(fs.rename(path, newPath));

        // rename foo.txt to foo.txt when foo.txt exists
        assertFalse(fs.rename(path, path));

        // delete foo.txt
        assertTrue(fs.delete(path, false));
        assertFalse(fs.exists(path));

        // create directory source with file
        Path source = new Path(basePath, "source");
        assertTrue(fs.createNewFile(new Path(source, "test.txt")));

        // rename source to non-existing target
        Path target = new Path(basePath, "target");
        assertFalse(fs.exists(target));
        assertTrue(fs.rename(source, target));
        assertFalse(fs.exists(source));
        assertTrue(fs.exists(target));

        // create directory source with file
        assertTrue(fs.createNewFile(new Path(source, "test.txt")));

        // rename source to existing target
        assertTrue(fs.rename(source, target));
        assertFalse(fs.exists(source));
        target = new Path(target, "source");
        assertTrue(fs.exists(target));
        assertTrue(fs.exists(new Path(target, "test.txt")));

        // delete target
        target = new Path(basePath, "target");
        assertTrue(fs.exists(target));
        assertTrue(fs.delete(target, true));
        assertFalse(fs.exists(target));

        // cleanup
        fs.delete(basePath, true);
    }

    @Test
    public void testTableCreation()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : getSupportedHiveStorageFormats()) {
            createTable(METASTORE_CONTEXT, temporaryCreateTable, storageFormat);
            dropTable(temporaryCreateTable);
        }
    }

    protected List<HiveStorageFormat> getSupportedHiveStorageFormats()
    {
        // CSV supports only unbounded VARCHAR type, and Alpha does not support DML yet
        return Arrays.stream(HiveStorageFormat.values())
                .filter(format -> format != HiveStorageFormat.CSV && format != HiveStorageFormat.ALPHA)
                .collect(toImmutableList());
    }

    private void createTable(MetastoreContext metastoreContext, SchemaTableName tableName, HiveStorageFormat storageFormat)
            throws Exception
    {
        List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .add(ColumnMetadata.builder().setName("id").setType(BIGINT).build())
                .build();

        MaterializedResult data = MaterializedResult.resultBuilder(newSession(), BIGINT)
                .row(1L)
                .row(3L)
                .row(2L)
                .build();

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            // begin creating the table
            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, columns, createTableProperties(storageFormat));
            ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(session, tableMetadata, Optional.empty());

            // write the records
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, outputHandle, TEST_HIVE_PAGE_SINK_CONTEXT);
            sink.appendPage(data.toPage());
            Collection<Slice> fragments = getFutureValue(sink.finish());

            // commit the table
            metadata.finishCreateTable(session, outputHandle, fragments, ImmutableList.of());

            transaction.commit();

            // Hack to work around the metastore not being configured for S3 or other FS.
            // The metastore tries to validate the location when creating the
            // table, which fails without explicit configuration for file system.
            // We work around that by using a dummy location when creating the
            // table and update it here to the correct location.
            metastoreClient.updateTableLocation(
                    metastoreContext,
                    database,
                    tableName.getTableName(),
                    locationService.getTableWriteInfo(((HiveOutputTableHandle) outputHandle).getLocationHandle()).getTargetPath().toString());
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            // load the new table
            ConnectorTableHandle hiveTableHandle = getTableHandle(metadata, tableName, session);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, hiveTableHandle).values());

            // verify the metadata
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, getTableHandle(metadata, tableName, session));
            assertEquals(filterNonHiddenColumnMetadata(tableMetadata.getColumns()), columns);

            // verify the data
            ConnectorTableLayoutResult tableLayoutResult = metadata.getTableLayoutForConstraint(session, hiveTableHandle, Constraint.alwaysTrue(), Optional.empty());
            HiveTableLayoutHandle layoutHandle = (HiveTableLayoutHandle) tableLayoutResult.getTableLayout().getHandle();
            assertEquals(layoutHandle.getPartitions().get().size(), 1);
            ConnectorSplitSource splitSource = splitManager.getSplits(transaction.getTransactionHandle(), session, layoutHandle, SPLIT_SCHEDULING_CONTEXT);
            ConnectorSplit split = getOnlyElement(getAllSplits(splitSource));

            TableHandle tableHandle = new TableHandle(new ConnectorId("hive"), hiveTableHandle, transaction.getTransactionHandle(), Optional.of(layoutHandle));

            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                    transaction.getTransactionHandle(),
                    session,
                    split,
                    tableHandle.getLayout().get(),
                    columnHandles,
                    NON_CACHEABLE,
                    new RuntimeStats())) {
                MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));
                assertEqualsIgnoreOrder(result.getMaterializedRows(), data.getMaterializedRows());
            }
        }
    }

    private void dropTable(SchemaTableName table)
    {
        try (Transaction transaction = newTransaction()) {
            transaction.getMetastore().dropTable(new HdfsContext(newSession()), table.getSchemaName(), table.getTableName());
            transaction.commit();
        }
    }

    public static class TestingHiveMetastore
            extends InMemoryCachingHiveMetastore
    {
        private final Path basePath;
        private final HdfsEnvironment hdfsEnvironment;

        public TestingHiveMetastore(ExtendedHiveMetastore delegate, ExecutorService executor, MetastoreClientConfig metastoreClientConfig, Path basePath, HdfsEnvironment hdfsEnvironment)
        {
            super(delegate, executor, NOOP_METASTORE_CACHE_STATS, metastoreClientConfig);
            this.basePath = basePath;
            this.hdfsEnvironment = hdfsEnvironment;
        }

        @Override
        public Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
        {
            return super.getDatabase(metastoreContext, databaseName)
                    .map(database -> Database.builder(database)
                            .setLocation(Optional.of(basePath.toString()))
                            .build());
        }

        @Override
        public MetastoreOperationResult createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges privileges, List<TableConstraint<String>> constraints)
        {
            // hack to work around the metastore not being configured for S3 or other FS
            Table.Builder tableBuilder = Table.builder(table);
            tableBuilder.getStorageBuilder().setLocation("/");
            super.createTable(metastoreContext, tableBuilder.build(), privileges, constraints);
            return EMPTY_RESULT;
        }

        @Override
        public void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
        {
            try {
                Optional<Table> table = getTable(metastoreContext, databaseName, tableName);
                if (!table.isPresent()) {
                    throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
                }

                // hack to work around the metastore not being configured for S3 or other FS
                List<String> locations = listAllDataPaths(metastoreContext, databaseName, tableName);

                Table.Builder tableBuilder = Table.builder(table.get());
                tableBuilder.getStorageBuilder().setLocation("/");

                // drop table
                replaceTable(metastoreContext, databaseName, tableName, tableBuilder.build(), new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of()));
                getDelegate().dropTable(metastoreContext, databaseName, tableName, false);

                // drop data
                if (deleteData) {
                    for (String location : locations) {
                        Path path = new Path(location);
                        hdfsEnvironment.getFileSystem(TESTING_CONTEXT, path).delete(path, true);
                    }
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            finally {
                invalidateTableCache(databaseName, tableName);
            }
        }

        public void updateTableLocation(MetastoreContext metastoreContext, String databaseName, String tableName, String location)
        {
            Optional<Table> table = getTable(metastoreContext, databaseName, tableName);
            if (!table.isPresent()) {
                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
            }

            Table.Builder tableBuilder = Table.builder(table.get());
            tableBuilder.getStorageBuilder().setLocation(location);

            // NOTE: this clears the permissions
            replaceTable(metastoreContext, databaseName, tableName, tableBuilder.build(), new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of()));
        }

        private List<String> listAllDataPaths(MetastoreContext metastoreContext, String schemaName, String tableName)
        {
            ImmutableList.Builder<String> locations = ImmutableList.builder();
            Table table = getTable(metastoreContext, schemaName, tableName).get();
            if (table.getStorage().getLocation() != null) {
                // For partitioned table, there should be nothing directly under this directory.
                // But including this location in the set makes the directory content assert more
                // extensive, which is desirable.
                locations.add(table.getStorage().getLocation());
            }

            Optional<List<PartitionNameWithVersion>> partitionNames = getPartitionNames(metastoreContext, schemaName, tableName);
            if (partitionNames.isPresent()) {
                getPartitionsByNames(metastoreContext, schemaName, tableName, partitionNames.get()).values().stream()
                        .map(Optional::get)
                        .map(partition -> partition.getStorage().getLocation())
                        .filter(location -> !location.startsWith(table.getStorage().getLocation()))
                        .forEach(locations::add);
            }

            return locations.build();
        }
    }
}
