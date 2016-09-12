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

import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.hive.AbstractTestHiveClient.HiveTransaction;
import com.facebook.presto.hive.AbstractTestHiveClient.Transaction;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.ThriftHiveMetastore;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
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
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.hadoop.HadoopFileStatus.isDirectory;
import static com.facebook.presto.hive.AbstractTestHiveClient.createTableProperties;
import static com.facebook.presto.hive.AbstractTestHiveClient.filterNonHiddenColumnHandles;
import static com.facebook.presto.hive.AbstractTestHiveClient.filterNonHiddenColumnMetadata;
import static com.facebook.presto.hive.AbstractTestHiveClient.getAllSplits;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveDataStreamFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveRecordCursorProvider;
import static com.facebook.presto.hive.HiveTestUtils.getTypes;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(groups = "hive-s3")
public abstract class AbstractTestHiveClientS3
{
    protected String writableBucket;

    protected String database;
    protected SchemaTableName tableS3;
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

    @BeforeClass
    public void setUp()
            throws Exception
    {
        executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    protected void setupHive(String databaseName)
    {
        database = databaseName;
        tableS3 = new SchemaTableName(database, "presto_test_s3");

        String random = UUID.randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
        temporaryCreateTable = new SchemaTableName(database, "tmp_presto_test_create_s3_" + random);
    }

    protected void setup(String host, int port, String databaseName, String awsAccessKey, String awsSecretKey, String writableBucket)
    {
        this.writableBucket = writableBucket;

        setupHive(databaseName);

        HiveClientConfig hiveClientConfig = new HiveClientConfig()
                .setS3AwsAccessKey(awsAccessKey)
                .setS3AwsSecretKey(awsSecretKey);

        String proxy = System.getProperty("hive.metastore.thrift.client.socks-proxy");
        if (proxy != null) {
            hiveClientConfig.setMetastoreSocksProxy(HostAndPort.fromString(proxy));
        }

        HiveConnectorId connectorId = new HiveConnectorId("hive-test");
        HiveCluster hiveCluster = new TestingHiveCluster(hiveClientConfig, host, port);
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("hive-s3-%s"));
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationUpdater(hiveClientConfig));
        HivePartitionManager hivePartitionManager = new HivePartitionManager(connectorId, TYPE_MANAGER, hiveClientConfig);

        hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveClientConfig, new NoHdfsAuthentication());
        metastoreClient = new TestingHiveMetastore(
                new BridgingHiveMetastore(new ThriftHiveMetastore(hiveCluster)),
                executor,
                hiveClientConfig,
                writableBucket,
                hdfsEnvironment);
        locationService = new HiveLocationService(hdfsEnvironment);
        TypeRegistry typeManager = new TypeRegistry();
        JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);
        metadataFactory = new HiveMetadataFactory(
                connectorId,
                hiveClientConfig,
                metastoreClient,
                hdfsEnvironment,
                hivePartitionManager,
                newDirectExecutorService(),
                typeManager,
                locationService,
                new TableParameterCodec(),
                partitionUpdateCodec,
                new HiveTypeTranslator(),
                new NodeVersion("test_version"));
        transactionManager = new HiveTransactionManager();
        splitManager = new HiveSplitManager(
                connectorId,
                transactionHandle -> ((HiveMetadata) transactionManager.get(transactionHandle)).getMetastore(),
                new NamenodeStats(),
                hdfsEnvironment,
                new HadoopDirectoryLister(),
                new BoundedExecutor(executor, hiveClientConfig.getMaxSplitIteratorThreads()),
                hiveClientConfig.getMaxOutstandingSplits(),
                hiveClientConfig.getMinPartitionBatchSize(),
                hiveClientConfig.getMaxPartitionBatchSize(),
                hiveClientConfig.getMaxInitialSplits(),
                hiveClientConfig.getRecursiveDirWalkerEnabled());
        pageSinkProvider = new HivePageSinkProvider(hdfsEnvironment, metastoreClient, new GroupByHashPageIndexerFactory(), typeManager, new HiveClientConfig(), locationService, partitionUpdateCodec);
        pageSourceProvider = new HivePageSourceProvider(hiveClientConfig, hdfsEnvironment, getDefaultHiveRecordCursorProvider(hiveClientConfig), getDefaultHiveDataStreamFactories(hiveClientConfig), TYPE_MANAGER);
    }

    protected ConnectorSession newSession()
    {
        return new TestingConnectorSession(new HiveSessionProperties(new HiveClientConfig()).getSessionProperties());
    }

    protected Transaction newTransaction()
    {
        return new HiveTransaction(transactionManager, metadataFactory.create());
    }

    @Test
    public void testGetRecordsS3()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle table = getTableHandle(metadata, tableS3);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, table).values());
            Map<String, Integer> columnIndex = indexColumns(columnHandles);

            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, table, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
            HiveTableLayoutHandle layoutHandle = (HiveTableLayoutHandle) getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
            assertEquals(layoutHandle.getPartitions().get().size(), 1);
            ConnectorSplitSource splitSource = splitManager.getSplits(transaction.getTransactionHandle(), session, layoutHandle);

            long sum = 0;

            for (ConnectorSplit split : getAllSplits(splitSource)) {
                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, split, columnHandles)) {
                    MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));

                    for (MaterializedRow row : result) {
                        sum += (Long) row.getField(columnIndex.get("t_bigint"));
                    }
                }
            }
            assertEquals(sum, 78300);
        }
    }

    @Test
    public void testGetFileStatus()
            throws Exception
    {
        Path basePath = new Path("s3://presto-test-hive/");
        Path tablePath = new Path(basePath, "presto_test_s3");
        Path filePath = new Path(tablePath, "test1.csv");
        FileSystem fs = hdfsEnvironment.getFileSystem("user", basePath);

        assertTrue(isDirectory(fs.getFileStatus(basePath)));
        assertTrue(isDirectory(fs.getFileStatus(tablePath)));
        assertFalse(isDirectory(fs.getFileStatus(filePath)));
        assertFalse(fs.exists(new Path(basePath, "foo")));
    }

    @Test
    public void testRename()
            throws Exception
    {
        Path basePath = new Path(format("s3://%s/rename/%s/", writableBucket, UUID.randomUUID()));
        FileSystem fs = hdfsEnvironment.getFileSystem("user", basePath);
        assertFalse(fs.exists(basePath));

        // create file foo.txt
        Path path = new Path(basePath, "foo.txt");
        assertTrue(fs.createNewFile(path));
        assertTrue(fs.exists(path));

        // rename foo.txt to bar.txt
        Path newPath = new Path(basePath, "bar.txt");
        assertFalse(fs.exists(newPath));
        assertTrue(fs.rename(path, newPath));
        assertFalse(fs.exists(path));
        assertTrue(fs.exists(newPath));

        // create file foo.txt and rename to bar.txt
        assertTrue(fs.createNewFile(path));
        assertFalse(fs.rename(path, newPath));
        assertTrue(fs.exists(path));

        // rename foo.txt to foo.txt
        assertTrue(fs.rename(path, path));
        assertTrue(fs.exists(path));

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
        for (HiveStorageFormat storageFormat : HiveStorageFormat.values()) {
            try {
                doCreateTable(temporaryCreateTable, storageFormat);
            }
            finally {
                dropTable(temporaryCreateTable);
            }
        }
    }

    private void doCreateTable(SchemaTableName tableName, HiveStorageFormat storageFormat)
            throws Exception
    {
        List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata("id", BIGINT))
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
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, outputHandle);
            sink.appendPage(data.toPage(), null);
            Collection<Slice> fragments = sink.finish();

            // commit the table
            metadata.finishCreateTable(session, outputHandle, fragments);

            transaction.commit();

            // Hack to work around the metastore not being configured for S3.
            // The metastore tries to validate the location when creating the
            // table, which fails without explicit configuration for S3.
            // We work around that by using a dummy location when creating the
            // table and update it here to the correct S3 location.
            metastoreClient.updateTableLocation(
                    database,
                    tableName.getTableName(),
                    locationService.writePathRoot(((HiveOutputTableHandle) outputHandle).getLocationHandle()).get().toString());
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            // load the new table
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

            // verify the metadata
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, getTableHandle(metadata, tableName));
            assertEquals(filterNonHiddenColumnMetadata(tableMetadata.getColumns()), columns);

            // verify the data
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
            HiveTableLayoutHandle layoutHandle = (HiveTableLayoutHandle) getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
            assertEquals(layoutHandle.getPartitions().get().size(), 1);
            ConnectorSplitSource splitSource = splitManager.getSplits(transaction.getTransactionHandle(), session, layoutHandle);
            ConnectorSplit split = getOnlyElement(getAllSplits(splitSource));

            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, split, columnHandles)) {
                MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));
                assertEqualsIgnoreOrder(result.getMaterializedRows(), data.getMaterializedRows());
            }
        }
    }

    private void dropTable(SchemaTableName table)
    {
        try (Transaction transaction = newTransaction()) {
            transaction.getMetastore(table.getSchemaName()).dropTable(newSession(), table.getSchemaName(), table.getTableName());
            transaction.commit();
        }
        catch (RuntimeException e) {
            // this usually occurs because the table was not created
        }
    }

    private ConnectorTableHandle getTableHandle(ConnectorMetadata metadata, SchemaTableName tableName)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(newSession(), tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private static ImmutableMap<String, Integer> indexColumns(List<ColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            HiveColumnHandle hiveColumnHandle = checkType(columnHandle, HiveColumnHandle.class, "columnHandle");
            index.put(hiveColumnHandle.getName(), i);
            i++;
        }
        return index.build();
    }

    private static class TestingHiveMetastore
            extends CachingHiveMetastore
    {
        private final String writableBucket;
        private final HdfsEnvironment hdfsEnvironment;

        public TestingHiveMetastore(ExtendedHiveMetastore delegate, ExecutorService executor, HiveClientConfig hiveClientConfig, String writableBucket, HdfsEnvironment hdfsEnvironment)
        {
            super(delegate, executor, hiveClientConfig);
            this.writableBucket = writableBucket;
            this.hdfsEnvironment = hdfsEnvironment;
        }

        @Override
        public Optional<Database> getDatabase(String databaseName)
        {
            Optional<Database> database = super.getDatabase(databaseName);
            if (database.isPresent()) {
                database.get().setLocationUri("s3://" + writableBucket + "/");
            }
            return database;
        }

        @Override
        public void createTable(Table table, PrincipalPrivilegeSet privilegeSet)
        {
            // hack to work around the metastore not being configured for S3
            Table.Builder tableBuilder = Table.builder(table);
            tableBuilder.getStorageBuilder().setLocation("/");
            super.createTable(tableBuilder.build(), privilegeSet);
        }

        @Override
        public void dropTable(String databaseName, String tableName, boolean deleteData)
        {
            try {
                Optional<Table> table = getTable(databaseName, tableName);
                if (!table.isPresent()) {
                    throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
                }

                // hack to work around the metastore not being configured for S3
                List<String> locations = listAllDataPaths(databaseName, tableName);

                Table.Builder tableBuilder = Table.builder(table.get());
                tableBuilder.getStorageBuilder().setLocation("/");

                // drop table
                replaceTable(databaseName, tableName, tableBuilder.build(), new PrincipalPrivilegeSet());
                delegate.dropTable(databaseName, tableName, false);

                // drop data
                if (deleteData) {
                    for (String location : locations) {
                        Path path = new Path(location);
                        hdfsEnvironment.getFileSystem("user", path).delete(path, true);
                    }
                }
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
            finally {
                invalidateTable(databaseName, tableName);
            }
        }

        public void updateTableLocation(String databaseName, String tableName, String location)
        {
            Optional<Table> table = getTable(databaseName, tableName);
            if (!table.isPresent()) {
                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
            }

            Table.Builder tableBuilder = Table.builder(table.get());
            tableBuilder.getStorageBuilder().setLocation(location);

            replaceTable(databaseName, tableName, tableBuilder.build(), new PrincipalPrivilegeSet());
        }

        private List<String> listAllDataPaths(String schemaName, String tableName)
        {
            ImmutableList.Builder<String> locations = ImmutableList.builder();
            Table table = getTable(schemaName, tableName).get();
            if (table.getStorage().getLocation() != null) {
                // For partitioned table, there should be nothing directly under this directory.
                // But including this location in the set makes the directory content assert more
                // extensive, which is desirable.
                locations.add(table.getStorage().getLocation());
            }

            Optional<List<String>> partitionNames = getPartitionNames(schemaName, tableName);
            if (partitionNames.isPresent()) {
                getPartitionsByNames(schemaName, tableName, partitionNames.get()).values().stream()
                        .map(Optional::get)
                        .map(partition -> partition.getStorage().getLocation())
                        .filter(location -> !location.startsWith(table.getStorage().getLocation()))
                        .forEach(locations::add);
            }

            return locations.build();
        }
    }
}
