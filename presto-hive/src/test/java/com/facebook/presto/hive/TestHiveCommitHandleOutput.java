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

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.datasink.OutputStreamDataSinkFactory;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.MetastoreOperationResult;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.UnimplementedHiveMetastore;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorCommitHandle;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.hive.AbstractTestHiveClient.TEST_SERVER_VERSION;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.EXTERNAL_LOCATION_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.SORTED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static com.facebook.presto.hive.HiveTestUtils.FILTER_STATS_CALCULATOR_SERVICE;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_RESOLUTION;
import static com.facebook.presto.hive.HiveTestUtils.ROW_EXPRESSION_SERVICE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_QUERY_ID_NAME;
import static com.facebook.presto.hive.metastore.MetastoreUtil.toPartitionValues;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static java.nio.file.Files.createTempDirectory;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveCommitHandleOutput
{
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";

    private static final Map<String, Object> testTableProperties;
    private static ConnectorTableMetadata testTableMetadata;

    static {
        try {
            URI tempUri = createTempDirectory("test").toUri();
            testTableProperties = ImmutableMap.<String, Object>builder()
                    .put(BUCKET_COUNT_PROPERTY, 0)
                    .put(BUCKETED_BY_PROPERTY, ImmutableList.of())
                    .put(SORTED_BY_PROPERTY, ImmutableList.of())
                    .put(STORAGE_FORMAT_PROPERTY, ORC)
                    .put(EXTERNAL_LOCATION_PROPERTY, tempUri.toASCIIString())
                    .put(PARTITIONED_BY_PROPERTY, ImmutableList.of("a"))
                    .build();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        testTableMetadata = new ConnectorTableMetadata(
                new SchemaTableName(TEST_SCHEMA, TEST_TABLE),
                ImmutableList.of(
                        new ColumnMetadata("b", BIGINT),
                        new ColumnMetadata("a", BIGINT)),
                testTableProperties);
    }

    @Test
    public void testCommitOutputForTable()
    {
        TestingExtendedHiveMetastore metastore = new TestingExtendedHiveMetastore();
        HiveClientConfig hiveClientConfig = new HiveClientConfig().setPartitionStatisticsBasedOptimizationEnabled(true);
        ListeningExecutorService listeningExecutor = MoreExecutors.listeningDecorator(newFixedThreadPool(10, daemonThreadsNamed("test-hive-commit-handle-%s")));
        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig().setPartitionStatisticsBasedOptimizationEnabled(true),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig()).getSessionProperties());
        HiveMetadata hiveMeta = getHiveMetadata(metastore, hiveClientConfig, listeningExecutor);

        // Create a table; write commit output should not be empty.
        hiveMeta.createTable(connectorSession, testTableMetadata, false);
        ConnectorCommitHandle handle = hiveMeta.commit();

        assertEquals(handle.getSerializedCommitOutputForRead(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)), "");
        assertFalse(handle.getSerializedCommitOutputForWrite(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)).isEmpty());

        // Get the table; both commit write and read should be empty.
        hiveMeta = getHiveMetadata(metastore, hiveClientConfig, listeningExecutor);
        HiveTableHandle hiveTableHandle = new HiveTableHandle(TEST_SCHEMA, TEST_TABLE, Optional.empty());
        hiveMeta.getTableMetadata(connectorSession, hiveTableHandle);
        handle = hiveMeta.commit();

        assertEquals(handle.getSerializedCommitOutputForRead(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)), "");
        assertEquals(handle.getSerializedCommitOutputForWrite(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)), "");
    }

    @Test
    public void testCommitOutputForPartitions()
    {
        TestingExtendedHiveMetastore metastore = new TestingExtendedHiveMetastore();
        HiveClientConfig hiveClientConfig = new HiveClientConfig().setPartitionStatisticsBasedOptimizationEnabled(true);
        ListeningExecutorService listeningExecutor = MoreExecutors.listeningDecorator(newFixedThreadPool(10, daemonThreadsNamed("test-hive-commit-handle-%s")));
        HiveMetadata hiveMeta = getHiveMetadata(metastore, hiveClientConfig, listeningExecutor);
        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig().setPartitionStatisticsBasedOptimizationEnabled(true),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig()).getSessionProperties());

        // Create a table.
        hiveMeta.createTable(connectorSession, testTableMetadata, false);
        ConnectorCommitHandle handle = hiveMeta.commit();

        assertEquals(handle.getSerializedCommitOutputForRead(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)), "");
        assertFalse(handle.getSerializedCommitOutputForWrite(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)).isEmpty());

        // Add a partition: a=1;
        hiveMeta = getHiveMetadata(metastore, hiveClientConfig, listeningExecutor);
        String partitionName = "a=1";
        hiveMeta.getMetastore().addPartition(
                connectorSession,
                TEST_SCHEMA,
                TEST_TABLE,
                "random_table_path",
                false,
                createPartition(partitionName, "location1"),
                new Path("/" + TEST_TABLE),
                PartitionStatistics.empty());
        handle = hiveMeta.commit();

        assertEquals(handle.getSerializedCommitOutputForRead(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)), "");
        assertFalse(handle.getSerializedCommitOutputForWrite(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)).isEmpty());
        String serializedCommitOutput = handle.getSerializedCommitOutputForWrite(new SchemaTableName(TEST_SCHEMA, TEST_TABLE));

        // Get the partition; the last commit output should equal to the one returned when adding the partition.
        hiveMeta = getHiveMetadata(metastore, hiveClientConfig, listeningExecutor);
        Map<String, Optional<Partition>> partitions = hiveMeta.getMetastore().getPartitionsByNames(
                new MetastoreContext(
                        connectorSession.getUser(),
                        connectorSession.getQueryId(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER),
                TEST_SCHEMA,
                TEST_TABLE,
                ImmutableList.of(partitionName));
        handle = hiveMeta.commit();

        Optional<Partition> partition = partitions.get(partitionName);
        assertTrue(partition.isPresent());
        assertEquals(
                handle.getSerializedCommitOutputForRead(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)),
                Long.toString(partition.get().getLastDataCommitTime()));
        assertEquals(handle.getSerializedCommitOutputForRead(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)), serializedCommitOutput);
        assertTrue(handle.getSerializedCommitOutputForWrite(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)).isEmpty());

        // Add the same partition with different location, it should trigger the metastore to generate different commit output.
        hiveMeta = getHiveMetadata(metastore, hiveClientConfig, listeningExecutor);
        hiveMeta.getMetastore().addPartition(
                connectorSession,
                TEST_SCHEMA,
                TEST_TABLE,
                "random_table_path",
                false,
                createPartition(partitionName, "location2"),
                new Path("/" + TEST_TABLE),
                PartitionStatistics.empty());
        handle = hiveMeta.commit();

        assertEquals(handle.getSerializedCommitOutputForRead(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)), "");
        assertFalse(handle.getSerializedCommitOutputForWrite(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)).isEmpty());
        assertTrue(handle.getSerializedCommitOutputForWrite(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)).equals(serializedCommitOutput));
    }

    private HiveMetadata getHiveMetadata(TestingExtendedHiveMetastore metastore, HiveClientConfig hiveClientConfig, ListeningExecutorService listeningExecutor)
    {
        HdfsEnvironment hdfsEnvironment = new TestingHdfsEnvironment(ImmutableList.of());
        HiveMetadataFactory hiveMetadataFactory = new HiveMetadataFactory(
                metastore,
                hdfsEnvironment,
                new HivePartitionManager(FUNCTION_AND_TYPE_MANAGER, hiveClientConfig),
                DateTimeZone.forOffsetHours(1),
                true,
                false,
                false,
                false,
                true,
                true,
                hiveClientConfig.getMaxPartitionBatchSize(),
                hiveClientConfig.getMaxPartitionsPerScan(),
                false,
                10_000,
                FUNCTION_AND_TYPE_MANAGER,
                new HiveLocationService(hdfsEnvironment),
                FUNCTION_RESOLUTION,
                ROW_EXPRESSION_SERVICE,
                FILTER_STATS_CALCULATOR_SERVICE,
                new TableParameterCodec(),
                HiveTestUtils.PARTITION_UPDATE_CODEC,
                HiveTestUtils.PARTITION_UPDATE_SMILE_CODEC,
                listeningExecutor,
                new HiveTypeTranslator(),
                new HiveStagingFileCommitter(hdfsEnvironment, listeningExecutor),
                new HiveZeroRowFileCreator(hdfsEnvironment, new OutputStreamDataSinkFactory(), listeningExecutor),
                TEST_SERVER_VERSION,
                new HivePartitionObjectBuilder(),
                new HiveEncryptionInformationProvider(ImmutableList.of()),
                new HivePartitionStats(),
                new HiveFileRenamer(),
                HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        return hiveMetadataFactory.get();
    }

    private Partition createPartition(String partitionName, String partitionLocation)
    {
        Partition.Builder partitionBuilder = Partition.builder()
                .setDatabaseName(TEST_SCHEMA)
                .setTableName(TEST_TABLE)
                .setColumns(ImmutableList.of())
                .setValues(toPartitionValues(partitionName))
                .withStorage(storage -> storage
                        .setStorageFormat(fromHiveStorageFormat(HiveStorageFormat.ORC))
                        .setLocation(new Path("/" + TEST_TABLE + "/" + partitionLocation, partitionName).toString()))
                .setEligibleToIgnore(true)
                .setSealedPartition(true)
                .setParameters(ImmutableMap.of(PRESTO_QUERY_ID_NAME, "random_query_id"));
        return partitionBuilder.build();
    }

    private static class TestingExtendedHiveMetastore
            extends UnimplementedHiveMetastore
    {
        private final Map<String, Long> lastDataCommitTimes = new HashMap<>();
        private final Map<String, Table> tables = new HashMap<>();
        private final Map<String, Partition> partitions = new HashMap<>();

        @Override
        public List<String> getAllDatabases(MetastoreContext metastoreContext)
        {
            return ImmutableList.of("hive_test");
        }

        @Override
        public Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
        {
            return Optional.of(new Database(databaseName, Optional.of("/"), "test_owner", PrincipalType.USER, Optional.empty(), ImmutableMap.of()));
        }

        @Override
        public MetastoreOperationResult createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges principalPrivileges)
        {
            String tableKey = createTableKey(table.getDatabaseName(), table.getTableName());
            tables.put(tableKey, table);
            long currentTime = System.currentTimeMillis() / 1000;
            lastDataCommitTimes.put(tableKey, currentTime);

            return new MetastoreOperationResult(ImmutableList.of(currentTime));
        }

        @Override
        public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
        {
            String tableKey = createTableKey(databaseName, tableName);
            return Optional.ofNullable(tables.get(tableKey));
        }

        @Override
        public void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
        {
            String tableKey = createTableKey(databaseName, tableName);
            lastDataCommitTimes.remove(tableKey);
            tables.remove(tableKey);
        }

        @Override
        public void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
        {
        }

        @Override
        public MetastoreOperationResult addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
        {
            List<Long> times = new ArrayList<>();
            for (PartitionWithStatistics partition : partitions) {
                String partitionKey = createPartitionKey(databaseName, tableName, partition.getPartitionName());
                Partition oldPartition = this.partitions.put(partitionKey, partition.getPartition());

                if (oldPartition != null) {
                    String oldLocation = oldPartition.getStorage().getLocation();
                    String newLocation = partition.getPartition().getStorage().getLocation();

                    // Use old data commit time if the location does not change.
                    if (oldLocation.equals(newLocation)) {
                        times.add(lastDataCommitTimes.get(partitionKey));
                    }
                    else {
                        // Update the data commit time if the partition location changes.
                        // Adding 1000 to ensure their times are different since their data commit times are compared in seconds.
                        long currentTime = System.currentTimeMillis() / 1000;
                        lastDataCommitTimes.put(partitionKey, currentTime);
                        times.add(currentTime);
                    }
                }
                else {
                    // If the partition is new, add the data commit time.
                    long currentTime = System.currentTimeMillis() / 1000;
                    lastDataCommitTimes.put(partitionKey, currentTime);
                    times.add(currentTime);
                }
            }
            return new MetastoreOperationResult(times);
        }

        @Override
        public MetastoreOperationResult alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partition)
        {
            String partitionKey = createPartitionKey(databaseName, tableName, partition.getPartitionName());
            Partition oldPartition = partitions.get(partitionKey);
            partitions.put(partitionKey, partition.getPartition());

            // When its location changes, we should generate a new commit time.
            if (oldPartition != null && oldPartition.getStorage().getLocation().equals(partition.getPartition().getStorage().getLocation())) {
                lastDataCommitTimes.put(partitionKey, System.currentTimeMillis() / 1000);
            }

            if (!lastDataCommitTimes.containsKey(partitionKey)) {
                return new MetastoreOperationResult(ImmutableList.of());
            }
            return new MetastoreOperationResult(ImmutableList.of(lastDataCommitTimes.get(partitionKey)));
        }

        @Override
        public Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
        {
            String partitionKey = createPartitionKey(databaseName, tableName, partitionValues);
            long time = lastDataCommitTimes.getOrDefault(partitionKey, 0L);

            Partition partition = partitions.get(partitionKey);
            if (partition != null) {
                Partition.Builder builder = Partition.builder(partition)
                        .setLastDataCommitTime(time);
                return Optional.ofNullable(builder.build());
            }
            return Optional.empty();
        }

        @Override
        public Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionNames)
        {
            Map<String, Optional<Partition>> result = new HashMap<>();
            for (String partitionName : partitionNames) {
                List<String> partitionValues = toPartitionValues(partitionName);
                String partitionKey = createPartitionKey(databaseName, tableName, partitionValues);
                long time = lastDataCommitTimes.getOrDefault(partitionKey, 0L);

                Partition partition = partitions.get(partitionKey);
                if (partition != null) {
                    Partition.Builder builder = Partition.builder(partition)
                            .setLastDataCommitTime(time);
                    result.put(partitionName, Optional.of(builder.build()));
                }
                else {
                    result.put(partitionName, Optional.empty());
                }
            }
            return result;
        }

        private String createPartitionKey(String databaseName, String tableName, String partitionName)
        {
            List<String> partitionValues = toPartitionValues(partitionName);

            return String.join(".", databaseName, tableName, partitionValues.toString());
        }

        private String createPartitionKey(String databaseName, String tableName, List<String> partitionValues)
        {
            return String.join(".", databaseName, tableName, partitionValues.toString());
        }

        private String createTableKey(String databaseName, String tableName)
        {
            return String.join(".", databaseName, tableName);
        }
    }

    private static class TestingHdfsEnvironment
            extends HdfsEnvironment
    {
        private final List<LocatedFileStatus> files;

        public TestingHdfsEnvironment(List<LocatedFileStatus> files)
        {
            super(
                    new HiveHdfsConfiguration(
                            new HdfsConfigurationInitializer(new HiveClientConfig(), new MetastoreClientConfig()),
                            ImmutableSet.of(),
                            new HiveClientConfig()),
                    new MetastoreClientConfig(),
                    new NoHdfsAuthentication());
            this.files = ImmutableList.copyOf(files);
        }

        @Override
        public ExtendedFileSystem getFileSystem(String user, Path path, Configuration configuration)
        {
            return new TestingHdfsFileSystem(files);
        }
    }

    private static class TestingHdfsFileSystem
            extends ExtendedFileSystem
    {
        private final List<LocatedFileStatus> files;

        public TestingHdfsFileSystem(List<LocatedFileStatus> files)
        {
            this.files = ImmutableList.copyOf(files);
        }

        @Override
        public boolean delete(Path f, boolean recursive)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rename(Path src, Path dst)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setWorkingDirectory(Path dir)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus[] listStatus(Path f)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
        {
            return new RemoteIterator<LocatedFileStatus>()
            {
                private final Iterator<LocatedFileStatus> iterator = files.iterator();

                @Override
                public boolean hasNext()
                        throws IOException
                {
                    return iterator.hasNext();
                }

                @Override
                public LocatedFileStatus next()
                        throws IOException
                {
                    return iterator.next();
                }
            };
        }

        @Override
        public FSDataOutputStream create(
                Path f,
                FsPermission permission,
                boolean overwrite,
                int bufferSize,
                short replication,
                long blockSize,
                Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean mkdirs(Path f, FsPermission permission)
        {
            return true;
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataInputStream open(Path f, int bufferSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus getFileStatus(Path f)
        {
            return new FileStatus(0, true, 0, 0, 0, 0, null, null, null, f);
        }

        @Override
        public Path getWorkingDirectory()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI getUri()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(Path f)
        {
            return false;
        }
    }
}
