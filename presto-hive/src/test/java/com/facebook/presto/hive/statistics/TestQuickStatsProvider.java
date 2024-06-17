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
package com.facebook.presto.hive.statistics;

import com.facebook.presto.hive.ColumnConverterProvider;
import com.facebook.presto.hive.DirectoryLister;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.NamenodeStats;
import com.facebook.presto.hive.PartitionNameWithVersion;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePartitionMutator;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.thrift.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.HiveCluster;
import com.facebook.presto.hive.metastore.thrift.TestingHiveCluster;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastore;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER;
import static com.facebook.presto.hive.HiveSessionProperties.QUICK_STATS_BACKGROUND_BUILD_TIMEOUT;
import static com.facebook.presto.hive.HiveSessionProperties.QUICK_STATS_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.QUICK_STATS_INLINE_BUILD_TIMEOUT;
import static com.facebook.presto.hive.HiveSessionProperties.SKIP_EMPTY_FILES;
import static com.facebook.presto.hive.HiveSessionProperties.USE_LIST_DIRECTORY_CACHE;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveTestUtils.createTestHdfsEnvironment;
import static com.facebook.presto.hive.RetryDriver.retry;
import static com.facebook.presto.hive.metastore.PartitionStatistics.empty;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.hive.statistics.PartitionQuickStats.convertToPartitionStatistics;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.Collections.emptyIterator;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testcontainers.shaded.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestQuickStatsProvider
{
    public static final String TEST_TABLE = "test_table";
    public static final String TEST_SCHEMA = "test_schema";
    private static final List<PropertyMetadata<?>> quickStatsProperties = ImmutableList.of(booleanProperty(
                    QUICK_STATS_ENABLED,
                    "Use quick stats to resolve stats",
                    true,
                    false),
            new PropertyMetadata<>(
                    QUICK_STATS_INLINE_BUILD_TIMEOUT,
                    "Inline build timeout for a quick stats call",
                    VARCHAR,
                    Duration.class,
                    new Duration(5, TimeUnit.MINUTES),
                    false,
                    value -> Duration.valueOf((String) value),
                    Duration::toString),
            new PropertyMetadata<>(
                    QUICK_STATS_BACKGROUND_BUILD_TIMEOUT,
                    "Duration to wait for a background build on another thread",
                    VARCHAR,
                    Duration.class,
                    new Duration(0, TimeUnit.MINUTES),
                    false,
                    value -> Duration.valueOf((String) value),
                    Duration::toString),
            booleanProperty(
                    USE_LIST_DIRECTORY_CACHE,
                    "Directory list caching",
                    false,
                    false),
            booleanProperty(
                    SKIP_EMPTY_FILES,
                    "If it is required empty files will be skipped",
                    false,
                    false));
    public static final ConnectorSession SESSION = new TestingConnectorSession(quickStatsProperties);
    private final HiveClientConfig hiveClientConfig = new HiveClientConfig().setRecursiveDirWalkerEnabled(true);
    private HdfsEnvironment hdfsEnvironment;
    private DirectoryLister directoryListerMock;
    private SemiTransactionalHiveMetastore metastoreMock;
    private MetastoreContext metastoreContext;
    private PartitionQuickStats mockPartitionQuickStats;
    private PartitionStatistics expectedPartitionStats;

    private static ConnectorSession getSession(String inlineBuildTimeout, String backgroundBuildTimeout)
    {
        return new TestingConnectorSession(quickStatsProperties, ImmutableMap.of(
                QUICK_STATS_INLINE_BUILD_TIMEOUT, inlineBuildTimeout,
                QUICK_STATS_BACKGROUND_BUILD_TIMEOUT, backgroundBuildTimeout));
    }

    @BeforeTest
    public void setUp()
    {
        metastoreContext = new MetastoreContext(SESSION.getUser(),
                SESSION.getQueryId(),
                Optional.empty(),
                Collections.emptySet(),
                Optional.empty(),
                Optional.empty(),
                false,
                DEFAULT_COLUMN_CONVERTER_PROVIDER,
                SESSION.getWarningCollector(),
                SESSION.getRuntimeStats());
        Storage mockStorage = new Storage(
                fromHiveStorageFormat(PARQUET),
                "some/path",
                Optional.empty(),
                true,
                ImmutableMap.of(),
                ImmutableMap.of());
        Partition mockPartition = new Partition(
                TEST_SCHEMA,
                TEST_TABLE,
                ImmutableList.of(),
                mockStorage,
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty(),
                false,
                true,
                0,
                0,
                Optional.empty());
        Table mockTable = new Table(
                TEST_SCHEMA,
                TEST_TABLE,
                "owner",
                MANAGED_TABLE,
                Storage.builder()
                        .setStorageFormat(fromHiveStorageFormat(PARQUET))
                        .setLocation("location")
                        .build(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty());

        metastoreMock = MockSemiTransactionalHiveMetastore.create(mockTable, mockPartition);

        directoryListerMock = (fileSystem, table2, path, partition, namenodeStats, hiveDirectoryContext) -> emptyIterator();

        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        hdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig, metastoreClientConfig);

        ColumnQuickStats<Integer> mockIntegerColumnStats = new ColumnQuickStats<>("column", Integer.class);
        mockIntegerColumnStats.setMinValue(Integer.MIN_VALUE);
        mockIntegerColumnStats.setMaxValue(Integer.MAX_VALUE);
        mockIntegerColumnStats.addToRowCount(4242L);
        mockPartitionQuickStats = new PartitionQuickStats("partitionId", ImmutableList.of(mockIntegerColumnStats), 42);
        expectedPartitionStats = convertToPartitionStatistics(mockPartitionQuickStats);
    }

    @Test
    public void testReadThruCaching()
    {
        QuickStatsBuilder quickStatsBuilderMock = (session, metastore, table, metastoreContext, partitionId, files) -> mockPartitionQuickStats;

        QuickStatsProvider quickStatsProvider = new QuickStatsProvider(hdfsEnvironment, directoryListerMock, hiveClientConfig, new NamenodeStats(),
                ImmutableList.of(quickStatsBuilderMock));

        // Execute
        ImmutableList<String> testPartitions1 = ImmutableList.of("partition1", "partition2", "partition3");
        Map<String, PartitionStatistics> quickStats = quickStatsProvider.getQuickStats(SESSION, metastoreMock,
                new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions1);

        // Verify only one call was made for each test partition
        assertEquals(quickStats.entrySet().size(), testPartitions1.size());
        assertTrue(quickStats.keySet().containsAll(testPartitions1));
        quickStats.values().forEach(ps -> assertEquals(ps, expectedPartitionStats));

        // For subsequent calls for the same partitions that are already cached, no new calls are mode to the quick stats builder
        quickStatsProvider.getQuickStats(SESSION, metastoreMock,
                new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions1);

        // For subsequent calls with a mix of old and new partitions, we only see calls to the quick stats builder for the new partitions
        ImmutableList<String> testPartitions2 = ImmutableList.of("partition4", "partition5", "partition6");
        ImmutableList<String> testPartitionsMix = ImmutableList.<String>builder().addAll(testPartitions1).addAll(testPartitions2).build();

        quickStats = quickStatsProvider.getQuickStats(SESSION, metastoreMock,
                new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitionsMix);
        assertEquals(quickStats.entrySet().size(), testPartitionsMix.size());
        assertTrue(quickStats.keySet().containsAll(testPartitionsMix));
        quickStats.values().forEach(ps -> assertEquals(ps, expectedPartitionStats));
    }

    /**
     * A test to demonstrate that concurrent quick stats build for the same partition results in only a single call to the quick stats builder
     * Note: This test simulates long-running operations with a sleep, which makes running these in CI flaky
     * To remove noise due to these flakiness, we disable this test.
     * It is a good candidate for a manual test run for any changes related to Quick Stats
     */
    @Test(enabled = false, invocationCount = 3)
    public void testConcurrentFetchForSamePartition()
            throws ExecutionException, InterruptedException
    {
        QuickStatsBuilder longRunningQuickStatsBuilderMock = (session, metastore, table, metastoreContext, partitionId, files) -> {
            // Sleep for 50ms to simulate a long-running quick stats call
            sleepUninterruptibly(50, MILLISECONDS);
            return mockPartitionQuickStats;
        };

        QuickStatsProvider quickStatsProvider = new QuickStatsProvider(hdfsEnvironment, directoryListerMock, hiveClientConfig, new NamenodeStats(),
                ImmutableList.of(longRunningQuickStatsBuilderMock));

        List<String> testPartitions = ImmutableList.of("partition1", "partition2", "partition3");

        {
            // Build a session where an inline quick stats build will occur for the 1st query that initiates the build,
            // but subsequent queries will NOT wait

            ConnectorSession session = getSession("600ms", "0ms");
            // Execute two concurrent calls for the same partitions; wait for them to complete
            CompletableFuture<Map<String, PartitionStatistics>> future1 = supplyAsync(() -> quickStatsProvider.getQuickStats(session, metastoreMock,
                    new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions), commonPool());

            CompletableFuture<Map<String, PartitionStatistics>> future2 = supplyAsync(() -> quickStatsProvider.getQuickStats(session, metastoreMock,
                    new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions), commonPool());

            allOf(future1, future2).join();

            Map<String, PartitionStatistics> quickStats1 = future1.get();
            Map<String, PartitionStatistics> quickStats2 = future2.get();

            // Both PartitionStatistics have stats for all partitions
            assertEquals(quickStats1.entrySet().size(), testPartitions.size());
            assertTrue(quickStats1.keySet().containsAll(testPartitions));
            assertEquals(quickStats2.entrySet().size(), testPartitions.size());
            assertTrue(quickStats2.keySet().containsAll(testPartitions));

            // For the same partition, we will observe that one of them has the expected partition stats and the other one is empty
            for (String testPartition : testPartitions) {
                PartitionStatistics partitionStatistics1 = quickStats1.get(testPartition);
                PartitionStatistics partitionStatistics2 = quickStats2.get(testPartition);

                if (partitionStatistics1.equals(empty())) {
                    assertEquals(partitionStatistics2, expectedPartitionStats);
                }
                else if (partitionStatistics2.equals(empty())) {
                    assertEquals(partitionStatistics1, expectedPartitionStats);
                }
                else {
                    fail(String.format("For [%s] one of the partitions stats was expected to be empty. Actual partitionStatistics1 [%s], partitionStatistics2 [%s]",
                            testPartition, partitionStatistics1, partitionStatistics2));
                }
            }

            // Future calls for the same partitions will return from cached partition stats with valid values
            Map<String, PartitionStatistics> quickStats = quickStatsProvider.getQuickStats(session, metastoreMock,
                    new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions);

            // Verify only one call was made for each test partition
            assertEquals(quickStats.entrySet().size(), testPartitions.size());
            assertTrue(quickStats.keySet().containsAll(testPartitions));
            quickStats.values().forEach(ps -> assertEquals(ps, expectedPartitionStats));
        }

        {
            // Build a session where an inline quick stats build will occur for the 1st query that initiates the build,
            // and subsequent queries will wait for this inline build to finish too

            ConnectorSession session = getSession("300ms", "300ms");
            // Execute two concurrent calls for the same partitions; wait for them to complete
            CompletableFuture<Map<String, PartitionStatistics>> future1 = supplyAsync(() -> quickStatsProvider.getQuickStats(session, metastoreMock,
                    new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions), commonPool());

            CompletableFuture<Map<String, PartitionStatistics>> future2 = supplyAsync(() -> quickStatsProvider.getQuickStats(session, metastoreMock,
                    new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions), commonPool());

            allOf(future1, future2).join();

            Map<String, PartitionStatistics> quickStats1 = future1.get();
            Map<String, PartitionStatistics> quickStats2 = future2.get();

            assertEquals(quickStats1, quickStats2); // Both PartitionStatistics have exactly same stats
            assertTrue(quickStats1.keySet().containsAll(testPartitions)); // Stats for all partitions are present
            // None of the partition stats are empty, they are all the mocked value
            assertTrue(quickStats1.values().stream().allMatch(x -> x.equals(convertToPartitionStatistics(mockPartitionQuickStats))));
        }
    }

    /**
     * A test to demonstrate that building quick stats, either inline or in the background is time-bounded
     * Note: This test simulates long-running operations with a sleep, which makes running these in CI flaky
     * To remove noise due to these flakiness, we disable this test.
     * It is a good candidate for a manual test run for any changes related to Quick Stats
     */
    @Test(enabled = false)
    public void quickStatsBuildTimeIsBounded()
            throws Exception
    {
        ImmutableMap<String, Long> mockPerPartitionStatsFetchTimes = ImmutableMap.of("p1", 10L, "p2", 20L, "p3", 1500L, "p4", 1800L);
        QuickStatsBuilder longRunningQuickStatsBuilderMock = (session, metastore, table, metastoreContext, partitionId, files) -> {
            sleepUninterruptibly(mockPerPartitionStatsFetchTimes.get(partitionId), MILLISECONDS);
            return mockPartitionQuickStats;
        };

        {
            QuickStatsProvider quickStatsProvider = new QuickStatsProvider(hdfsEnvironment, directoryListerMock, hiveClientConfig, new NamenodeStats(),
                    ImmutableList.of(longRunningQuickStatsBuilderMock));
            // Create a session where an inline build will occur for any newly requested partition
            ConnectorSession session = getSession("300ms", "0ms");
            List<String> testPartitions = ImmutableList.copyOf(mockPerPartitionStatsFetchTimes.keySet());
            Map<String, PartitionStatistics> quickStats = quickStatsProvider.getQuickStats(session, metastoreMock,
                    new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions);
            Map<String, Instant> inProgressBuildsSnapshot = quickStatsProvider.getInProgressBuildsSnapshot();

            assertEquals(quickStats.size(), 4);
            assertEquals(quickStats.get("p1"), convertToPartitionStatistics(mockPartitionQuickStats));
            assertEquals(quickStats.get("p2"), convertToPartitionStatistics(mockPartitionQuickStats));
            // Since fetching quick stats for p3 and p4 would take > 300ms, we would return EMPTY partition stats for them
            assertEquals(quickStats.get("p3"), empty());
            assertEquals(quickStats.get("p4"), empty());

            // Snapshot of the in-progress builds confirms that quickstats build for these partitions is in progress
            String p3CacheKey = String.format("%s.%s/%s", TEST_SCHEMA, TEST_TABLE, "p3");
            String p4CacheKey = String.format("%s.%s/%s", TEST_SCHEMA, TEST_TABLE, "p4");
            assertEquals(inProgressBuildsSnapshot.keySet(), ImmutableSet.of(p3CacheKey, p4CacheKey));
        }

        {
            // Create a session where no inline builds will occur for any requested partition; empty() quick stats will be returned
            ConnectorSession session = getSession("0ms", "0ms");

            QuickStatsProvider quickStatsProvider = new QuickStatsProvider(hdfsEnvironment, directoryListerMock, hiveClientConfig, new NamenodeStats(),
                    ImmutableList.of((session1, metastore, table, metastoreContext, partitionId, files) -> mockPartitionQuickStats));

            Map<String, PartitionStatistics> quickStats = quickStatsProvider.getQuickStats(session, metastoreMock,
                    new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, ImmutableList.of("p5", "p6"));

            assertEquals(quickStats.size(), 2);
            assertEquals(quickStats.get("p5"), empty());
            assertEquals(quickStats.get("p6"), empty());

            // Subsequent queries for the same partitions will fetch the cached stats
            // Stats may not appear immediately, so we retry with exponential delay
            retry()
                    .maxAttempts(10)
                    .exponentialBackoff(new Duration(20D, MILLISECONDS), new Duration(500D, MILLISECONDS), new Duration(2, SECONDS), 2.0)
                    .run("waitForQuickStatsBuild", () -> {
                        Map<String, PartitionStatistics> quickStatsAfter = quickStatsProvider.getQuickStats(session, metastoreMock,
                                new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, ImmutableList.of("p5", "p6"));

                        try {
                            assertEquals(quickStatsAfter.size(), 2);
                            assertEquals(quickStatsAfter.get("p5"), convertToPartitionStatistics(mockPartitionQuickStats));
                            assertEquals(quickStatsAfter.get("p6"), convertToPartitionStatistics(mockPartitionQuickStats));
                            return true;
                        }
                        catch (AssertionError e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    public static class MockSemiTransactionalHiveMetastore
            extends SemiTransactionalHiveMetastore
    {
        private final Table mockTable;
        private final Partition mockPartition;

        private MockSemiTransactionalHiveMetastore(HdfsEnvironment hdfsEnvironment,
                ExtendedHiveMetastore delegate,
                ListeningExecutorService renameExecutor,
                boolean skipDeletionForAlter,
                boolean skipTargetCleanupOnRollback,
                boolean undoMetastoreOperationsEnabled,
                ColumnConverterProvider columnConverterProvider,
                Table mockTable, Partition mockPartition)
        {
            super(hdfsEnvironment, delegate, renameExecutor, skipDeletionForAlter, skipTargetCleanupOnRollback, undoMetastoreOperationsEnabled, columnConverterProvider);
            this.mockPartition = mockPartition;
            this.mockTable = mockTable;
        }

        public static MockSemiTransactionalHiveMetastore create(Table mockTable, Partition mockPartition)
        {
            // none of these values matter, as we never use them
            HiveClientConfig config = new HiveClientConfig();
            MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
            HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config, metastoreClientConfig), ImmutableSet.of(), config);
            HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
            HiveCluster hiveCluster = new TestingHiveCluster(metastoreClientConfig, "dummy", 1000);
            ColumnConverterProvider columnConverterProvider = HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER;
            ExtendedHiveMetastore delegate = new BridgingHiveMetastore(new ThriftHiveMetastore(hiveCluster, metastoreClientConfig, hdfsEnvironment), new HivePartitionMutator());
            ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));
            ListeningExecutorService renameExecutor = listeningDecorator(executor);

            return new MockSemiTransactionalHiveMetastore(hdfsEnvironment, delegate, renameExecutor, false, false, true,
                    columnConverterProvider, mockTable, mockPartition);
        }

        @Override
        public synchronized Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
        {
            return Optional.of(mockPartition);
        }

        @Override
        public synchronized Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionNameWithVersion> partitionNames)
        {
            checkArgument(partitionNames.size() == 1, "Expected caller to only pass in a single partition to fetch");
            return ImmutableMap.of(partitionNames.get(0).getPartitionName(), Optional.of(mockPartition));
        }

        @Override
        public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
        {
            return Optional.of(mockTable);
        }
    }
}
