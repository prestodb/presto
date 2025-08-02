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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.hive.DirectoryLister;
import com.facebook.presto.hive.HadoopDirectoryLister;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveDirectoryContext;
import com.facebook.presto.hive.HiveFileInfo;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.NamenodeStats;
import com.facebook.presto.hive.TestingExtendedHiveMetastore;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HiveSessionProperties.QUICK_STATS_BACKGROUND_BUILD_TIMEOUT;
import static com.facebook.presto.hive.HiveSessionProperties.QUICK_STATS_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.QUICK_STATS_INLINE_BUILD_TIMEOUT;
import static com.facebook.presto.hive.HiveSessionProperties.SKIP_EMPTY_FILES;
import static com.facebook.presto.hive.HiveSessionProperties.USE_LIST_DIRECTORY_CACHE;
import static com.facebook.presto.hive.HiveSessionProperties.isSkipEmptyFilesEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isUseListDirectoryCache;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveTestUtils.createTestHdfsEnvironment;
import static com.facebook.presto.hive.HiveUtil.buildDirectoryContextProperties;
import static com.facebook.presto.hive.HiveUtil.getInputFormat;
import static com.facebook.presto.hive.NestedDirectoryPolicy.RECURSE;
import static com.facebook.presto.hive.RetryDriver.retry;
import static com.facebook.presto.hive.metastore.PartitionStatistics.empty;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.hive.statistics.PartitionQuickStats.convertToPartitionStatistics;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.io.Resources.getResource;
import static java.nio.file.Files.copy;
import static java.nio.file.Files.createDirectory;
import static java.nio.file.Files.createFile;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.newBufferedWriter;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.Collections.emptyIterator;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testcontainers.shaded.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
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
    private ExtendedHiveMetastore metastoreMock;
    private MetastoreContext metastoreContext;
    private PartitionQuickStats mockPartitionQuickStats;
    private PartitionStatistics expectedPartitionStats;
    private ColumnQuickStats<Integer> mockIntegerColumnStats;

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
                Optional.of("catalogName"),
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
                Optional.of("catalogName"),
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

        metastoreMock = new TestingExtendedHiveMetastore();
        metastoreMock.createTable(metastoreContext, mockTable, new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of()), ImmutableList.of());
        metastoreMock.addPartitions(metastoreContext, TEST_SCHEMA, TEST_TABLE, ImmutableList.of(new PartitionWithStatistics(mockPartition, "TEST_PARTITION", empty())));

        directoryListerMock = (fileSystem, table2, path, partition, namenodeStats, hiveDirectoryContext) -> emptyIterator();

        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        hdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig, metastoreClientConfig);

        mockIntegerColumnStats = new ColumnQuickStats<>("column", Integer.class);
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

        QuickStatsProvider quickStatsProvider = new QuickStatsProvider(metastoreMock, hdfsEnvironment, directoryListerMock, hiveClientConfig, new NamenodeStats(),
                ImmutableList.of(quickStatsBuilderMock));

        // Execute
        ImmutableList<String> testPartitions1 = ImmutableList.of("partition1", "partition2", "partition3");
        Map<String, PartitionStatistics> quickStats = quickStatsProvider.getQuickStats(SESSION,
                new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions1);

        // Verify only one call was made for each test partition
        assertEquals(quickStats.entrySet().size(), testPartitions1.size());
        assertTrue(quickStats.keySet().containsAll(testPartitions1));
        quickStats.values().forEach(ps -> assertEquals(ps, expectedPartitionStats));

        // For subsequent calls for the same partitions that are already cached, no new calls are mode to the quick stats builder
        quickStatsProvider.getQuickStats(SESSION,
                new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions1);

        // For subsequent calls with a mix of old and new partitions, we only see calls to the quick stats builder for the new partitions
        ImmutableList<String> testPartitions2 = ImmutableList.of("partition4", "partition5", "partition6");
        ImmutableList<String> testPartitionsMix = ImmutableList.<String>builder().addAll(testPartitions1).addAll(testPartitions2).build();

        quickStats = quickStatsProvider.getQuickStats(SESSION,
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

        QuickStatsProvider quickStatsProvider = new QuickStatsProvider(metastoreMock, hdfsEnvironment, directoryListerMock, hiveClientConfig, new NamenodeStats(),
                ImmutableList.of(longRunningQuickStatsBuilderMock));

        List<String> testPartitions = ImmutableList.of("partition1", "partition2", "partition3");

        {
            // Build a session where an inline quick stats build will occur for the 1st query that initiates the build,
            // but subsequent queries will NOT wait

            ConnectorSession session = getSession("600ms", "0ms");
            // Execute two concurrent calls for the same partitions; wait for them to complete
            CompletableFuture<Map<String, PartitionStatistics>> future1 = supplyAsync(() -> quickStatsProvider.getQuickStats(session,
                    new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions), commonPool());

            CompletableFuture<Map<String, PartitionStatistics>> future2 = supplyAsync(() -> quickStatsProvider.getQuickStats(session,
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
            Map<String, PartitionStatistics> quickStats = quickStatsProvider.getQuickStats(session,
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
            CompletableFuture<Map<String, PartitionStatistics>> future1 = supplyAsync(() -> quickStatsProvider.getQuickStats(session,
                    new SchemaTableName(TEST_SCHEMA, TEST_TABLE), metastoreContext, testPartitions), commonPool());

            CompletableFuture<Map<String, PartitionStatistics>> future2 = supplyAsync(() -> quickStatsProvider.getQuickStats(session,
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
            QuickStatsProvider quickStatsProvider = new QuickStatsProvider(metastoreMock, hdfsEnvironment, directoryListerMock, hiveClientConfig, new NamenodeStats(),
                    ImmutableList.of(longRunningQuickStatsBuilderMock));
            // Create a session where an inline build will occur for any newly requested partition
            ConnectorSession session = getSession("300ms", "0ms");
            List<String> testPartitions = ImmutableList.copyOf(mockPerPartitionStatsFetchTimes.keySet());
            Map<String, PartitionStatistics> quickStats = quickStatsProvider.getQuickStats(session,
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

            QuickStatsProvider quickStatsProvider = new QuickStatsProvider(metastoreMock, hdfsEnvironment, directoryListerMock, hiveClientConfig, new NamenodeStats(),
                    ImmutableList.of((session1, metastore, table, metastoreContext, partitionId, files) -> mockPartitionQuickStats));

            Map<String, PartitionStatistics> quickStats = quickStatsProvider.getQuickStats(session,
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
                        Map<String, PartitionStatistics> quickStatsAfter = quickStatsProvider.getQuickStats(session,
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

    @Test
    public void testFollowSymlinkFile()
            throws IOException
    {
        java.nio.file.Path testTempDir = createTempDirectory("test");
        java.nio.file.Path symlinkFileDir = testTempDir.resolve("symlink");
        java.nio.file.Path tableDir1 = testTempDir.resolve("table_1");
        java.nio.file.Path tableDir2 = testTempDir.resolve("table_2");
        createDirectory(symlinkFileDir);
        createDirectory(tableDir1);
        createDirectory(tableDir2);

        // Copy a parquet file from resources to the test/table temp dir
        String fileName1 = "data_1.parquet";
        String fileName2 = "data_2.parquet";
        URL resourceUrl1 = getResource("quick_stats/tpcds_store_sales_sf_point_01/20230706_110621_00007_4uxkh_10e94cd0-1f67-4440-afd0-75cd328ea570");
        URL resourceUrl2 = getResource("quick_stats/tpcds_store_sales_sf_point_01/20230706_110621_00007_4uxkh_12b3ec73-4952-4df7-9987-2beb20cd5953");
        assertNotNull(resourceUrl1);
        assertNotNull(resourceUrl2);

        java.nio.file.Path targetFilePath1 = tableDir1.resolve(fileName1);
        java.nio.file.Path targetFilePath2 = tableDir2.resolve(fileName2);

        try (InputStream in = resourceUrl1.openStream()) {
            copy(in, targetFilePath1, REPLACE_EXISTING);
        }
        try (InputStream in = resourceUrl2.openStream()) {
            copy(in, targetFilePath2, REPLACE_EXISTING);
        }

        // Create the symlink manifest pointing to data.parquet
        java.nio.file.Path manifestFilePath = createFile(symlinkFileDir.resolve("manifest"));
        try (BufferedWriter writer = newBufferedWriter(manifestFilePath, CREATE, TRUNCATE_EXISTING)) {
            writer.write("file:" + tableDir1 + "/" + fileName1);
            writer.newLine();
            writer.write("file:" + tableDir2 + "/" + fileName2);
        }

        String symlinkTableName = "symlink_table";
        Table symlinkTable = new Table(
                Optional.of("catalogName"),
                TEST_SCHEMA,
                symlinkTableName,
                "owner",
                EXTERNAL_TABLE,
                Storage.builder()
                        .setStorageFormat(StorageFormat.create(ParquetHiveSerDe.class.getName(), SymlinkTextInputFormat.class.getName(), HiveIgnoreKeyTextOutputFormat.class.getName()))
                        .setLocation(symlinkFileDir.toString())
                        .build(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty());

        metastoreMock.createTable(metastoreContext, symlinkTable, new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of()), ImmutableList.of());

        DirectoryLister directoryLister = new HadoopDirectoryLister();

        QuickStatsBuilder quickStatsBuilder = (session1, metastore, table, metastoreContext, partitionId, files) -> {
            List<HiveFileInfo> fileInfoList = ImmutableList.copyOf(files);
            assertEquals(fileInfoList.size(), 2);
            for (HiveFileInfo hiveFileInfo : fileInfoList) {
                assertTrue(hiveFileInfo.getPath().equals("file:" + targetFilePath1) || hiveFileInfo.getPath().equals("file:" + targetFilePath2));
            }
            return new PartitionQuickStats(UNPARTITIONED_ID.getPartitionName(), ImmutableList.of(mockIntegerColumnStats), fileInfoList.size());
        };

        QuickStatsProvider quickStatsProvider = new QuickStatsProvider(metastoreMock, hdfsEnvironment, directoryLister, hiveClientConfig, new NamenodeStats(),
                ImmutableList.of(quickStatsBuilder));

        SchemaTableName table = new SchemaTableName(TEST_SCHEMA, symlinkTableName);

        Table resolvedTable = metastoreMock.getTable(metastoreContext, table.getSchemaName(), table.getTableName()).get();
        Path symlinkTablePath = new Path(resolvedTable.getStorage().getLocation());
        HdfsContext hdfsContext = new HdfsContext(SESSION, table.getSchemaName(), table.getTableName(), UNPARTITIONED_ID.getPartitionName(), false);
        ExtendedFileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, symlinkTablePath);
        HiveDirectoryContext hiveDirectoryContext = new HiveDirectoryContext(RECURSE, isUseListDirectoryCache(SESSION),
                isSkipEmptyFilesEnabled(SESSION), hdfsContext.getIdentity(), buildDirectoryContextProperties(SESSION), SESSION.getRuntimeStats());

        // Test directoryLister finds the manifest file in the table dir
        Iterator<HiveFileInfo> fileInfoIterator = directoryLister.list(fs, resolvedTable, symlinkTablePath, Optional.empty(), new NamenodeStats(), hiveDirectoryContext);
        ImmutableList<HiveFileInfo> fileInfoList = ImmutableList.copyOf(fileInfoIterator);

        assertEquals(fileInfoList.size(), 1);
        assertEquals(fileInfoList.get(0).getPath(), "file:" + manifestFilePath);
        assertEquals(fileInfoList.get(0).getParent(), "file:" + symlinkFileDir);

        // Test that the input format is correct
        InputFormat<?, ?> inputFormat = getInputFormat(
                hdfsEnvironment.getConfiguration(hdfsContext, symlinkTablePath),
                resolvedTable.getStorage().getStorageFormat().getInputFormat(),
                resolvedTable.getStorage().getStorageFormat().getSerDe(),
                false);

        assertTrue(inputFormat instanceof SymlinkTextInputFormat);

        // Test entire getQuickStats and ensure file count matches fileList size in buildQuickStats
        PartitionStatistics quickStats = quickStatsProvider.getQuickStats(SESSION, table, metastoreContext, UNPARTITIONED_ID.getPartitionName());

        assertTrue(quickStats.getBasicStatistics().getFileCount().isPresent());
        assertEquals(quickStats.getBasicStatistics().getFileCount().getAsLong(), 2L);

        deleteRecursively(testTempDir, ALLOW_INSECURE);
    }
}
