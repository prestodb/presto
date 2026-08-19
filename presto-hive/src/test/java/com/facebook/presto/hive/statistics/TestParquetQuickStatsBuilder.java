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
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.HiveFileInfo;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.TestingExtendedHiveMetastore;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.chrono.ChronoLocalDate;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.hive.HiveCommonSessionProperties.READ_MASKED_VALUE_ENABLED;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveTestUtils.createTestHdfsEnvironment;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.System.exit;
import static java.time.LocalDate.parse;
import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestParquetQuickStatsBuilder
{
    public static final ConnectorSession SESSION = new TestingConnectorSession(ImmutableList.of(booleanProperty(
            READ_MASKED_VALUE_ENABLED,
            "Return null when access is denied for an encrypted parquet column",
            false,
            false)));
    public static final String TEST_SCHEMA = "test_schema";
    public static final String TEST_TABLE = "quick_stats";
    private ParquetQuickStatsBuilder parquetQuickStatsBuilder;
    private MetastoreContext metastoreContext;
    private ExtendedHiveMetastore metastore;
    private HdfsEnvironment hdfsEnvironment;
    private HiveClientConfig hiveClientConfig;
    private MetastoreClientConfig metastoreClientConfig;

    public static void main(String[] args)
            throws Exception
    {
        benchmarkS3ReadsDriver();

        exit(0);
    }

    /**
     * Micro benchmark for performance of the ParquetQuickStats builder
     */
    private static void benchmarkS3ReadsDriver()
    {
        TestParquetQuickStatsBuilder testParquetQuickStatsBuilder = new TestParquetQuickStatsBuilder();
        String s3BucketUri = "s3://some-bucket";
        String s3Directory = "/path/to/partition";

        for (int i = 0; i < 5; i++) {
            // Do some warmup reads
            testParquetQuickStatsBuilder.benchmarkS3Reads(1, true, s3BucketUri, s3Directory);
        }

        testParquetQuickStatsBuilder.benchmarkS3Reads(10, false, s3BucketUri, s3Directory);
    }

    private static ColumnQuickStats<ChronoLocalDate> createDateStats(String columnName, long nullsCount, LocalDate minDate, LocalDate maxDate)
    {
        ColumnQuickStats<ChronoLocalDate> result = new ColumnQuickStats<>(columnName, ChronoLocalDate.class);
        result.addToNullsCount(nullsCount);
        result.setMinValue(minDate);
        result.setMaxValue(maxDate);

        return result;
    }

    private static ColumnQuickStats<Long> createLongStats(String columnName, long nullsCount, long min, long max)
    {
        ColumnQuickStats<Long> result = new ColumnQuickStats<>(columnName, Long.class);
        result.addToNullsCount(nullsCount);
        result.setMinValue(min);
        result.setMaxValue(max);

        return result;
    }

    private static ColumnQuickStats<Integer> createIntegerStats(String columnName, long nullsCount, int min, int max)
    {
        ColumnQuickStats<Integer> result = new ColumnQuickStats<>(columnName, Integer.class);
        result.addToNullsCount(nullsCount);
        result.setMinValue(min);
        result.setMaxValue(max);

        return result;
    }

    private static ColumnQuickStats<Slice> createBinaryStats(String columnName, long nullsCount)
    {
        ColumnQuickStats<Slice> result = new ColumnQuickStats<>(columnName, Slice.class);
        result.addToNullsCount(nullsCount);

        return result;
    }

    private static ColumnQuickStats<Double> createDoubleStats(String columnName, long nullsCount, double min, double max)
    {
        ColumnQuickStats<Double> result = new ColumnQuickStats<>(columnName, Double.class);
        result.addToNullsCount(nullsCount);
        result.setMinValue(min);
        result.setMaxValue(max);

        return result;
    }

    private ImmutableList<HiveFileInfo> buildHiveFileInfos(String basePath, String partitionDir, int repeatCount)
    {
        ImmutableList.Builder<HiveFileInfo> fileInfoBuilder = ImmutableList.builder();
        Path fullPath = new Path(basePath + "/" + partitionDir);

        try (FileSystem fs = hdfsEnvironment.getFileSystem(new HdfsContext(SESSION), new Path(basePath))) {
            RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(fullPath, true);

            while (fileList.hasNext()) {
                LocatedFileStatus fileStatus = fileList.next();
                // Add each discovered file repeatCount times - useful for simulating a large file test
                for (int i = 0; i < repeatCount; i++) {
                    fileInfoBuilder.add(HiveFileInfo.createHiveFileInfo(fileStatus, Optional.empty()));
                }
            }
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return fileInfoBuilder.build();
    }

    @BeforeTest
    private void setUp()
    {
        Table table = new Table(
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

        metastoreContext = new MetastoreContext(SESSION.getUser(),
                SESSION.getQueryId(),
                Optional.empty(),
                Collections.emptySet(),
                Optional.empty(),
                Optional.empty(),
                false,
                HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER,
                SESSION.getWarningCollector(),
                SESSION.getRuntimeStats());
        ExtendedHiveMetastore mock = new TestingExtendedHiveMetastore();
        mock.createTable(metastoreContext, table, new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of()), ImmutableList.of());
        metastore = mock;

        hiveClientConfig = new HiveClientConfig();
        metastoreClientConfig = new MetastoreClientConfig();
        // Use HiveUtils#createTestHdfsEnvironment to ensure that PrestoS3FileSystem is used for s3a paths
        hdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig, metastoreClientConfig);

        parquetQuickStatsBuilder = new ParquetQuickStatsBuilder(new FileFormatDataSourceStats(), hdfsEnvironment, hiveClientConfig);
    }

    public void benchmarkS3Reads(int mockedFileCount, boolean isWarmup, String s3BucketUri, String partitionPath)
    {
        setUp();
        ImmutableList<HiveFileInfo> hiveFileInfos = buildHiveFileInfos(s3BucketUri, partitionPath, mockedFileCount);

        Stopwatch sw = Stopwatch.createStarted();
        PartitionQuickStats partitionQuickStats = parquetQuickStatsBuilder.buildQuickStats(SESSION, metastore, new SchemaTableName(TEST_SCHEMA, TEST_TABLE),
                metastoreContext, UNPARTITIONED_ID.getPartitionName(), hiveFileInfos.iterator());
        sw.stop();

        if (!isWarmup) {
            System.out.printf("For %d files, took %d ms%n", mockedFileCount, sw.elapsed(TimeUnit.MILLISECONDS));
            System.out.println(partitionQuickStats);
        }
        else {
            System.out.println("Warmup..");
        }
    }

    @Test
    public void testStatsBuildTimeIsBoundedUsingFooterFetchTimeout()
    {
        HiveClientConfig customHiveClientConfig = new HiveClientConfig().setParquetQuickStatsFileMetadataFetchTimeout(new Duration(10, TimeUnit.MILLISECONDS));
        HdfsEnvironment mockHdfsEnvironment = new DelayingHdfsEnvironment(hdfsEnvironment, hiveClientConfig, metastoreClientConfig);

        String resourceDir = TestParquetQuickStatsBuilder.class.getClassLoader().getResource("quick_stats").toString();
        ParquetQuickStatsBuilder customParquetQuickStatsBuilder = new ParquetQuickStatsBuilder(new FileFormatDataSourceStats(), mockHdfsEnvironment, customHiveClientConfig);
        ImmutableList<HiveFileInfo> hiveFileInfos = buildHiveFileInfos(resourceDir, "tpcds_store_sales_sf_point_01", 1);

        try {
            customParquetQuickStatsBuilder.buildQuickStats(SESSION, metastore, new SchemaTableName(TEST_SCHEMA, TEST_TABLE),
                    metastoreContext, UNPARTITIONED_ID.getPartitionName(), hiveFileInfos.iterator());
        }
        catch (RuntimeException ex) {
            assertEquals(TimeoutException.class, ex.getCause().getClass());
        }
    }

    @Test
    public void testStatsAreBuiltFromFooters()
    {
        String resourceDir = TestParquetQuickStatsBuilder.class.getClassLoader().getResource("quick_stats").toString();

        // Table :  TPCDS SF 0.01 store_sales
        ImmutableList<HiveFileInfo> hiveFileInfos = buildHiveFileInfos(resourceDir, "tpcds_store_sales_sf_point_01", 1);
        PartitionQuickStats partitionQuickStats = parquetQuickStatsBuilder.buildQuickStats(SESSION, metastore, new SchemaTableName(TEST_SCHEMA, TEST_TABLE),
                metastoreContext, UNPARTITIONED_ID.getPartitionName(), hiveFileInfos.iterator());

        assertEquals(partitionQuickStats.getFileCount(), 8);
        assertEquals(partitionQuickStats.getRowCount(), 120527L);
        // We check a few of the columns
        Map<String, ? extends ColumnQuickStats<?>> columnQuickStatsMap = partitionQuickStats.getStats().stream().collect(toMap(ColumnQuickStats::getColumnName, v -> v));
        assertEquals(columnQuickStatsMap.get("ss_promo_sk"), createLongStats("ss_promo_sk", 5303L, 1L, 3L));
        assertEquals(columnQuickStatsMap.get("ss_sold_date_sk"), createLongStats("ss_sold_date_sk", 5335L, 2450816L, 2452642L));
        assertEquals(columnQuickStatsMap.get("ss_quantity"), createIntegerStats("ss_quantity", 5450L, 1, 100));
        // DECIMAL columns are stored as binary arrays in parquet
        assertEquals(columnQuickStatsMap.get("ss_wholesale_cost"), createBinaryStats("ss_wholesale_cost", 5369L));

        // NDV is bounded by the [min, max] range where it is narrower than the non-null row count.
        assertEquals(estimateNdv(partitionQuickStats, columnQuickStatsMap.get("ss_sold_date_sk")), OptionalLong.of(1827L));
        assertEquals(estimateNdv(partitionQuickStats, columnQuickStatsMap.get("ss_promo_sk")), OptionalLong.of(3L));
        assertEquals(estimateNdv(partitionQuickStats, columnQuickStatsMap.get("ss_quantity")), OptionalLong.of(100L));
        // No min/max is collected for DECIMAL, so it has no NDV.
        assertFalse(estimateNdv(partitionQuickStats, columnQuickStatsMap.get("ss_wholesale_cost")).isPresent());
        // Table : TPCH orders table; 100 rows
        hiveFileInfos = buildHiveFileInfos(resourceDir, "tpch_orders_100_rows", 1);
        partitionQuickStats = parquetQuickStatsBuilder.buildQuickStats(SESSION, metastore, new SchemaTableName(TEST_SCHEMA, TEST_TABLE),
                metastoreContext, UNPARTITIONED_ID.getPartitionName(), hiveFileInfos.iterator());

        assertEquals(partitionQuickStats.getFileCount(), 1);
        assertEquals(partitionQuickStats.getRowCount(), 100L);
        columnQuickStatsMap = partitionQuickStats.getStats().stream().collect(toMap(ColumnQuickStats::getColumnName, v -> v));
        // VARCHAR columns are stored as binary arrays in parquet
        assertEquals(columnQuickStatsMap.get("comment"), createBinaryStats("comment", 0L));
        assertEquals(columnQuickStatsMap.get("orderdate"), createDateStats("orderdate", 0L, parse("1992-01-29"), parse("1998-07-24")));
        assertEquals(columnQuickStatsMap.get("totalprice"), createDoubleStats("totalprice", 0L, 1373.4, 352797.28));

        assertFalse(estimateNdv(partitionQuickStats, columnQuickStatsMap.get("comment")).isPresent());
        assertEquals(estimateNdv(partitionQuickStats, columnQuickStatsMap.get("orderdate")), OptionalLong.of(100L));
        assertEquals(estimateNdv(partitionQuickStats, columnQuickStatsMap.get("totalprice")), OptionalLong.of(100L));
    }

    @Test
    public void testStatsFromNestedColumnsAreNotIncluded()
    {
        String resourceDir = TestParquetQuickStatsBuilder.class.getClassLoader().getResource("quick_stats").toString();

        // Table definition :
        // CREATE TABLE nested_parquet(
        //     id bigint,
        //     x row(a bigint, b varchar, c double, d row(d1 bigint, d2 double)),
        //     y array(row(a bigint, b varchar, c double, d row(d1 bigint, d2 double))))
        //     with (format = 'PARQUET')
        // 3  rows were added to the table
        ImmutableList<HiveFileInfo> hiveFileInfos = buildHiveFileInfos(resourceDir, "nested_table", 1);
        PartitionQuickStats partitionQuickStats = parquetQuickStatsBuilder.buildQuickStats(SESSION, metastore, new SchemaTableName(TEST_SCHEMA, TEST_TABLE),
                metastoreContext, UNPARTITIONED_ID.getPartitionName(), hiveFileInfos.iterator());

        assertEquals(partitionQuickStats.getStats().size(), 1, "Expected stats for only non-nested column : 'id'");
        assertEquals(partitionQuickStats.getRowCount(), 3L);
        ColumnQuickStats<?> idColumnQuickStats = partitionQuickStats.getStats().get(0);
        assertEquals(idColumnQuickStats, createLongStats("id", 0L, 1L, 3L));
    }

    private static OptionalLong estimateNdv(PartitionQuickStats partition, ColumnQuickStats<?> column)
    {
        return column.getDistinctValuesCount(partition.getRowCount() - column.getNullsCount());
    }

    public static class DelayingHdfsEnvironment
            extends HdfsEnvironment
    {
        private final HdfsEnvironment hdfsEnvironment;

        public DelayingHdfsEnvironment(HdfsEnvironment hdfsEnvironment, HiveClientConfig hiveClientConfig, MetastoreClientConfig metastoreClientConfig)
        {
            super(
                    new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig), ImmutableSet.of(), hiveClientConfig),
                    metastoreClientConfig,
                    new NoHdfsAuthentication());
            this.hdfsEnvironment = hdfsEnvironment;
        }

        @Override
        public ExtendedFileSystem getFileSystem(String user, Path path, Configuration configuration)
                throws IOException
        {
            sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
            return hdfsEnvironment.getFileSystem(user, path, configuration);
        }
    }
}
