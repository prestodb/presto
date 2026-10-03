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
import com.facebook.presto.hive.metastore.Column;
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
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.chrono.ChronoLocalDate;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.hive.HiveCommonSessionProperties.READ_MASKED_VALUE_ENABLED;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HiveSessionProperties.QUICK_STATS_PROVABLE_EMPTY_ENABLED;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveStorageFormat.TEXTFILE;
import static com.facebook.presto.hive.HiveTestUtils.createTestHdfsEnvironment;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.System.exit;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.write;
import static java.time.LocalDate.parse;
import static java.util.Collections.emptyIterator;
import static java.util.stream.Collectors.toMap;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestParquetQuickStatsBuilder
{
    private static final List<PropertyMetadata<?>> SESSION_PROPERTIES = ImmutableList.of(
            booleanProperty(
                    READ_MASKED_VALUE_ENABLED,
                    "Return null when access is denied for an encrypted parquet column",
                    false,
                    false),
            booleanProperty(
                    QUICK_STATS_PROVABLE_EMPTY_ENABLED,
                    "Emit a row count of 0 when quick stats can prove a partition is empty",
                    true,
                    false));
    public static final ConnectorSession SESSION = new TestingConnectorSession(SESSION_PROPERTIES);
    public static final String TEST_SCHEMA = "test_schema";
    public static final String TEST_TABLE = "quick_stats";
    // Tables used by the this change format-gate tests; all created in setUp() below.
    private static final String TEXT_TABLE = "quick_stats_text";
    private static final String HUDI_TABLE = "quick_stats_hudi";
    private static final String ACID_TABLE = "quick_stats_acid";
    private static final String PARTITIONED_TABLE = "quick_stats_partitioned";
    private static final String PARTITION_NAME = "ds=2026-08-01";
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

    private static ColumnQuickStats<ChronoLocalDate> createDateStats(String columnName, long rowCount, long nullsCount, LocalDate minDate, LocalDate maxDate)
    {
        ColumnQuickStats<ChronoLocalDate> result = new ColumnQuickStats<>(columnName, ChronoLocalDate.class);
        result.addToRowCount(rowCount);
        result.addToNullsCount(nullsCount);
        result.setMinValue(minDate);
        result.setMaxValue(maxDate);

        return result;
    }

    private static ColumnQuickStats<Long> createLongStats(String columnName, long rowCount, long nullsCount, long min, long max)
    {
        ColumnQuickStats<Long> result = new ColumnQuickStats<>(columnName, Long.class);
        result.addToRowCount(rowCount);
        result.addToNullsCount(nullsCount);
        result.setMinValue(min);
        result.setMaxValue(max);

        return result;
    }

    private static ColumnQuickStats<Integer> createIntegerStats(String columnName, long rowCount, long nullsCount, int min, int max)
    {
        ColumnQuickStats<Integer> result = new ColumnQuickStats<>(columnName, Integer.class);
        result.addToRowCount(rowCount);
        result.addToNullsCount(nullsCount);
        result.setMinValue(min);
        result.setMaxValue(max);

        return result;
    }

    private static ColumnQuickStats<Slice> createBinaryStats(String columnName, long rowCount, long nullsCount)
    {
        ColumnQuickStats<Slice> result = new ColumnQuickStats<>(columnName, Slice.class);
        result.addToRowCount(rowCount);
        result.addToNullsCount(nullsCount);

        return result;
    }

    private static ColumnQuickStats<Double> createDoubleStats(String columnName, long rowCount, long nullsCount, double min, double max)
    {
        ColumnQuickStats<Double> result = new ColumnQuickStats<>(columnName, Double.class);
        result.addToRowCount(rowCount);
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

    // PropertyMetadata#booleanProperty decodes with Boolean.class::cast, so the value must be a
    // Boolean rather than its String form.
    private static ConnectorSession session(boolean provableEmptyEnabled)
    {
        return new TestingConnectorSession(SESSION_PROPERTIES, ImmutableMap.of(QUICK_STATS_PROVABLE_EMPTY_ENABLED, provableEmptyEnabled));
    }

    private static Table table(String tableName, StorageFormat storageFormat, Map<String, String> parameters, List<Column> partitionColumns)
    {
        return new Table(
                Optional.of("catalogName"),
                TEST_SCHEMA,
                tableName,
                "owner",
                MANAGED_TABLE,
                Storage.builder()
                        .setStorageFormat(storageFormat)
                        .setLocation("location")
                        .build(),
                ImmutableList.of(),
                partitionColumns,
                parameters,
                Optional.empty(),
                Optional.empty());
    }

    /**
     * Writes a Parquet file with {@code rowCount} rows of the given schema. A file written with zero
     * rows has zero row groups in its footer, which is the "files exist but hold no rows" case this change
     * must recognise as provable emptiness.
     */
    private static void writeParquetFile(java.nio.file.Path file, MessageType schema, int rowCount)
            throws IOException
    {
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.toString()))
                .withConf(new Configuration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()) {
            for (int i = 0; i < rowCount; i++) {
                Group group = groupFactory.newGroup();
                for (org.apache.parquet.schema.Type field : schema.getFields()) {
                    if (field.isPrimitive()) {
                        group.add(field.getName(), (long) i);
                    }
                    else {
                        group.addGroup(field.getName()).add(0, (long) i);
                    }
                }
                writer.write(group);
            }
        }
    }

    @BeforeTest
    private void setUp()
    {
        Table table = table(TEST_TABLE, fromHiveStorageFormat(PARQUET), ImmutableMap.of(), ImmutableList.of());

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
        PrincipalPrivileges noPrivileges = new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of());
        mock.createTable(metastoreContext, table, noPrivileges, ImmutableList.of());
        // A non-Parquet serde: quick stats must stay UNKNOWN for it (never PROVABLY_EMPTY).
        mock.createTable(metastoreContext, table(TEXT_TABLE, fromHiveStorageFormat(TEXTFILE), ImmutableMap.of(), ImmutableList.of()), noPrivileges, ImmutableList.of());
        // Hudi: a Parquet serde behind a Hoodie input format, whose visible file set comes from the
        // commit timeline rather than a plain listing, so "no files" proves nothing.
        mock.createTable(
                metastoreContext,
                table(HUDI_TABLE,
                        StorageFormat.create(ParquetHiveSerDe.class.getName(), HoodieParquetInputFormat.class.getName(), HiveIgnoreKeyTextOutputFormat.class.getName()),
                        ImmutableMap.of(),
                        ImmutableList.of()),
                noPrivileges,
                ImmutableList.of());
        // ACID/transactional: data lives in nested base_*/delta_* directories which the default
        // nested-directory policy ignores, so a non-empty table can list as zero files.
        mock.createTable(
                metastoreContext,
                table(ACID_TABLE, fromHiveStorageFormat(PARQUET), ImmutableMap.of("transactional", "true"), ImmutableList.of()),
                noPrivileges,
                ImmutableList.of());
        // A partitioned Parquet table, to exercise the getPartitionsByNames branch of the storage
        // format resolution as well as the table-level ACID lookup for a partition.
        mock.createTable(
                metastoreContext,
                table(PARTITIONED_TABLE, fromHiveStorageFormat(PARQUET), ImmutableMap.of(), ImmutableList.of(new Column("ds", HIVE_STRING, Optional.empty(), Optional.empty()))),
                noPrivileges,
                ImmutableList.of());
        mock.addPartitions(
                metastoreContext,
                TEST_SCHEMA,
                PARTITIONED_TABLE,
                ImmutableList.of(new PartitionWithStatistics(
                        new Partition(
                                Optional.of("catalogName"),
                                TEST_SCHEMA,
                                PARTITIONED_TABLE,
                                ImmutableList.of("2026-08-01"),
                                new Storage(fromHiveStorageFormat(PARQUET), "location/ds=2026-08-01", Optional.empty(), true, ImmutableMap.of(), ImmutableMap.of()),
                                ImmutableList.of(),
                                ImmutableMap.of(),
                                Optional.empty(),
                                false,
                                true,
                                0,
                                0,
                                Optional.empty()),
                        PARTITION_NAME,
                        PartitionStatistics.empty())));
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

        assertEquals(8, partitionQuickStats.getFileCount());
        // We check a few of the columns
        Map<String, ? extends ColumnQuickStats<?>> columnQuickStatsMap = partitionQuickStats.getStats().stream().collect(toMap(ColumnQuickStats::getColumnName, v -> v));
        assertEquals(columnQuickStatsMap.get("ss_promo_sk"), createLongStats("ss_promo_sk", 120527L, 5303L, 1L, 3L));
        assertEquals(columnQuickStatsMap.get("ss_sold_date_sk"), createLongStats("ss_sold_date_sk", 120527L, 5335L, 2450816L, 2452642L));
        assertEquals(columnQuickStatsMap.get("ss_quantity"), createIntegerStats("ss_quantity", 120527L, 5450L, 1, 100));
        // DECIMAL columns are stored as binary arrays in parquet
        assertEquals(columnQuickStatsMap.get("ss_wholesale_cost"), createBinaryStats("ss_wholesale_cost", 120527L, 5369L));

        // NDV assertions: ss_sold_date_sk is a bigint surrogate key column, structurally identical
        // to a join key like `order_id` -- min/max are known and the value range (1827) is much
        // narrower than the non-null row count (115192), so the range bound should be emitted as a
        // conservative, finite NDV instead of leaving distinctValuesCount unset (which previously
        // surfaced as NaN to the cost-based optimizer and triggered a cross-join fallback).
        assertEquals(columnQuickStatsMap.get("ss_sold_date_sk").getDistinctValuesCount(), OptionalLong.of(1827L));
        assertEquals(columnQuickStatsMap.get("ss_promo_sk").getDistinctValuesCount(), OptionalLong.of(3L));
        assertEquals(columnQuickStatsMap.get("ss_quantity").getDistinctValuesCount(), OptionalLong.of(100L));
        // DECIMAL/binary columns have no min/max collected, so NDV remains unset (unchanged).
        assertFalse(columnQuickStatsMap.get("ss_wholesale_cost").getDistinctValuesCount().isPresent());
        for (ColumnQuickStats<?> columnQuickStats : columnQuickStatsMap.values()) {
            OptionalLong distinctValuesCount = columnQuickStats.getDistinctValuesCount();
            assertTrue(!distinctValuesCount.isPresent() || distinctValuesCount.getAsLong() >= 0);
            assertTrue(!distinctValuesCount.isPresent() || distinctValuesCount.getAsLong() <= columnQuickStats.getRowCount());
        }

        // Table : TPCH orders table; 100 rows
        hiveFileInfos = buildHiveFileInfos(resourceDir, "tpch_orders_100_rows", 1);
        partitionQuickStats = parquetQuickStatsBuilder.buildQuickStats(SESSION, metastore, new SchemaTableName(TEST_SCHEMA, TEST_TABLE),
                metastoreContext, UNPARTITIONED_ID.getPartitionName(), hiveFileInfos.iterator());

        assertEquals(1, partitionQuickStats.getFileCount());
        columnQuickStatsMap = partitionQuickStats.getStats().stream().collect(toMap(ColumnQuickStats::getColumnName, v -> v));
        // VARCHAR columns are stored as binary arrays in parquet
        assertEquals(columnQuickStatsMap.get("comment"), createBinaryStats("comment", 100L, 0L));
        assertEquals(columnQuickStatsMap.get("orderdate"), createDateStats("orderdate", 100L, 0L, parse("1992-01-29"), parse("1998-07-24")));
        assertEquals(columnQuickStatsMap.get("totalprice"), createDoubleStats("totalprice", 100L, 0L, 1373.4, 352797.28));

        // VARCHAR (comment) has no min/max, so NDV remains unset (unchanged behavior).
        assertFalse(columnQuickStatsMap.get("comment").getDistinctValuesCount().isPresent());
        // DATE and DOUBLE columns get a conservative NDV bound instead of NaN.
        assertEquals(columnQuickStatsMap.get("orderdate").getDistinctValuesCount(), OptionalLong.of(100L));
        assertEquals(columnQuickStatsMap.get("totalprice").getDistinctValuesCount(), OptionalLong.of(100L));
    }

    @Test
    public void testDistinctValuesCountForBigintJoinKeyLikeColumn()
    {
        // Reproduces the reported failure mode: a bigint join key (e.g. order_id) with real
        // min/max but, prior to this fix, no distinctValuesCount -- which surfaced as NaN to the
        // cost-based optimizer and forced a cross-join x default-selectivity fallback estimate.
        ColumnQuickStats<Long> orderId = createLongStats("order_id", 559_000_000L, 0L, 314L, 98_319_154L);

        OptionalLong distinctValuesCount = orderId.getDistinctValuesCount();
        assertTrue(distinctValuesCount.isPresent(), "Expected a conservative NDV bound instead of an unset/NaN distinctValuesCount");
        assertTrue(distinctValuesCount.getAsLong() >= 0);
        assertTrue(distinctValuesCount.getAsLong() <= orderId.getRowCount());
        assertEquals(distinctValuesCount, OptionalLong.of(98_319_154L - 314L + 1L));
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
        ColumnQuickStats<?> idColumnQuickStats = partitionQuickStats.getStats().get(0);
        assertEquals(idColumnQuickStats, createLongStats("id", 3L, 0L, 1L, 3L));
    }

    private PartitionQuickStats buildQuickStats(ConnectorSession session, String tableName, String partitionId, Iterator<HiveFileInfo> files)
    {
        return parquetQuickStatsBuilder.buildQuickStats(session, metastore, new SchemaTableName(TEST_SCHEMA, tableName), metastoreContext, partitionId, files);
    }

    /**
     * A partition whose directory listing found no files at all contains no rows, and
     * that is a proof rather than an estimate. Covers both the unpartitioned and the partitioned
     * storage-format resolution paths.
     */
    @Test
    public void testNoFilesIsProvablyEmpty()
    {
        assertSame(
                buildQuickStats(session(true), TEST_TABLE, UNPARTITIONED_ID.getPartitionName(), emptyIterator()),
                PartitionQuickStats.PROVABLY_EMPTY);
        assertSame(
                buildQuickStats(session(true), PARTITIONED_TABLE, PARTITION_NAME, emptyIterator()),
                PartitionQuickStats.PROVABLY_EMPTY);
    }

    /**
     * With the kill-switch off, the same input reverts to the pre-change
     * UNKNOWN sentinel.
     */
    @Test
    public void testNoFilesIsUnknownWhenKillSwitchIsOff()
    {
        assertSame(
                buildQuickStats(session(false), TEST_TABLE, UNPARTITIONED_ID.getPartitionName(), emptyIterator()),
                PartitionQuickStats.EMPTY);
        assertSame(
                buildQuickStats(session(false), PARTITIONED_TABLE, PARTITION_NAME, emptyIterator()),
                PartitionQuickStats.EMPTY);
    }

    /**
     * The format gate. "No files" does not imply "no rows" for a Hudi table
     * (file set derived from the commit timeline) or for a transactional/ACID table (data in nested
     * base / delta directories that the default nested-directory policy ignores), and the
     * non-Parquet serde case must stay UNKNOWN as before.
     */
    @Test
    public void testProvablyEmptyOnlyForSafeFormats()
    {
        assertSame(
                buildQuickStats(session(true), TEXT_TABLE, UNPARTITIONED_ID.getPartitionName(), emptyIterator()),
                PartitionQuickStats.EMPTY,
                "A non-Parquet serde must never be reported as provably empty");
        assertSame(
                buildQuickStats(session(true), HUDI_TABLE, UNPARTITIONED_ID.getPartitionName(), emptyIterator()),
                PartitionQuickStats.EMPTY,
                "A Hudi table must never be reported as provably empty");
        assertSame(
                buildQuickStats(session(true), ACID_TABLE, UNPARTITIONED_ID.getPartitionName(), emptyIterator()),
                PartitionQuickStats.EMPTY,
                "A transactional/ACID table must never be reported as provably empty");
    }

    /**
     * If the table itself cannot be resolved we can prove nothing, so fall back
     * to UNKNOWN rather than assuming the ACID/Hudi gates do not apply. Reachable for a partitioned
     * table whose partition is still in the metastore while the table entry has gone.
     */
    @Test
    public void testUnresolvableTableProvesNothing()
    {
        String orphanedTable = "quick_stats_orphaned_partition";
        metastore.addPartitions(
                metastoreContext,
                TEST_SCHEMA,
                orphanedTable,
                ImmutableList.of(new PartitionWithStatistics(
                        new Partition(
                                Optional.of("catalogName"),
                                TEST_SCHEMA,
                                orphanedTable,
                                ImmutableList.of("2026-08-01"),
                                new Storage(fromHiveStorageFormat(PARQUET), "location/ds=2026-08-01", Optional.empty(), true, ImmutableMap.of(), ImmutableMap.of()),
                                ImmutableList.of(),
                                ImmutableMap.of(),
                                Optional.empty(),
                                false,
                                true,
                                0,
                                0,
                                Optional.empty()),
                        PARTITION_NAME,
                        PartitionStatistics.empty())));

        assertSame(
                buildQuickStats(session(true), orphanedTable, PARTITION_NAME, emptyIterator()),
                PartitionQuickStats.EMPTY);
    }

    /**
     * Files that exist but hold zero row groups also prove zero rows. This is the
     * second of the three cases that must not be conflated (the roll-up is empty because there was nothing
     * to roll up, not because reading failed -- a read failure throws, see
     * {@link #testFooterFailureStillThrows()}).
     */
    @Test
    public void testFilesWithNoRowGroupsAreProvablyEmpty()
            throws IOException
    {
        java.nio.file.Path dir = createTempDirectory("provably-empty");
        try {
            MessageType schema = Types.buildMessage()
                    .addField(Types.optional(INT64).named("id"))
                    .named("zero_rows");
            writeParquetFile(dir.resolve("data.parquet"), schema, 0);

            ImmutableList<HiveFileInfo> files = buildHiveFileInfos(dir.toUri().toString(), "", 1);
            assertEquals(files.size(), 1, "expected exactly the one zero-row file we wrote");

            assertSame(
                    buildQuickStats(session(true), TEST_TABLE, UNPARTITIONED_ID.getPartitionName(), files.iterator()),
                    PartitionQuickStats.PROVABLY_EMPTY);
            // Kill-switch off: byte-identical legacy behavior (UNKNOWN).
            assertSame(
                    buildQuickStats(session(false), TEST_TABLE, UNPARTITIONED_ID.getPartitionName(), files.iterator()),
                    PartitionQuickStats.EMPTY);
            // And the format gate still applies to this path.
            assertSame(
                    buildQuickStats(session(true), ACID_TABLE, UNPARTITIONED_ID.getPartitionName(), files.iterator()),
                    PartitionQuickStats.EMPTY);
        }
        finally {
            deleteRecursively(dir, ALLOW_INSECURE);
        }
    }

    /**
     * An all-nested-columns schema also produces an empty roll-up (nested columns are
     * skipped), but with row groups present -- so it means "we could not read anything useful", not
     * "there are no rows". It must stay UNKNOWN.
     */
    @Test
    public void testAllNestedColumnsStaysUnknown()
            throws IOException
    {
        java.nio.file.Path dir = createTempDirectory("all-nested");
        try {
            MessageType schema = Types.buildMessage()
                    .addField(Types.optionalGroup().addField(Types.optional(INT64).named("a")).named("nested"))
                    .named("only_nested");
            writeParquetFile(dir.resolve("data.parquet"), schema, 3);

            ImmutableList<HiveFileInfo> files = buildHiveFileInfos(dir.toUri().toString(), "", 1);
            assertEquals(files.size(), 1);

            assertSame(
                    buildQuickStats(session(true), TEST_TABLE, UNPARTITIONED_ID.getPartitionName(), files.iterator()),
                    PartitionQuickStats.EMPTY,
                    "Row groups were present, so an empty roll-up means unusable stats, not zero rows");
        }
        finally {
            deleteRecursively(dir, ALLOW_INSECURE);
        }
    }

    /**
     * The partitioned storage-format resolution path with files present, i.e. the
     * {@code getPartitionsByNames} branch that the zero-files tests above reach through the non-throwing
     * variant. Real stats must come back unchanged for a partition of a partitioned table.
     */
    @Test
    public void testRealStatsForPartitionOfPartitionedTable()
    {
        String resourceDir = TestParquetQuickStatsBuilder.class.getClassLoader().getResource("quick_stats").toString();
        ImmutableList<HiveFileInfo> hiveFileInfos = buildHiveFileInfos(resourceDir, "tpch_orders_100_rows", 1);

        PartitionQuickStats stats = buildQuickStats(session(true), PARTITIONED_TABLE, PARTITION_NAME, hiveFileInfos.iterator());

        assertNotSame(stats, PartitionQuickStats.EMPTY);
        assertNotSame(stats, PartitionQuickStats.PROVABLY_EMPTY);
        assertEquals(stats.getFileCount(), 1);
        assertFalse(stats.getStats().isEmpty());
    }

    /**
     * Mixed file sets. Emptiness is a property of the partition, not of any one file, so
     * a single row-bearing file must defeat the proof -- and a zero-row file mixed with an
     * all-nested-columns file must stay UNKNOWN rather than being mistaken for emptiness (the roll-up is
     * empty in both cases; only {@code rowGroupsSeen} tells them apart).
     */
    @Test
    public void testMixedFilesDoNotProveEmptiness()
            throws IOException
    {
        MessageType flatSchema = Types.buildMessage()
                .addField(Types.optional(INT64).named("id"))
                .named("flat");
        MessageType nestedOnlySchema = Types.buildMessage()
                .addField(Types.optionalGroup().addField(Types.optional(INT64).named("a")).named("nested"))
                .named("only_nested");

        java.nio.file.Path withRows = createTempDirectory("mixed-with-rows");
        try {
            writeParquetFile(withRows.resolve("empty.parquet"), flatSchema, 0);
            writeParquetFile(withRows.resolve("three_rows.parquet"), flatSchema, 3);
            ImmutableList<HiveFileInfo> files = buildHiveFileInfos(withRows.toUri().toString(), "", 1);
            assertEquals(files.size(), 2);

            PartitionQuickStats stats = buildQuickStats(session(true), TEST_TABLE, UNPARTITIONED_ID.getPartitionName(), files.iterator());
            assertNotSame(stats, PartitionQuickStats.PROVABLY_EMPTY, "One row-bearing file means the partition is not empty");
            assertNotSame(stats, PartitionQuickStats.EMPTY);
            assertEquals(stats.getStats().size(), 1);
            assertEquals(stats.getStats().get(0).getRowCount(), 3L);
        }
        finally {
            deleteRecursively(withRows, ALLOW_INSECURE);
        }

        java.nio.file.Path withNested = createTempDirectory("mixed-with-nested");
        try {
            writeParquetFile(withNested.resolve("empty.parquet"), flatSchema, 0);
            writeParquetFile(withNested.resolve("nested.parquet"), nestedOnlySchema, 3);
            ImmutableList<HiveFileInfo> files = buildHiveFileInfos(withNested.toUri().toString(), "", 1);
            assertEquals(files.size(), 2);

            assertSame(
                    buildQuickStats(session(true), TEST_TABLE, UNPARTITIONED_ID.getPartitionName(), files.iterator()),
                    PartitionQuickStats.EMPTY,
                    "Row groups were present in one file, so the empty roll-up means unusable stats, not zero rows");
        }
        finally {
            deleteRecursively(withNested, ALLOW_INSECURE);
        }
    }

    /**
     * A footer that cannot be read throws; it
     * does not fall through to the empty-roll-up branch and must therefore never be mistaken for
     * provable emptiness.
     */
    @Test
    public void testFooterFailureStillThrows()
            throws IOException
    {
        java.nio.file.Path dir = createTempDirectory("bad-footer");
        try {
            java.nio.file.Path corrupt = dir.resolve("data.parquet");
            write(corrupt, "this is not a parquet file".getBytes(UTF_8));

            ImmutableList<HiveFileInfo> files = buildHiveFileInfos(dir.toUri().toString(), "", 1);
            assertEquals(files.size(), 1);

            try {
                buildQuickStats(session(true), TEST_TABLE, UNPARTITIONED_ID.getPartitionName(), files.iterator());
                fail("Expected an unreadable footer to throw rather than resolve to any sentinel");
            }
            catch (RuntimeException expected) {
                // Expected: the future completes exceptionally and buildQuickStats rethrows.
            }
        }
        finally {
            deleteRecursively(dir, ALLOW_INSECURE);
        }
    }

    /**
     * The two sentinels are distinct instances, and the strategy
     * loop in QuickStatsProvider compares against EMPTY by identity, so PROVABLY_EMPTY must not be
     * equal-by-identity to it. Real stats are unaffected.
     */
    @Test
    public void testQuickStatsEmptySentinelIsNoLongerConflated()
    {
        assertNotSame(PartitionQuickStats.EMPTY, PartitionQuickStats.PROVABLY_EMPTY);

        String resourceDir = TestParquetQuickStatsBuilder.class.getClassLoader().getResource("quick_stats").toString();
        ImmutableList<HiveFileInfo> hiveFileInfos = buildHiveFileInfos(resourceDir, "tpch_orders_100_rows", 1);
        PartitionQuickStats realStats = buildQuickStats(session(true), TEST_TABLE, UNPARTITIONED_ID.getPartitionName(), hiveFileInfos.iterator());

        assertNotSame(realStats, PartitionQuickStats.EMPTY);
        assertNotSame(realStats, PartitionQuickStats.PROVABLY_EMPTY);
        assertFalse(realStats.getStats().isEmpty());
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
