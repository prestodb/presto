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

import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.TupleDomain.ColumnDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.datasink.OutputStreamDataSinkFactory;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.UnimplementedHiveMetastore;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingContext;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.common.predicate.Range.range;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.AbstractTestHiveClient.TEST_SERVER_VERSION;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveFileInfo.createHiveFileInfo;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveTestUtils.FILTER_STATS_CALCULATOR_SERVICE;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_RESOLUTION;
import static com.facebook.presto.hive.HiveTestUtils.ROW_EXPRESSION_SERVICE;
import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DATE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDateColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDecimalColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDoubleColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToIntBits;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestHiveSplitManager
{
    private static final int MAX_PARTITION_KEY_COLUMN_INDEX = -13;
    private static final SplitSchedulingContext SPLIT_SCHEDULING_CONTEXT = new SplitSchedulingContext(UNGROUPED_SCHEDULING, false, WarningCollector.NOOP);
    private static final HiveType LONT_DECIMAL = HiveType.valueOf("decimal(38,10)");
    private static final HiveType SHORT_DECIMAL = HiveType.valueOf("decimal(10,0)");
    private static final List<Column> COLUMNS = ImmutableList.of(
            new Column("t_tinyint", HIVE_BYTE, Optional.empty()),
            new Column("t_smallint", HIVE_SHORT, Optional.empty()),
            new Column("t_int", HIVE_INT, Optional.empty()),
            new Column("t_bigint", HIVE_LONG, Optional.empty()),
            new Column("t_float", HIVE_FLOAT, Optional.empty()),
            new Column("t_double", HIVE_DOUBLE, Optional.empty()),
            new Column("t_short_decimal", SHORT_DECIMAL, Optional.empty()),
            new Column("t_long_decimal", LONT_DECIMAL, Optional.empty()),
            new Column("t_date", HIVE_DATE, Optional.empty()));
    private static final String PARTITION_VALUE = "2020-01-01";
    private static final String PARTITION_NAME = "ds=2020-01-01";
    private static final Table TEST_TABLE = new Table(
            "test_db",
            "test_table",
            "test_owner",
            MANAGED_TABLE,
            new Storage(
                    VIEW_STORAGE_FORMAT,
                    "",
                    Optional.empty(),
                    false,
                    ImmutableMap.of(),
                    ImmutableMap.of()),
            COLUMNS,
            ImmutableList.of(new Column("ds", HIVE_STRING, Optional.empty())),
            ImmutableMap.of(),
            Optional.empty(),
            Optional.empty());

    private ListeningExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = MoreExecutors.listeningDecorator(newFixedThreadPool(10, daemonThreadsNamed("test-hive-split-manager-%s")));
    }

    @AfterClass(alwaysRun = true)
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testPartitionStatsBasedOptimizationForInteger()
            throws Exception
    {
        testPartitionStatsBasedOptimizationForInteger("t_tinyint", TINYINT, HIVE_BYTE);
        testPartitionStatsBasedOptimizationForInteger("t_smallint", SMALLINT, HIVE_SHORT);
        testPartitionStatsBasedOptimizationForInteger("t_int", INTEGER, HIVE_INT);
        testPartitionStatsBasedOptimizationForInteger("t_bigint", BIGINT, HIVE_LONG);
    }

    private void testPartitionStatsBasedOptimizationForInteger(String columnName, Type type, HiveType hiveType)
            throws Exception
    {
        HiveColumnHandle columnHandle = new HiveColumnHandle(
                columnName,
                hiveType,
                type.getTypeSignature(),
                0,
                REGULAR,
                Optional.empty(),
                Optional.empty());
        Range partitionRange = range(type, 10L, true, 20L, true);

        // Test no partition stats
        assertRedundantColumnDomains(
                partitionRange,
                PartitionStatistics.empty(),
                ImmutableList.of(ImmutableSet.of()),
                columnHandle);

        // Test partition left unchanged
        assertRedundantColumnDomains(
                partitionRange,
                createIntegerPartitionStatistics(5, 25, columnName),
                ImmutableList.of(ImmutableSet.of()),
                columnHandle);

        // Test partition being pruned
        assertRedundantColumnDomains(
                partitionRange,
                createIntegerPartitionStatistics(1, 3, columnName),
                ImmutableList.of(),
                columnHandle);

        // Test partition having subfield domain stripped
        assertRedundantColumnDomains(
                partitionRange,
                createIntegerPartitionStatistics(13, 15, columnName),
                ImmutableList.of(ImmutableSet.of(columnHandle)),
                columnHandle);
    }

    private PartitionStatistics createIntegerPartitionStatistics(long min, long max, String columnName)
    {
        return PartitionStatistics.builder()
                .setColumnStatistics(ImmutableMap.of(
                        columnName, createIntegerColumnStatistics(OptionalLong.of(min), OptionalLong.of(max), OptionalLong.of(0), OptionalLong.of(max - min + 1))))
                .build();
    }

    @Test
    public void testPartitionStatsBasedOptimizationForReal()
            throws Exception
    {
        Type type = REAL;
        Range partitionRange = range(type, (long) floatToIntBits(10.0f), true, (long) floatToIntBits(20.0f), true);
        HiveColumnHandle columnHandle = new HiveColumnHandle(
                "t_real",
                HIVE_FLOAT,
                type.getTypeSignature(),
                0,
                REGULAR,
                Optional.empty(),
                Optional.empty());

        // Test no partition stats
        assertRedundantColumnDomains(
                partitionRange,
                PartitionStatistics.empty(),
                ImmutableList.of(ImmutableSet.of()),
                columnHandle);

        // Test partition left unchanged
        assertRedundantColumnDomains(
                partitionRange,
                createDoublePartitionStatistics(5.0, 25.0, columnHandle.getName()),
                ImmutableList.of(ImmutableSet.of()),
                columnHandle);

        // Test partition being pruned
        assertRedundantColumnDomains(
                partitionRange,
                createDoublePartitionStatistics(1.0, 3.0, columnHandle.getName()),
                ImmutableList.of(),
                columnHandle);

        // Test partition having subfield domain stripped
        assertRedundantColumnDomains(
                partitionRange,
                createDoublePartitionStatistics(13.0, 15.0, columnHandle.getName()),
                ImmutableList.of(ImmutableSet.of(columnHandle)),
                columnHandle);
    }

    @Test
    public void testPartitionStatsBasedOptimizationForDouble()
            throws Exception
    {
        Type type = DOUBLE;
        Range partitionRange = range(type, 10.0, true, 20.0, true);
        HiveColumnHandle columnHandle = new HiveColumnHandle(
                "t_double",
                HIVE_DOUBLE,
                type.getTypeSignature(),
                0,
                REGULAR,
                Optional.empty(),
                Optional.empty());

        // Test no partition stats
        assertRedundantColumnDomains(
                partitionRange,
                PartitionStatistics.empty(),
                ImmutableList.of(ImmutableSet.of()),
                columnHandle);

        // Test partition left unchanged
        assertRedundantColumnDomains(
                partitionRange,
                createDoublePartitionStatistics(5.0, 25.0, columnHandle.getName()),
                ImmutableList.of(ImmutableSet.of()),
                columnHandle);

        // Test partition being pruned
        assertRedundantColumnDomains(
                partitionRange,
                createDoublePartitionStatistics(1.0, 3.0, columnHandle.getName()),
                ImmutableList.of(),
                columnHandle);

        // Test partition having subfield domain stripped
        assertRedundantColumnDomains(
                partitionRange,
                createDoublePartitionStatistics(13.0, 15.0, columnHandle.getName()),
                ImmutableList.of(ImmutableSet.of(columnHandle)),
                columnHandle);
    }

    private PartitionStatistics createDoublePartitionStatistics(double min, double max, String columnName)
    {
        return PartitionStatistics.builder()
                .setColumnStatistics(ImmutableMap.of(
                        columnName, createDoubleColumnStatistics(OptionalDouble.of(min), OptionalDouble.of(max), OptionalLong.of(0), OptionalLong.empty())))
                .build();
    }

    @Test
    public void testPartitionStatsBasedOptimizationForDecimal()
            throws Exception
    {
        Type shortDecimal = HiveType.getPrimitiveType((PrimitiveTypeInfo) SHORT_DECIMAL.getTypeInfo());
        testPartitionStatsBasedOptimizationForDecimal(
                range(shortDecimal, 10L, true, 20L, true),
                new HiveColumnHandle(
                        "t_short_decimal",
                        SHORT_DECIMAL,
                        shortDecimal.getTypeSignature(),
                        0,
                        REGULAR,
                        Optional.empty(),
                        Optional.empty()));

        Type longDecimal = HiveType.getPrimitiveType((PrimitiveTypeInfo) LONT_DECIMAL.getTypeInfo());
        testPartitionStatsBasedOptimizationForDecimal(
                range(longDecimal, encodeScaledValue(BigDecimal.valueOf(10)), true, encodeScaledValue(BigDecimal.valueOf(20)), true),
                new HiveColumnHandle(
                        "t_long_decimal",
                        LONT_DECIMAL,
                        longDecimal.getTypeSignature(),
                        0,
                        REGULAR,
                        Optional.empty(),
                        Optional.empty()));
    }

    private void testPartitionStatsBasedOptimizationForDecimal(Range partitionRange, HiveColumnHandle columnHandle)
            throws Exception
    {
        // Test no partition stats
        assertRedundantColumnDomains(
                partitionRange,
                PartitionStatistics.empty(),
                ImmutableList.of(ImmutableSet.of()),
                columnHandle);

        // Test partition left unchanged
        assertRedundantColumnDomains(
                partitionRange,
                createDecimalPartitionStatistics(5, 25, columnHandle.getName()),
                ImmutableList.of(ImmutableSet.of()),
                columnHandle);

        // Test partition being pruned
        assertRedundantColumnDomains(
                partitionRange,
                createDecimalPartitionStatistics(1, 3, columnHandle.getName()),
                ImmutableList.of(),
                columnHandle);

        // Test partition having subfield domain stripped
        assertRedundantColumnDomains(
                partitionRange,
                createDecimalPartitionStatistics(13, 15, columnHandle.getName()),
                ImmutableList.of(ImmutableSet.of(columnHandle)),
                columnHandle);
    }

    private PartitionStatistics createDecimalPartitionStatistics(long min, long max, String columnName)
    {
        return PartitionStatistics.builder()
                .setColumnStatistics(ImmutableMap.of(
                        columnName, createDecimalColumnStatistics(Optional.of(BigDecimal.valueOf(min)), Optional.of(BigDecimal.valueOf(max)), OptionalLong.empty(), OptionalLong.empty())))
                .build();
    }

    @Test
    public void testPartitionStatsBasedOptimizationForDate()
            throws Exception
    {
        Type type = DATE;
        Range partitionRange = range(type, 10L, true, 20L, true);
        HiveColumnHandle columnHandle = new HiveColumnHandle(
                "t_date",
                HIVE_DATE,
                type.getTypeSignature(),
                0,
                REGULAR,
                Optional.empty(),
                Optional.empty());

        // Test no partition stats
        assertRedundantColumnDomains(
                partitionRange,
                PartitionStatistics.empty(),
                ImmutableList.of(ImmutableSet.of()),
                columnHandle);

        // Test partition left unchanged
        assertRedundantColumnDomains(
                partitionRange,
                createDatePartitionStatistics(5, 25, columnHandle.getName()),
                ImmutableList.of(ImmutableSet.of()),
                columnHandle);

        // Test partition being pruned
        assertRedundantColumnDomains(
                partitionRange,
                createDatePartitionStatistics(1, 3, columnHandle.getName()),
                ImmutableList.of(),
                columnHandle);

        // Test partition having subfield domain stripped
        assertRedundantColumnDomains(
                partitionRange,
                createDatePartitionStatistics(13, 15, columnHandle.getName()),
                ImmutableList.of(ImmutableSet.of(columnHandle)),
                columnHandle);
    }

    private PartitionStatistics createDatePartitionStatistics(long min, long max, String columnName)
    {
        return PartitionStatistics.builder()
                .setColumnStatistics(ImmutableMap.of(
                        columnName, createDateColumnStatistics(Optional.of(LocalDate.ofEpochDay(min)), Optional.of(LocalDate.ofEpochDay(max)), OptionalLong.empty(), OptionalLong.empty())))
                .build();
    }

    private void assertRedundantColumnDomains(Range predicateRange, PartitionStatistics partitionStatistics, List<Set<ColumnHandle>> expectedRedundantColumnDomains, HiveColumnHandle columnHandle)
            throws Exception
    {
        // Prepare query predicate tuple domain
        TupleDomain<ColumnHandle> queryTupleDomain = TupleDomain.fromColumnDomains(
                Optional.of(ImmutableList.of(new ColumnDomain<>(
                        columnHandle,
                        Domain.create(
                                SortedRangeSet.copyOf(predicateRange.getType(), ImmutableList.of(predicateRange)),
                                false)))));

        // Prepare partition with stats
        PartitionWithStatistics partitionWithStatistics = new PartitionWithStatistics(
                new Partition(
                        "test_db",
                        "test_table",
                        ImmutableList.of(PARTITION_VALUE),
                        new Storage(
                                fromHiveStorageFormat(ORC),
                                "location",
                                Optional.empty(),
                                true,
                                ImmutableMap.of(),
                                ImmutableMap.of()),
                        COLUMNS,
                        ImmutableMap.of(),
                        Optional.empty(),
                        false,
                        true,
                        0),
                PARTITION_NAME,
                partitionStatistics);

        HiveClientConfig hiveClientConfig = new HiveClientConfig().setPartitionStatisticsBasedOptimizationEnabled(true);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(
                new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, new MetastoreClientConfig()), ImmutableSet.of()),
                new MetastoreClientConfig(),
                new NoHdfsAuthentication());
        HiveMetadataFactory metadataFactory = new HiveMetadataFactory(
                new TestingExtendedHiveMetastore(TEST_TABLE, partitionWithStatistics),
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
                FUNCTION_AND_TYPE_MANAGER,
                new HiveLocationService(hdfsEnvironment),
                FUNCTION_RESOLUTION,
                ROW_EXPRESSION_SERVICE,
                FILTER_STATS_CALCULATOR_SERVICE,
                new TableParameterCodec(),
                HiveTestUtils.PARTITION_UPDATE_CODEC,
                HiveTestUtils.PARTITION_UPDATE_SMILE_CODEC,
                executor,
                new HiveTypeTranslator(),
                new HiveStagingFileCommitter(hdfsEnvironment, executor),
                new HiveZeroRowFileCreator(hdfsEnvironment, new OutputStreamDataSinkFactory(), executor),
                TEST_SERVER_VERSION,
                new HivePartitionObjectBuilder(),
                new HiveEncryptionInformationProvider(ImmutableList.of()),
                new HivePartitionStats(),
                new HiveFileRenamer());

        HiveSplitManager splitManager = new HiveSplitManager(
                new TestingHiveTransactionManager(metadataFactory),
                new NamenodeStats(),
                hdfsEnvironment,
                new TestingDirectoryLister(),
                directExecutor(),
                new HiveCoercionPolicy(FUNCTION_AND_TYPE_MANAGER),
                new CounterStat(),
                100,
                hiveClientConfig.getMaxOutstandingSplitsSize(),
                hiveClientConfig.getMinPartitionBatchSize(),
                hiveClientConfig.getMaxPartitionBatchSize(),
                hiveClientConfig.getMaxInitialSplits(),
                hiveClientConfig.getSplitLoaderConcurrency(),
                false,
                new ConfigBasedCacheQuotaRequirementProvider(new CacheConfig()),
                new HiveEncryptionInformationProvider(ImmutableList.of()));

        HiveColumnHandle partitionColumn = new HiveColumnHandle(
                "ds",
                HIVE_STRING,
                parseTypeSignature(VARCHAR),
                MAX_PARTITION_KEY_COLUMN_INDEX,
                PARTITION_KEY,
                Optional.empty(),
                Optional.empty());
        List<HivePartition> partitions = ImmutableList.of(
                new HivePartition(
                        new SchemaTableName("test_schema", "test_table"),
                        PARTITION_NAME,
                        ImmutableMap.of(partitionColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice(PARTITION_VALUE)))));
        TupleDomain<Subfield> domainPredicate = queryTupleDomain
                .transform(HiveColumnHandle.class::cast)
                .transform(column -> new Subfield(column.getName(), ImmutableList.of()));

        ConnectorSplitSource splitSource = splitManager.getSplits(
                new HiveTransactionHandle(),
                new TestingConnectorSession(new HiveSessionProperties(hiveClientConfig, new OrcFileWriterConfig(), new ParquetFileWriterConfig(), new CacheConfig()).getSessionProperties()),
                new HiveTableLayoutHandle(
                        new SchemaTableName("test_schema", "test_table"),
                        "test_path",
                        ImmutableList.of(partitionColumn),
                        COLUMNS,
                        ImmutableMap.of(),
                        partitions,
                        domainPredicate,
                        TRUE_CONSTANT,
                        ImmutableMap.of(
                                partitionColumn.getName(), partitionColumn,
                                columnHandle.getName(), columnHandle),
                        queryTupleDomain,
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        "layout",
                        Optional.empty(),
                        false),
                SPLIT_SCHEDULING_CONTEXT);
        List<Set<ColumnHandle>> actualRedundantColumnDomains = splitSource.getNextBatch(NOT_PARTITIONED, 100).get().getSplits().stream()
                .map(HiveSplit.class::cast)
                .map(HiveSplit::getRedundantColumnDomains)
                .collect(toImmutableList());
        assertEquals(actualRedundantColumnDomains, expectedRedundantColumnDomains);
    }

    private static class TestingHiveTransactionManager
            extends HiveTransactionManager
    {
        private final HiveMetadataFactory metadataFactory;

        public TestingHiveTransactionManager(HiveMetadataFactory metadataFactory)
        {
            this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        }

        @Override
        public TransactionalMetadata get(ConnectorTransactionHandle transactionHandle)
        {
            return metadataFactory.get();
        }
    }

    private static class TestingExtendedHiveMetastore
            extends UnimplementedHiveMetastore
    {
        private final Table table;
        private final PartitionWithStatistics partitionWithStatistics;

        public TestingExtendedHiveMetastore(Table table, PartitionWithStatistics partitionWithStatistics)
        {
            this.table = requireNonNull(table, "table is null");
            this.partitionWithStatistics = requireNonNull(partitionWithStatistics, "partitionWithStatistics is null");
        }

        @Override
        public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
        {
            return Optional.of(table);
        }

        @Override
        public Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionNames)
        {
            return ImmutableMap.of(partitionWithStatistics.getPartitionName(), Optional.of(partitionWithStatistics.getPartition()));
        }

        @Override
        public Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames)
        {
            return ImmutableMap.of(partitionWithStatistics.getPartitionName(), partitionWithStatistics.getStatistics());
        }
    }

    private static class TestingDirectoryLister
            implements DirectoryLister
    {
        @Override
        public Iterator<HiveFileInfo> list(ExtendedFileSystem fileSystem, Table table, Path path, NamenodeStats namenodeStats, PathFilter pathFilter, HiveDirectoryContext hiveDirectoryContext)
        {
            try {
                return ImmutableList.of(
                        createHiveFileInfo(
                                new LocatedFileStatus(
                                        new FileStatus(0, false, 1, 0, 0, path),
                                        new BlockLocation[] {}),
                                Optional.empty()))
                        .iterator();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
