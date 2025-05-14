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
package com.facebook.presto.iceberg.hive;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.RuntimeMetric;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.iceberg.CatalogType;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergHiveTableOperationsConfig;
import com.facebook.presto.iceberg.IcebergMetadataColumn;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.ManifestFileCache;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateStatistics;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZER_USE_HISTOGRAMS;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.TEST_DATA_DIRECTORY;
import static com.facebook.presto.iceberg.IcebergSessionProperties.HIVE_METASTORE_STATISTICS_MERGE_STRATEGY;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.iceberg.IcebergSessionProperties.STATISTICS_KLL_SKETCH_K_PARAMETER;
import static com.facebook.presto.iceberg.statistics.KllHistogram.isKllHistogramSupportedType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * This class tests the ability for the Hive implementation of the IcebergMetadata to store and read
 * table statistics
 */
public class TestIcebergHiveStatistics
        extends AbstractTestQueryFramework
{
    private IcebergQueryRunner icebergQueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        icebergQueryRunner = IcebergQueryRunner.builder()
                .setExtraConnectorProperties(ImmutableMap.of("iceberg.hive-statistics-merge-strategy", NUMBER_OF_DISTINCT_VALUES.name()))
                .build();
        return icebergQueryRunner.getQueryRunner();
    }

    private static final Set<String> NUMERIC_ORDERS_COLUMNS = ImmutableSet.<String>builder()
            .add("orderkey")
            .add("custkey")
            .add("totalprice")
            .add("orderdate")
            .add("shippriority")
            .build();

    private static final Set<String> NON_NUMERIC_ORDERS_COLUMNS = ImmutableSet.<String>builder()
            .add("orderstatus")
            .add("orderpriority")
            .add("clerk")
            .add("comment")
            .build();
    private static final Set<String> ALL_ORDERS_COLUMNS = Arrays.stream(StatsSchema.values()).map(StatsSchema::getColumnName).collect(Collectors.toSet());

    enum StatsSchema
    {
        COLUMN_NAME("column_name"),
        DATA_SIZE("data_size"),
        DISTINCT_VALUES_COUNT("distinct_values_count"),
        NULLS_FRACTION("nulls_fraction"),
        ROW_COUNT("row_count"),
        LOW_VALUE("low_value"),
        HIGH_VALUE("high_value");
        private final String name;

        StatsSchema(String name)
        {
            this.name = name;
        }

        public String getColumnName()
        {
            return name;
        }
    }

    /**
     * ensures that the stats not provided by {@link com.facebook.presto.iceberg.TableStatisticsMaker} are
     * populated and served from the metadata after running an ANALYZE query
     */
    @Test
    public void testSimpleAnalyze()
    {
        assertQuerySucceeds("CREATE TABLE simpleAnalyze as SELECT * FROM orders");
        MaterializedResult stats = getQueryRunner().execute("SHOW STATS FOR simpleAnalyze");
        assertStatValueAbsent(StatsSchema.DISTINCT_VALUES_COUNT, stats, NUMERIC_ORDERS_COLUMNS);
        assertQuerySucceeds("ANALYZE simpleAnalyze");
        stats = getQueryRunner().execute("SHOW STATS FOR simpleAnalyze");
        assertStatValuePresent(StatsSchema.DISTINCT_VALUES_COUNT, stats, NUMERIC_ORDERS_COLUMNS);
        assertStatValuePresent(StatsSchema.NULLS_FRACTION, stats, NUMERIC_ORDERS_COLUMNS);
        assertStatValuePresent(StatsSchema.NULLS_FRACTION, stats, NON_NUMERIC_ORDERS_COLUMNS);
    }

    /**
     * Tests the TableStatisticsMaker is used, even when ANALYZE has not been run yet
     */
    @Test
    public void testStatsBeforeAnalyze()
    {
        assertQuerySucceeds("CREATE TABLE statsBeforeAnalyze as SELECT * FROM orders");
        MaterializedResult stats = getQueryRunner().execute("SHOW STATS FOR statsBeforeAnalyze");
        assertStatValueAbsent(StatsSchema.DISTINCT_VALUES_COUNT, stats, NUMERIC_ORDERS_COLUMNS);
        assertStatValuePresent(StatsSchema.DATA_SIZE, stats, ALL_ORDERS_COLUMNS);
        assertStatValuePresent(StatsSchema.LOW_VALUE, stats, NUMERIC_ORDERS_COLUMNS);
        assertStatValuePresent(StatsSchema.HIGH_VALUE, stats, ALL_ORDERS_COLUMNS);
    }

    @Test
    public void testStatsWithPartitionedTableAnalyzed()
    {
        assertQuerySucceeds("CREATE TABLE statsNoPartitionAnalyze as SELECT * FROM orders LIMIT 100");
        assertQuerySucceeds("CREATE TABLE statsWithPartitionAnalyze WITH (partitioning = ARRAY['orderdate']) as SELECT * FROM statsNoPartitionAnalyze");
        assertQuerySucceeds("ANALYZE statsNoPartitionAnalyze");
        assertQuerySucceeds("ANALYZE statsWithPartitionAnalyze");
        deleteTableStatistics("statsWithPartitionAnalyze");
        Metadata meta = getQueryRunner().getMetadata();
        TransactionId txid = getQueryRunner().getTransactionManager().beginTransaction(false);
        Session session = getSession().beginTransactionId(txid, getQueryRunner().getTransactionManager(), new AllowAllAccessControl());
        Map<String, ColumnHandle> noPartColumns = getColumnHandles("statsnopartitionanalyze", session);
        Map<String, ColumnHandle> partColumns = getColumnHandles("statswithpartitionanalyze", session);
        List<ColumnHandle> noPartColumnHandles = new ArrayList<>(noPartColumns.values());
        List<ColumnHandle> partColumnHandles = new ArrayList<>(partColumns.values());
        // Test that with all columns and no constraints that stats are equivalent
        TableStatistics statsNoPartition = meta.getTableStatistics(session, getAnalyzeTableHandle("statsNoPartitionAnalyze", session), noPartColumnHandles, Constraint.alwaysTrue());
        TableStatistics statsWithPartition = meta.getTableStatistics(session, getAnalyzeTableHandle("statsWithPartitionAnalyze", session), partColumnHandles, Constraint.alwaysTrue());
        assertNDVsPresent(statsNoPartition);
        assertNDVsNotPresent(statsWithPartition);
        assertEquals(statsNoPartition.getRowCount(), statsWithPartition.getRowCount());

        // Test that with one column and no constraints that stats are equivalent
        statsNoPartition = meta.getTableStatistics(session, getAnalyzeTableHandle("statsNoPartitionAnalyze", session), Collections.singletonList(noPartColumns.get("totalprice")), Constraint.alwaysTrue());
        statsWithPartition = meta.getTableStatistics(session, getAnalyzeTableHandle("statsWithPartitionAnalyze", session), Collections.singletonList(partColumns.get("totalprice")), Constraint.alwaysTrue());
        assertEquals(statsNoPartition.getRowCount(), statsWithPartition.getRowCount());
        assertEquals(statsNoPartition.getColumnStatistics().size(), 1);
        assertEquals(statsWithPartition.getColumnStatistics().size(), 1);
        assertNotNull(statsWithPartition.getColumnStatistics().get(partColumns.get("totalprice")));
        assertNotNull(statsNoPartition.getColumnStatistics().get(noPartColumns.get("totalprice")));
        assertNDVsPresent(statsNoPartition);
        assertNDVsNotPresent(statsWithPartition);

        // Test that with all columns and a Tuple constraint that stats are equivalent
        statsNoPartition = meta.getTableStatistics(session, getAnalyzeTableHandle("statsNoPartitionAnalyze", session), noPartColumnHandles, Constraint.alwaysTrue());
        statsWithPartition = meta.getTableStatistics(session, getAnalyzeTableHandle("statsWithPartitionAnalyze", session), partColumnHandles, Constraint.alwaysTrue());
        assertEquals(statsNoPartition.getRowCount(), statsWithPartition.getRowCount());
        assertEquals(statsNoPartition.getColumnStatistics().size(), noPartColumnHandles.size());
        assertEquals(statsWithPartition.getColumnStatistics().size(), partColumnHandles.size());
        assertNDVsPresent(statsNoPartition);
        assertNDVsNotPresent(statsWithPartition);

        // Test that with one column and a constraint on that column that the partitioned stats return less values
        Constraint<ColumnHandle> noPartConstraint = constraintWithMinValue(noPartColumns.get("totalprice"), 100000.0);
        Constraint<ColumnHandle> partConstraint = constraintWithMinValue(partColumns.get("totalprice"), 100000.0);
        statsNoPartition = meta.getTableStatistics(session, getAnalyzeTableHandle("statsNoPartitionAnalyze", session), Collections.singletonList(noPartColumns.get("totalprice")), noPartConstraint);
        statsWithPartition = meta.getTableStatistics(session, getAnalyzeTableHandle("statsWithPartitionAnalyze", session), Collections.singletonList(partColumns.get("totalprice")), partConstraint);
        // ensure partitioned table actually returns less rows
        assertEquals(statsNoPartition.getColumnStatistics().size(), 1);
        assertEquals(statsWithPartition.getColumnStatistics().size(), 1);
        assertNotNull(statsWithPartition.getColumnStatistics().get(partColumns.get("totalprice")));
        assertNotNull(statsNoPartition.getColumnStatistics().get(noPartColumns.get("totalprice")));
        assertNDVsPresent(statsNoPartition);
        assertNDVsNotPresent(statsWithPartition);
        // partitioned table should have stats partially filtered since data should span > 1 file
        assertTrue(statsWithPartition.getRowCount().getValue() < statsNoPartition.getRowCount().getValue());

        // Test that with one column and a constraint on a different column that stats are equivalent.
        noPartConstraint = constraintWithMinValue(noPartColumns.get("totalprice"), 100000.0);
        partConstraint = constraintWithMinValue(partColumns.get("totalprice"), 100000.0);
        statsNoPartition = meta.getTableStatistics(session, getAnalyzeTableHandle("statsNoPartitionAnalyze", session), Collections.singletonList(noPartColumns.get("orderkey")), noPartConstraint);
        statsWithPartition = meta.getTableStatistics(session, getAnalyzeTableHandle("statsWithPartitionAnalyze", session), Collections.singletonList(partColumns.get("orderkey")), partConstraint);
        assertEquals(statsNoPartition.getColumnStatistics().size(), 1);
        assertEquals(statsWithPartition.getColumnStatistics().size(), 1);
        assertNotNull(statsWithPartition.getColumnStatistics().get(partColumns.get("orderkey")));
        assertNotNull(statsNoPartition.getColumnStatistics().get(noPartColumns.get("orderkey")));
        assertNDVsPresent(statsNoPartition);
        assertNDVsNotPresent(statsWithPartition);
        // partitioned table should have stats partially filtered since data should span > 1 file
        assertTrue(statsWithPartition.getRowCount().getValue() < statsNoPartition.getRowCount().getValue());

        assertQuerySucceeds("DROP TABLE statsNoPartitionAnalyze");
        assertQuerySucceeds("DROP TABLE statsWithPartitionAnalyze");
    }

    @Test
    public void testStatsWithPartitionedTablesNoAnalyze()
    {
        assertQuerySucceeds("CREATE TABLE statsNoPartition as SELECT * FROM orders LIMIT 100");
        assertQuerySucceeds("CREATE TABLE statsWithPartition WITH (partitioning = ARRAY['orderdate']) as SELECT * FROM statsNoPartition");
        Metadata meta = getQueryRunner().getMetadata();
        TransactionId txid = getQueryRunner().getTransactionManager().beginTransaction(false);
        Session s = getSession().beginTransactionId(txid, getQueryRunner().getTransactionManager(), new AllowAllAccessControl());
        Map<String, ColumnHandle> noPartColumns = getColumnHandles("statsnopartition", s);
        Map<String, ColumnHandle> partColumns = getColumnHandles("statswithpartition", s);
        List<ColumnHandle> noPartColumnHandles = new ArrayList<>(noPartColumns.values());
        List<ColumnHandle> partColumnHandles = new ArrayList<>(partColumns.values());
        // Test that with all columns and no constraints that stats are equivalent
        TableStatistics statsNoPartition = meta.getTableStatistics(s, getAnalyzeTableHandle("statsNoPartition", s), noPartColumnHandles, Constraint.alwaysTrue());
        TableStatistics statsWithPartition = meta.getTableStatistics(s, getAnalyzeTableHandle("statsWithPartition", s), partColumnHandles, Constraint.alwaysTrue());
        assertEquals(statsNoPartition.getRowCount(), statsWithPartition.getRowCount());
        columnStatsEqual(statsNoPartition.getColumnStatistics(), statsWithPartition.getColumnStatistics());

        // Test that with one column and no constraints that stats are equivalent
        statsNoPartition = meta.getTableStatistics(s, getAnalyzeTableHandle("statsNoPartition", s), Collections.singletonList(noPartColumns.get("totalprice")), Constraint.alwaysTrue());
        statsWithPartition = meta.getTableStatistics(s, getAnalyzeTableHandle("statsWithPartition", s), Collections.singletonList(partColumns.get("totalprice")), Constraint.alwaysTrue());
        assertEquals(statsNoPartition.getRowCount(), statsWithPartition.getRowCount());
        assertEquals(statsNoPartition.getColumnStatistics().size(), 1);
        assertEquals(statsWithPartition.getColumnStatistics().size(), 1);
        assertNotNull(statsWithPartition.getColumnStatistics().get(partColumns.get("totalprice")));
        assertNotNull(statsNoPartition.getColumnStatistics().get(noPartColumns.get("totalprice")));
        columnStatsEqual(statsNoPartition.getColumnStatistics(), statsWithPartition.getColumnStatistics());

        // Test that with all columns and a Tuple constraint that stats are equivalent
        statsNoPartition = meta.getTableStatistics(s, getAnalyzeTableHandle("statsNoPartition", s), noPartColumnHandles, Constraint.alwaysTrue());
        statsWithPartition = meta.getTableStatistics(s, getAnalyzeTableHandle("statsWithPartition", s), partColumnHandles, Constraint.alwaysTrue());
        assertEquals(statsNoPartition.getRowCount(), statsWithPartition.getRowCount());
        assertEquals(statsNoPartition.getColumnStatistics().size(), noPartColumnHandles.size());
        assertEquals(statsWithPartition.getColumnStatistics().size(), partColumnHandles.size());
        columnStatsEqual(statsNoPartition.getColumnStatistics(), statsWithPartition.getColumnStatistics());

        // Test that with one column and a constraint on that column that the partitioned stats return less values
        Constraint<ColumnHandle> noPartConstraint = constraintWithMinValue(noPartColumns.get("totalprice"), 100000.0);
        Constraint<ColumnHandle> partConstraint = constraintWithMinValue(partColumns.get("totalprice"), 100000.0);
        statsNoPartition = meta.getTableStatistics(s, getAnalyzeTableHandle("statsNoPartition", s), Collections.singletonList(noPartColumns.get("totalprice")), noPartConstraint);
        statsWithPartition = meta.getTableStatistics(s, getAnalyzeTableHandle("statsWithPartition", s), Collections.singletonList(partColumns.get("totalprice")), partConstraint);
        // ensure partitioned table actually returns less rows
        assertEquals(statsNoPartition.getColumnStatistics().size(), 1);
        assertEquals(statsWithPartition.getColumnStatistics().size(), 1);
        assertNotNull(statsWithPartition.getColumnStatistics().get(partColumns.get("totalprice")));
        assertNotNull(statsNoPartition.getColumnStatistics().get(noPartColumns.get("totalprice")));
        // partitioned table should have stats partially filtered since data should span > 1 file
        assertTrue(statsWithPartition.getRowCount().getValue() < statsNoPartition.getRowCount().getValue());

        // Test that with one column and a constraint on a different column that stats are equivalent.
        noPartConstraint = constraintWithMinValue(noPartColumns.get("totalprice"), 100000.0);
        partConstraint = constraintWithMinValue(partColumns.get("totalprice"), 100000.0);
        statsNoPartition = meta.getTableStatistics(s, getAnalyzeTableHandle("statsNoPartition", s), Collections.singletonList(noPartColumns.get("orderkey")), noPartConstraint);
        statsWithPartition = meta.getTableStatistics(s, getAnalyzeTableHandle("statsWithPartition", s), Collections.singletonList(partColumns.get("orderkey")), partConstraint);
        assertEquals(statsNoPartition.getColumnStatistics().size(), 1);
        assertEquals(statsWithPartition.getColumnStatistics().size(), 1);
        assertNotNull(statsWithPartition.getColumnStatistics().get(partColumns.get("orderkey")));
        assertNotNull(statsNoPartition.getColumnStatistics().get(noPartColumns.get("orderkey")));
        // partitioned table should have stats partially filtered since data should span > 1 file
        assertTrue(statsWithPartition.getRowCount().getValue() < statsNoPartition.getRowCount().getValue());
        assertQuerySucceeds("DROP TABLE statsNoPartition");
        assertQuerySucceeds("DROP TABLE statsWithPartition");
    }

    @Test
    public void testHiveStatisticsMergeFlags()
    {
        assertQuerySucceeds("CREATE TABLE mergeFlagsStats (i int, v varchar)");
        assertQuerySucceeds("INSERT INTO mergeFlagsStats VALUES (0, '1'), (1, '22'), (2, '333'), (NULL, 'aaaaa'), (4, NULL)");
        assertQuerySucceeds("ANALYZE mergeFlagsStats");

        // invalidate puffin files so only hive stats can be returned
        deleteTableStatistics("mergeFlagsStats");

        // Test stats without merging doesn't return NDVs or data size
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", HIVE_METASTORE_STATISTICS_MERGE_STRATEGY, "")
                .build();
        TableStatistics stats = getTableStatistics(session, "mergeFlagsStats");

        Map<String, ColumnStatistics> columnStatistics = getColumnNameMap(stats);
        assertEquals(columnStatistics.get("i").getDistinctValuesCount(), Estimate.unknown());
        assertEquals(columnStatistics.get("i").getDataSize(), Estimate.unknown());
        assertEquals(columnStatistics.get("v").getDistinctValuesCount(), Estimate.unknown());
        assertEquals(columnStatistics.get("v").getDataSize(), Estimate.unknown());

        // Test stats merging for NDVs
        session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", HIVE_METASTORE_STATISTICS_MERGE_STRATEGY, NUMBER_OF_DISTINCT_VALUES.name())
                .build();
        stats = getTableStatistics(session, "mergeFlagsStats");
        columnStatistics = getColumnNameMap(stats);
        assertEquals(columnStatistics.get("i").getDistinctValuesCount(), Estimate.of(4.0));
        assertEquals(columnStatistics.get("i").getDataSize(), Estimate.unknown());
        assertEquals(columnStatistics.get("v").getDistinctValuesCount(), Estimate.of(4.0));
        assertEquals(columnStatistics.get("v").getDataSize(), Estimate.unknown());

        // Test stats for data size
        session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", HIVE_METASTORE_STATISTICS_MERGE_STRATEGY, TOTAL_SIZE_IN_BYTES.name())
                .build();
        stats = getTableStatistics(session, "mergeFlagsStats");
        columnStatistics = getColumnNameMap(stats);
        assertEquals(columnStatistics.get("i").getDistinctValuesCount(), Estimate.unknown());
        assertEquals(columnStatistics.get("i").getDataSize(), Estimate.unknown()); // fixed-width isn't collected
        assertEquals(columnStatistics.get("v").getDistinctValuesCount(), Estimate.unknown());
        assertEquals(columnStatistics.get("v").getDataSize(), Estimate.of(11));

        // Test stats for both
        session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", HIVE_METASTORE_STATISTICS_MERGE_STRATEGY, NUMBER_OF_DISTINCT_VALUES.name() + "," + TOTAL_SIZE_IN_BYTES)
                .build();
        stats = getTableStatistics(session, "mergeFlagsStats");
        columnStatistics = getColumnNameMap(stats);
        assertEquals(columnStatistics.get("i").getDistinctValuesCount(), Estimate.of(4.0));
        assertEquals(columnStatistics.get("i").getDataSize(), Estimate.unknown());
        assertEquals(columnStatistics.get("v").getDistinctValuesCount(), Estimate.of(4.0));
        assertEquals(columnStatistics.get("v").getDataSize(), Estimate.of(11));
    }

    @DataProvider(name = "pushdownFilterEnabled")
    public Object[][] pushdownFilterPropertyProvider()
    {
        return new Object[][] {
                {true, true},
                {true, false},
                {false, true},
                {false, false},
        };
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testPredicateOnlyColumnInStatisticsOutput(boolean pushdownFilterEnabled, boolean partitioned)
    {
        assertQuerySucceeds(format("CREATE TABLE scanFilterStats (i int, j int, k int)%s", partitioned ? " WITH (partitioning = ARRAY['j'])" : ""));
        assertUpdate("INSERT INTO scanFilterStats VALUES (1, 2, 3), (3, 4, 5), (5, 6, 7)", 3);
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", PUSHDOWN_FILTER_ENABLED, Boolean.toString(pushdownFilterEnabled))
                .build();
        try {
            TableStatistics est = getScanStatsEstimate(session, "SELECT k from scanFilterStats WHERE j > 2 AND i = 3");
            assertEquals(est.getColumnStatistics().size(), 3);
        }
        finally {
            getQueryRunner().execute("DROP TABLE scanFilterStats");
        }
    }

    @Test
    public void testStatisticsCachePartialEviction()
    {
        String catalogName = "ice_stat_file_cache";
        String tableName = "lineitem_statisticsFileCache";
        try {
            Map<String, String> catalogProperties = ImmutableMap.<String, String>builder().putAll(icebergQueryRunner.getIcebergCatalogs().get("iceberg"))
                    .put("iceberg.max-statistics-file-cache-size", "1024B")
                    .build();
            getQueryRunner().createCatalog(catalogName, "iceberg", catalogProperties);
            Session session = Session.builder(getSession())
                    // runtime stats must be reset manually when using the builder
                    .setRuntimeStats(new RuntimeStats())
                    .setCatalog(catalogName)
                    // set histograms enabled to increase statistics cache size
                    .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "true")
                    .setCatalogSessionProperty(catalogName, STATISTICS_KLL_SKETCH_K_PARAMETER, "32768")
                    .build();

            assertQuerySucceeds(format("CREATE TABLE %s as SELECT * FROM lineitem", tableName));

            assertQuerySucceeds(session, "ANALYZE " + tableName);
            // get table statistics, to populate some of the cache
            TableStatistics statistics = getTableStatistics(getQueryRunner(), session, tableName);
            assertTrue(statistics.getColumnStatistics().values().stream().map(ColumnStatistics::getHistogram).anyMatch(Optional::isPresent));
            RuntimeStats runtimeStats = session.getRuntimeStats();
            runtimeStats.getMetrics().keySet().stream().filter(name -> name.contains("ColumnCount")).findFirst()
                    .ifPresent(stat -> assertEquals(runtimeStats.getMetric(stat).getSum(), 32, "column count must be 32 on metric: " + stat));
            runtimeStats.getMetrics().keySet().stream().filter(name -> name.contains("PuffinFileSize")).findFirst()
                    .ifPresent(stat -> assertTrue(runtimeStats.getMetric(stat).getSum() > 1024));
            // get them again to trigger retrieval of _some_ cached statistics
            statistics = getTableStatistics(getQueryRunner(), session, tableName);
            RuntimeMetric partialMiss = runtimeStats.getMetrics().keySet().stream().filter(name -> name.contains("PartialMiss")).findFirst()
                    .map(runtimeStats::getMetric)
                    .orElseThrow(() -> new RuntimeException("partial miss on statistics cache should have occurred"));
            assertTrue(partialMiss.getCount() > 0);

            statistics.getColumnStatistics().forEach((handle, stats) -> {
                assertFalse(stats.getDistinctValuesCount().isUnknown());
                if (isKllHistogramSupportedType(((IcebergColumnHandle) handle).getType())) {
                    assertTrue(stats.getHistogram().isPresent());
                }
            });
        }
        finally {
            assertQuerySucceeds("DROP TABLE " + tableName);
        }
    }

    private TableStatistics getScanStatsEstimate(Session session, @Language("SQL") String sql)
    {
        Plan plan = plan(sql, session);
        TableScanNode node = plan.getPlanIdNodeMap().values().stream()
                .filter(planNode -> planNode instanceof TableScanNode)
                .map(TableScanNode.class::cast)
                .findFirst().get();
        return transaction(getQueryRunner().getTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(session, (Function<Session, TableStatistics>) txnSession -> getQueryRunner().getMetadata()
                        .getTableStatistics(txnSession,
                                node.getTable(),
                                ImmutableList.copyOf(node.getAssignments().values()),
                                new Constraint<>(node.getCurrentConstraint())));
    }

    private static TableStatistics getTableStatistics(QueryRunner queryRunner, Session session, String table)
    {
        Metadata meta = queryRunner.getMetadata();
        TransactionId txid = queryRunner.getTransactionManager().beginTransaction(false);
        Session txnSession = session.beginTransactionId(txid, queryRunner.getTransactionManager(), new AllowAllAccessControl());
        Map<String, ColumnHandle> columnHandles = getColumnHandles(queryRunner, table, txnSession);
        List<ColumnHandle> columnHandleList = new ArrayList<>(columnHandles.values());
        return meta.getTableStatistics(txnSession, getAnalyzeTableHandle(queryRunner, table, txnSession), columnHandleList, Constraint.alwaysTrue());
    }

    private TableStatistics getTableStatistics(Session session, String table)
    {
        return getTableStatistics(getQueryRunner(), session, table);
    }

    private void columnStatsEqual(Map<ColumnHandle, ColumnStatistics> actualStats, Map<ColumnHandle, ColumnStatistics> expectedStats)
    {
        for (ColumnHandle handle : expectedStats.keySet()) {
            ColumnStatistics expected = expectedStats.get(handle);
            if (((IcebergColumnHandle) handle).getColumnType() == PARTITION_KEY) {
                handle = new IcebergColumnHandle(
                        ((IcebergColumnHandle) handle).getColumnIdentity(),
                        ((IcebergColumnHandle) handle).getType(),
                        ((IcebergColumnHandle) handle).getComment(),
                        REGULAR,
                        handle.getRequiredSubfields());
            }
            ColumnStatistics actual = actualStats.get(handle);
            assertEquals(actual.getRange(), expected.getRange(), "range for col: " + handle);
            assertEquals(actual.getNullsFraction(), expected.getNullsFraction(), "nulls fraction for col: " + handle);
            assertEquals(actual.getDistinctValuesCount(), expected.getDistinctValuesCount(), "NDVs for col: " + handle);
        }
    }

    private static Constraint<ColumnHandle> constraintWithMinValue(ColumnHandle col, Double min)
    {
        return new Constraint<>(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(col, Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, min)), true))));
    }

    private static TableHandle getAnalyzeTableHandle(QueryRunner queryRunner, String tableName, Session session)
    {
        Metadata meta = queryRunner.getMetadata();
        return meta.getTableHandleForStatisticsCollection(
                session,
                new QualifiedObjectName(session.getCatalog().get(), session.getSchema().get(), tableName.toLowerCase(Locale.US)),
                Collections.emptyMap()).get();
    }

    private TableHandle getAnalyzeTableHandle(String tableName, Session session)
    {
        return getAnalyzeTableHandle(getQueryRunner(), tableName, session);
    }

    private static TableHandle getTableHandle(QueryRunner queryRunner, String tableName, Session session)
    {
        MetadataResolver resolver = queryRunner.getMetadata().getMetadataResolver(session);
        return resolver.getTableHandle(new QualifiedObjectName(session.getCatalog().get(), session.getSchema().get(), tableName.toLowerCase(Locale.US))).get();
    }

    private static Map<String, ColumnHandle> getColumnHandles(QueryRunner queryRunner, String tableName, Session session)
    {
        return queryRunner.getMetadata().getColumnHandles(session, getTableHandle(queryRunner, tableName, session)).entrySet().stream()
                .filter(entry -> !IcebergMetadataColumn.isMetadataColumnId(((IcebergColumnHandle) (entry.getValue())).getId()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<String, ColumnHandle> getColumnHandles(String tableName, Session session)
    {
        return getColumnHandles(getQueryRunner(), tableName, session);
    }

    static void assertStatValuePresent(StatsSchema column, MaterializedResult result, Set<String> columnNames)
    {
        assertStatValue(column, result, columnNames, null, true);
    }

    static void assertStatValueAbsent(StatsSchema column, MaterializedResult result, Set<String> columnNames)
    {
        assertStatValue(column, result, columnNames, null, false);
    }

    static void assertStatValue(StatsSchema column, MaterializedResult result, Set<String> columnNames, Object value, boolean assertNot)
    {
        result.getMaterializedRows().forEach(row -> {
            if (columnNames.contains((String) row.getField(StatsSchema.COLUMN_NAME.ordinal()))) {
                Object resultValue = row.getField(column.ordinal());
                if (assertNot) {
                    assertNotEquals(resultValue, value);
                }
                else {
                    assertEquals(resultValue, value);
                }
            }
        });
    }

    private void deleteTableStatistics(String tableName)
    {
        Table icebergTable = loadTable(tableName);
        UpdateStatistics statsUpdate = icebergTable.updateStatistics();
        icebergTable.statisticsFiles().stream().map(StatisticsFile::snapshotId).forEach(statsUpdate::removeStatistics);
        statsUpdate.commit();
    }

    private Table loadTable(String tableName)
    {
        tableName = normalizeIdentifier(tableName, ICEBERG_CATALOG);
        CatalogManager catalogManager = getDistributedQueryRunner().getCoordinator().getCatalogManager();
        ConnectorId connectorId = catalogManager.getCatalog(ICEBERG_CATALOG).get().getConnectorId();

        return IcebergUtil.getHiveIcebergTable(getFileHiveMetastore(),
                getHdfsEnvironment(),
                new IcebergHiveTableOperationsConfig(),
                new ManifestFileCache(CacheBuilder.newBuilder().build(), false, 0, 1024),
                getQueryRunner().getDefaultSession().toConnectorSession(connectorId),
                SchemaTableName.valueOf("tpch." + tableName));
    }

    protected ExtendedHiveMetastore getFileHiveMetastore()
    {
        IcebergFileHiveMetastore fileHiveMetastore = new IcebergFileHiveMetastore(getHdfsEnvironment(),
                Optional.of(getCatalogDirectory(HIVE))
                        .filter(File::exists)
                        .map(File::getPath)
                        .orElseThrow(() -> new RuntimeException("Catalog directory does not exist: " + getCatalogDirectory(HIVE))),
                "test");
        return memoizeMetastore(fileHiveMetastore, false, 1000, 0);
    }

    protected static HdfsEnvironment getHdfsEnvironment()
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig),
                ImmutableSet.of(),
                hiveClientConfig);
        return new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
    }

    protected File getCatalogDirectory(CatalogType catalogType)
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        switch (catalogType) {
            case HIVE:
                return dataDirectory
                        .resolve(TEST_DATA_DIRECTORY)
                        .resolve(HIVE.name())
                        .toFile();
            case HADOOP:
            case NESSIE:
                return dataDirectory.toFile();
        }

        throw new PrestoException(NOT_SUPPORTED, "Unsupported Presto Iceberg catalog type " + catalogType);
    }

    private static Map<String, ColumnStatistics> getColumnNameMap(TableStatistics statistics)
    {
        return statistics.getColumnStatistics().entrySet().stream().collect(Collectors.toMap(e ->
                        ((IcebergColumnHandle) e.getKey()).getName(),
                Map.Entry::getValue));
    }

    static void assertNDVsPresent(TableStatistics stats)
    {
        for (Map.Entry<ColumnHandle, ColumnStatistics> entry : stats.getColumnStatistics().entrySet()) {
            assertFalse(entry.getValue().getDistinctValuesCount().isUnknown(), entry.getKey() + " NDVs are unknown");
        }
    }

    static void assertNDVsNotPresent(TableStatistics stats)
    {
        for (Map.Entry<ColumnHandle, ColumnStatistics> entry : stats.getColumnStatistics().entrySet()) {
            assertTrue(entry.getValue().getDistinctValuesCount().isUnknown(), entry.getKey() + " NDVs are not unknown");
        }
    }
}
