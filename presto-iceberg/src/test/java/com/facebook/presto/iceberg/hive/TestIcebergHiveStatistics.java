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
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergMetadataColumn;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static com.facebook.presto.iceberg.IcebergSessionProperties.HIVE_METASTORE_STATISTICS_MERGE_STRATEGY;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
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
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(ImmutableMap.of(), ImmutableMap.of("iceberg.hive-statistics-merge-strategy", NUMBER_OF_DISTINCT_VALUES.name()));
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
        assertQuerySucceeds("ANALYZE mergeFlagsStats"); // stats stored in
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

    private TableStatistics getScanStatsEstimate(Session session, @Language("SQL") String sql)
    {
        Plan plan = plan(sql, session);
        TableScanNode node = plan.getPlanIdNodeMap().values().stream()
                .filter(planNode -> planNode instanceof TableScanNode)
                .map(TableScanNode.class::cast)
                .findFirst().orElseThrow();
        return transaction(getQueryRunner().getTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(session, (Function<Session, TableStatistics>) txnSession -> getQueryRunner().getMetadata()
                        .getTableStatistics(txnSession,
                                node.getTable(),
                                ImmutableList.copyOf(node.getAssignments().values()),
                                new Constraint<>(node.getCurrentConstraint())));
    }

    private TableStatistics getTableStatistics(Session session, String table)
    {
        Metadata meta = getQueryRunner().getMetadata();
        TransactionId txid = getQueryRunner().getTransactionManager().beginTransaction(false);
        Session txnSession = session.beginTransactionId(txid, getQueryRunner().getTransactionManager(), new AllowAllAccessControl());
        Map<String, ColumnHandle> columnHandles = getColumnHandles(table, txnSession);
        List<ColumnHandle> columnHandleList = new ArrayList<>(columnHandles.values());
        return meta.getTableStatistics(txnSession, getAnalyzeTableHandle(table, txnSession), columnHandleList, Constraint.alwaysTrue());
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

    private TableHandle getAnalyzeTableHandle(String tableName, Session session)
    {
        Metadata meta = getQueryRunner().getMetadata();
        return meta.getTableHandleForStatisticsCollection(
                session,
                new QualifiedObjectName("iceberg", "tpch", tableName.toLowerCase(Locale.US)),
                Collections.emptyMap()).orElseThrow();
    }

    private TableHandle getTableHandle(String tableName, Session session)
    {
        MetadataResolver resolver = getQueryRunner().getMetadata().getMetadataResolver(session);
        return resolver.getTableHandle(new QualifiedObjectName("iceberg", "tpch", tableName.toLowerCase(Locale.US))).orElseThrow();
    }

    private Map<String, ColumnHandle> getColumnHandles(String tableName, Session session)
    {
        return getQueryRunner().getMetadata().getColumnHandles(session, getTableHandle(tableName, session)).entrySet().stream()
                .filter(entry -> !IcebergMetadataColumn.isMetadataColumnId(((IcebergColumnHandle) (entry.getValue())).getId()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
