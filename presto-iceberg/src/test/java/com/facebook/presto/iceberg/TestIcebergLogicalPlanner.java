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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.PUSHDOWN_DEREFERENCE_ENABLED;
import static com.facebook.presto.common.predicate.Domain.create;
import static com.facebook.presto.common.predicate.Domain.singleValue;
import static com.facebook.presto.common.predicate.Range.greaterThan;
import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.common.predicate.ValueSet.ofRanges;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.iceberg.IcebergColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.iceberg.IcebergColumnHandle.getSynthesizedIcebergColumnHandle;
import static com.facebook.presto.iceberg.IcebergColumnHandle.isPushedDownSubfield;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PARQUET_DEREFERENCE_PUSHDOWN_ENABLED;
import static com.facebook.presto.parquet.ParquetTypeUtils.pushdownColumnNameForSubfield;
import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class TestIcebergLogicalPlanner
        extends AbstractTestQueryFramework
{
    protected TestIcebergLogicalPlanner() {}

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(ImmutableMap.of("experimental.pushdown-subfields-enabled", "true"), ImmutableMap.of());
    }

    @Test
    public void testParquetDereferencePushDown()
    {
        assertUpdate("CREATE TABLE test_pushdown_nestedcolumn_parquet(" +
                "id bigint, " +
                "x row(a bigint, b varchar, c double, d row(d1 bigint, d2 double))," +
                "y array(row(a bigint, b varchar, c double, d row(d1 bigint, d2 double)))) " +
                "with (format = 'PARQUET')");

        assertParquetDereferencePushDown("SELECT x.a FROM test_pushdown_nestedcolumn_parquet",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        assertParquetDereferencePushDown("SELECT x.a, mod(x.d.d1, 2) FROM test_pushdown_nestedcolumn_parquet",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a", "x.d.d1"));

        assertParquetDereferencePushDown("SELECT x.d, mod(x.d.d1, 2), x.d.d2 FROM test_pushdown_nestedcolumn_parquet",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d"));

        assertParquetDereferencePushDown("SELECT x.a FROM test_pushdown_nestedcolumn_parquet WHERE x.b LIKE 'abc%'",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a", "x.b"));

        Subfield nestedColumn1 = nestedColumn("x.a");
        String pushdownColumnName1 = pushdownColumnNameForSubfield(nestedColumn1);
        assertParquetDereferencePushDown("SELECT x.a FROM test_pushdown_nestedcolumn_parquet WHERE x.a > 10 AND x.b LIKE 'abc%'",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a", "x.b"),
                ImmutableSet.of(pushdownColumnNameForSubfield(nestedColumn("x.a"))),
                withColumnDomains(ImmutableMap.of(getSynthesizedIcebergColumnHandle(pushdownColumnName1, BIGINT, nestedColumn1), create(ofRanges(greaterThan(BIGINT, 10L)), false))));

        // Join
        assertPlan(withParquetDereferencePushDownEnabled(), "SELECT l.orderkey, x.a, mod(x.d.d1, 2) FROM lineitem l, test_pushdown_nestedcolumn_parquet a WHERE l.linenumber = a.id",
                anyTree(
                        node(JoinNode.class,
                                anyTree(tableScan("lineitem", ImmutableMap.of())),
                                anyTree(tableScanParquetDeferencePushDowns("test_pushdown_nestedcolumn_parquet", nestedColumnMap("x.a", "x.d.d1"))))));

        assertPlan(withParquetDereferencePushDownEnabled(), "SELECT l.orderkey, x.a, mod(x.d.d1, 2) FROM lineitem l, test_pushdown_nestedcolumn_parquet a WHERE l.linenumber = a.id AND x.a > 10",
                anyTree(
                        node(JoinNode.class,
                                anyTree(tableScan("lineitem", ImmutableMap.of())),
                                anyTree(tableScanParquetDeferencePushDowns(
                                        "test_pushdown_nestedcolumn_parquet",
                                        nestedColumnMap("x.a", "x.d.d1"),
                                        ImmutableSet.of(pushdownColumnNameForSubfield(nestedColumn("x.a"))),
                                        withColumnDomains(ImmutableMap.of(getSynthesizedIcebergColumnHandle(pushdownColumnName1, BIGINT, nestedColumn1), create(ofRanges(greaterThan(BIGINT, 10L)), false))))))));
        // Aggregation
        assertParquetDereferencePushDown("SELECT id, min(x.a) FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        assertParquetDereferencePushDown("SELECT id, min(mod(x.a, 3)) FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        assertParquetDereferencePushDown("SELECT id, min(x.a) FILTER (WHERE x.b LIKE 'abc%') FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a", "x.b"));

        assertParquetDereferencePushDown("SELECT id, min(x.a + 1) * avg(x.d.d1) FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a", "x.d.d1"));

        assertParquetDereferencePushDown("SELECT id, arbitrary(x.a) FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        // Dereference can't be pushed down, but the subfield pushdown will help in pruning the number of columns to read
        assertPushdownSubfields("SELECT id, arbitrary(x.a) FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                ImmutableMap.of("x", toSubfields("x.a")));

        // Dereference can't be pushed down, but the subfield pushdown will help in pruning the number of columns to read
        assertPushdownSubfields("SELECT id, arbitrary(x).d.d1 FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                ImmutableMap.of("x", toSubfields("x.d.d1")));

        assertParquetDereferencePushDown("SELECT id, arbitrary(x.d).d1 FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d"));

        assertParquetDereferencePushDown("SELECT id, arbitrary(x.d.d2) FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d.d2"));

        // Unnest
        assertParquetDereferencePushDown("SELECT t.a, t.d.d1, x.a FROM test_pushdown_nestedcolumn_parquet CROSS JOIN UNNEST(y) as t(a, b, c, d)",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        assertParquetDereferencePushDown("SELECT t.*, x.a FROM test_pushdown_nestedcolumn_parquet CROSS JOIN UNNEST(y) as t(a, b, c, d)",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        assertParquetDereferencePushDown("SELECT id, x.a FROM test_pushdown_nestedcolumn_parquet CROSS JOIN UNNEST(y) as t(a, b, c, d)",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        // Legacy unnest
        Session legacyUnnest = Session.builder(withParquetDereferencePushDownEnabled())
                .setSystemProperty("legacy_unnest", "true")
                .build();
        assertParquetDereferencePushDown(legacyUnnest, "SELECT t.y.a, t.y.d.d1, x.a FROM test_pushdown_nestedcolumn_parquet CROSS JOIN UNNEST(y) as t(y)",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        assertParquetDereferencePushDown(legacyUnnest, "SELECT t.*, x.a FROM test_pushdown_nestedcolumn_parquet CROSS JOIN UNNEST(y) as t(y)",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        assertParquetDereferencePushDown(legacyUnnest, "SELECT id, x.a FROM test_pushdown_nestedcolumn_parquet CROSS JOIN UNNEST(y) as t(y)",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        // Case sensitivity
        assertParquetDereferencePushDown("SELECT x.a, x.b, x.A + 2 FROM test_pushdown_nestedcolumn_parquet WHERE x.B LIKE 'abc%'",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a", "x.b"));

        // No pass-through nested column pruning
        assertParquetDereferencePushDown("SELECT id, min(x.d).d1 FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d"));

        assertParquetDereferencePushDown("SELECT id, min(x.d).d1, min(x.d.d2) FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d"));

        // Test pushdown of filters on dereference columns
        nestedColumn1 = nestedColumn("x.d.d1");
        pushdownColumnName1 = pushdownColumnNameForSubfield(nestedColumn1);
        assertParquetDereferencePushDown("SELECT id, x.d.d1 FROM test_pushdown_nestedcolumn_parquet WHERE x.d.d1 = 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d.d1"),
                ImmutableSet.of(pushdownColumnNameForSubfield(nestedColumn("x.d.d1"))),
                withColumnDomains(ImmutableMap.of(
                        getSynthesizedIcebergColumnHandle(pushdownColumnName1, BIGINT, nestedColumn1), singleValue(BIGINT, 1L))));

        Subfield nestedColumn2 = nestedColumn("x.d.d2");
        String pushdownColumnName2 = pushdownColumnNameForSubfield(nestedColumn2);
        assertParquetDereferencePushDown("SELECT id, x.d.d1 FROM test_pushdown_nestedcolumn_parquet WHERE x.d.d1 = 1 and x.d.d2 = 5.0",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d.d1", "x.d.d2"),
                ImmutableSet.of(
                        pushdownColumnNameForSubfield(nestedColumn("x.d.d1")),
                        pushdownColumnNameForSubfield(nestedColumn("x.d.d2"))),
                withColumnDomains(ImmutableMap.of(
                        getSynthesizedIcebergColumnHandle(pushdownColumnName1, BIGINT, nestedColumn1), singleValue(BIGINT, 1L),
                        getSynthesizedIcebergColumnHandle(pushdownColumnName2, DOUBLE, nestedColumn2), singleValue(DOUBLE, 5.0))));

        assertUpdate("DROP TABLE test_pushdown_nestedcolumn_parquet");
    }

    private static PlanMatchPattern tableScanParquetDeferencePushDowns(String expectedTableName, Map<String, Subfield> expectedDeferencePushDowns)
    {
        return PlanMatchPattern.tableScan(expectedTableName).with(new IcebergParquetDereferencePushdownMatcher(expectedDeferencePushDowns, ImmutableSet.of(), TupleDomain.all()));
    }

    private static PlanMatchPattern tableScanParquetDeferencePushDowns(
            String expectedTableName,
            Map<String, Subfield> expectedDeferencePushDowns,
            Set<String> predicateColumns,
            TupleDomain<IcebergColumnHandle> predicate)
    {
        return PlanMatchPattern.tableScan(expectedTableName).with(new IcebergParquetDereferencePushdownMatcher(expectedDeferencePushDowns, predicateColumns, predicate));
    }

    private void assertParquetDereferencePushDown(@Language("SQL") String query, String tableName, Map<String, Subfield> expectedDeferencePushDowns)
    {
        assertParquetDereferencePushDown(withParquetDereferencePushDownEnabled(), query, tableName, expectedDeferencePushDowns);
    }

    private void assertParquetDereferencePushDown(@Language("SQL") String query, String tableName, Map<String, Subfield> expectedDeferencePushDowns,
            Set<String> predicateColumns,
            TupleDomain<IcebergColumnHandle> predicate)
    {
        assertPlan(withParquetDereferencePushDownEnabled(), query,
                anyTree(tableScanParquetDeferencePushDowns(tableName, expectedDeferencePushDowns, predicateColumns, predicate)));
    }

    private void assertParquetDereferencePushDown(Session session, @Language("SQL") String query, String tableName, Map<String, Subfield> expectedDeferencePushDowns)
    {
        assertPlan(session, query, anyTree(tableScanParquetDeferencePushDowns(tableName, expectedDeferencePushDowns)));
    }

    private Session withParquetDereferencePushDownEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PUSHDOWN_DEREFERENCE_ENABLED, "true")
                .setCatalogSessionProperty(ICEBERG_CATALOG, PARQUET_DEREFERENCE_PUSHDOWN_ENABLED, "true")
                .build();
    }

    private static Set<Subfield> toSubfields(String... subfieldPaths)
    {
        return Arrays.stream(subfieldPaths)
                .map(Subfield::new)
                .collect(toImmutableSet());
    }

    private void assertPushdownSubfields(@Language("SQL") String query, String tableName, Map<String, Set<Subfield>> requiredSubfields)
    {
        assertPlan(query, anyTree(tableScan(tableName, requiredSubfields)));
    }

    private static PlanMatchPattern tableScan(String expectedTableName, Map<String, Set<Subfield>> expectedRequiredSubfields)
    {
        return PlanMatchPattern.tableScan(expectedTableName).with(new IcebergTableScanMatcher(expectedRequiredSubfields));
    }

    private static final class IcebergTableScanMatcher
            implements Matcher
    {
        private final Map<String, Set<Subfield>> requiredSubfields;

        private IcebergTableScanMatcher(Map<String, Set<Subfield>> requiredSubfields)
        {
            this.requiredSubfields = requireNonNull(requiredSubfields, "requiredSubfields is null");
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof TableScanNode;
        }

        @Override
        public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            TableScanNode tableScan = (TableScanNode) node;
            for (ColumnHandle column : tableScan.getAssignments().values()) {
                IcebergColumnHandle icebergColumn = (IcebergColumnHandle) column;
                String columnName = icebergColumn.getName();
                if (requiredSubfields.containsKey(columnName)) {
                    if (!requiredSubfields.get(columnName).equals(ImmutableSet.copyOf(icebergColumn.getRequiredSubfields()))) {
                        return NO_MATCH;
                    }
                }
                else {
                    if (!icebergColumn.getRequiredSubfields().isEmpty()) {
                        return NO_MATCH;
                    }
                }
            }

            return match();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("requiredSubfields", requiredSubfields)
                    .toString();
        }
    }

    private static final class IcebergParquetDereferencePushdownMatcher
            implements Matcher
    {
        private final Map<String, Subfield> dereferenceColumns;
        private final Set<String> predicateColumns;
        private final TupleDomain<IcebergColumnHandle> predicate;

        private IcebergParquetDereferencePushdownMatcher(
                Map<String, Subfield> dereferenceColumns,
                Set<String> predicateColumns,
                TupleDomain<IcebergColumnHandle> predicate)
        {
            this.dereferenceColumns = requireNonNull(dereferenceColumns, "dereferenceColumns is null");
            this.predicateColumns = requireNonNull(predicateColumns, "predicateColumns is null");
            this.predicate = requireNonNull(predicate, "predicate is null");
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof TableScanNode;
        }

        @Override
        public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            TableScanNode tableScan = (TableScanNode) node;
            for (ColumnHandle column : tableScan.getAssignments().values()) {
                IcebergColumnHandle icebergColumn = (IcebergColumnHandle) column;
                String columnName = icebergColumn.getName();
                if (dereferenceColumns.containsKey(columnName)) {
                    if (icebergColumn.getColumnType() != SYNTHESIZED ||
                            icebergColumn.getRequiredSubfields().size() != 1 ||
                            !icebergColumn.getRequiredSubfields().get(0).equals(dereferenceColumns.get(columnName))) {
                        return NO_MATCH;
                    }
                    dereferenceColumns.remove(columnName);
                }
                else {
                    if (isPushedDownSubfield(icebergColumn)) {
                        return NO_MATCH;
                    }
                }
            }

            if (!dereferenceColumns.isEmpty()) {
                return NO_MATCH;
            }

            Optional<ConnectorTableLayoutHandle> layout = tableScan.getTable().getLayout();

            if (!layout.isPresent()) {
                return NO_MATCH;
            }

            IcebergTableLayoutHandle layoutHandle = (IcebergTableLayoutHandle) layout.get();

            Optional<List<TupleDomain.ColumnDomain<ColumnHandle>>> columnDomains = layoutHandle.getTupleDomain().getColumnDomains();
            Set<String> actualPredicateColumns = ImmutableSet.of();
            if (columnDomains.isPresent()) {
                actualPredicateColumns = columnDomains.get().stream()
                        .map(TupleDomain.ColumnDomain::getColumn)
                        .filter(c -> c instanceof IcebergColumnHandle)
                        .map(c -> ((IcebergColumnHandle) c).getName())
                        .collect(Collectors.toSet());
            }

            if (!Objects.equals(actualPredicateColumns, predicateColumns)
                    || !Objects.equals(layoutHandle.getTupleDomain(), predicate)) {
                return NO_MATCH;
            }

            return match();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("dereferenceColumns", dereferenceColumns)
                    .add("predicateColumns", predicateColumns)
                    .add("predicate", predicate)
                    .toString();
        }
    }

    private static Map<String, Subfield> nestedColumnMap(String... columns)
    {
        return Arrays.stream(columns).collect(Collectors.toMap(
                column -> pushdownColumnNameForSubfield(nestedColumn(column)),
                TestIcebergLogicalPlanner::nestedColumn));
    }

    private static Subfield nestedColumn(String column)
    {
        return new Subfield(column);
    }
}
