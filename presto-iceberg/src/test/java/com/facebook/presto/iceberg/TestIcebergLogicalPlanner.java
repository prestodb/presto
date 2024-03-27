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
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.PUSHDOWN_DEREFERENCE_ENABLED;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.predicate.Domain.create;
import static com.facebook.presto.common.predicate.Domain.multipleValues;
import static com.facebook.presto.common.predicate.Domain.singleValue;
import static com.facebook.presto.common.predicate.Range.greaterThan;
import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.common.predicate.ValueSet.ofRanges;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.isEntireColumn;
import static com.facebook.presto.iceberg.IcebergColumnHandle.getSynthesizedIcebergColumnHandle;
import static com.facebook.presto.iceberg.IcebergColumnHandle.isPushedDownSubfield;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PARQUET_DEREFERENCE_PUSHDOWN_ENABLED;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.parquet.ParquetTypeUtils.pushdownColumnNameForSubfield;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyNot;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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

    @DataProvider(name = "push_down_filter_enabled")
    public Object[][] pushDownFilter()
    {
        return new Object[][] {
                {true},
                {false}};
    }

    @Test(dataProvider = "push_down_filter_enabled")
    public void testMetadataQueryOptimizer(boolean enabled)
    {
        QueryRunner queryRunner = getQueryRunner();
        Session session = getSessionWithOptimizeMetadataQueries(enabled);
        try {
            queryRunner.execute("create table metadata_optimize(v1 int, v2 varchar, a int, b varchar)" +
                    " with(partitioning = ARRAY['a', 'b'])");
            queryRunner.execute("insert into metadata_optimize values" +
                    " (1, '1001', 1, '1001')," +
                    " (2, '1002', 2, '1001')," +
                    " (3, '1003', 3, '1002')," +
                    " (4, '1004', 4, '1002')");

            assertQuery(session, "select b, max(a), min(a) from metadata_optimize group by b",
                    "values('1001', 2, 1), ('1002', 4, 3)");
            assertPlan(session, "select b, max(a), min(a) from metadata_optimize group by b",
                    anyTree(values(
                            ImmutableList.of("a", "b"),
                            ImmutableList.of(
                                    ImmutableList.of(new LongLiteral("1"), new StringLiteral("1001")),
                                    ImmutableList.of(new LongLiteral("2"), new StringLiteral("1001")),
                                    ImmutableList.of(new LongLiteral("3"), new StringLiteral("1002")),
                                    ImmutableList.of(new LongLiteral("4"), new StringLiteral("1002"))))));

            assertQuery(session, "select distinct a, b from metadata_optimize",
                    "values(1, '1001'), (2, '1001'), (3, '1002'), (4, '1002')");
            assertPlan(session, "select distinct a, b from metadata_optimize",
                    anyTree(values(
                            ImmutableList.of("a", "b"),
                            ImmutableList.of(
                                    ImmutableList.of(new LongLiteral("1"), new StringLiteral("1001")),
                                    ImmutableList.of(new LongLiteral("2"), new StringLiteral("1001")),
                                    ImmutableList.of(new LongLiteral("3"), new StringLiteral("1002")),
                                    ImmutableList.of(new LongLiteral("4"), new StringLiteral("1002"))))));

            assertQuery(session, "select min(a), max(b) from metadata_optimize", "values(1, '1002')");
            assertPlan(session, "select min(a), max(b) from metadata_optimize",
                    anyNot(AggregationNode.class, strictProject(
                            ImmutableMap.of("a", expression("1"), "b", expression("1002")),
                            anyTree(values()))));

            // Do metadata optimization on a complex query
            assertQuery(session, "with tt as (select a, b, concat(cast(a as varchar), b) as c from metadata_optimize where a > 1 and a < 4 order by b desc)" +
                            " select min(a), max(b), approx_distinct(b), c from tt group by c",
                    "values(2, '1001', 1, '21001'), (3, '1002', 1, '31002')");
            assertPlan(session, "with tt as (select a, b, concat(cast(a as varchar), b) as c from metadata_optimize where a > 1 and a < 4 order by b desc)" +
                            " select min(a), max(b), approx_distinct(b), c from tt group by c",
                    anyTree(values(ImmutableList.of("a", "b"),
                            ImmutableList.of(
                                    ImmutableList.of(new LongLiteral("2"), new StringLiteral("1001")),
                                    ImmutableList.of(new LongLiteral("3"), new StringLiteral("1002"))))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS metadata_optimize");
        }
    }

    @Test(dataProvider = "push_down_filter_enabled")
    public void testMetadataQueryOptimizerOnPartitionEvolution(boolean enabled)
    {
        QueryRunner queryRunner = getQueryRunner();
        Session session = getSessionWithOptimizeMetadataQueries(enabled);
        try {
            queryRunner.execute("create table metadata_optimize_partition_evolution(v1 int, v2 varchar, a int, b varchar)" +
                    " with(partitioning = ARRAY['a', 'b'])");
            queryRunner.execute("insert into metadata_optimize_partition_evolution values" +
                    " (1, '1001', 1, '1001')," +
                    " (2, '1002', 2, '1001')," +
                    " (3, '1003', 3, '1002')," +
                    " (4, '1004', 4, '1002')");

            queryRunner.execute("alter table metadata_optimize_partition_evolution add column c int with(partitioning = 'identity')");
            queryRunner.execute("insert into metadata_optimize_partition_evolution values" +
                    " (5, '1005', 5, '1001', 5)," +
                    " (6, '1006', 6, '1002', 6)");

            // Do not affect metadata optimization on original partition columns
            assertQuery(session, "select b, max(a), min(a) from metadata_optimize_partition_evolution group by b",
                    "values('1001', 5, 1), ('1002', 6, 3)");
            assertPlan(session, "select b, max(a), min(a) from metadata_optimize_partition_evolution group by b",
                    anyTree(values(
                            ImmutableList.of("a", "b"),
                            ImmutableList.of(
                                    ImmutableList.of(new LongLiteral("1"), new StringLiteral("1001")),
                                    ImmutableList.of(new LongLiteral("2"), new StringLiteral("1001")),
                                    ImmutableList.of(new LongLiteral("3"), new StringLiteral("1002")),
                                    ImmutableList.of(new LongLiteral("4"), new StringLiteral("1002")),
                                    ImmutableList.of(new LongLiteral("5"), new StringLiteral("1001")),
                                    ImmutableList.of(new LongLiteral("6"), new StringLiteral("1002"))))));

            // Only non-filterPushDown could run on Iceberg Java Connector
            if (!Boolean.valueOf(enabled)) {
                assertQuery(session, "select b, max(c), min(c) from metadata_optimize_partition_evolution group by b",
                        "values('1001', 5, 5), ('1002', 6, 6)");
            }
            // New added partition column is not supported for metadata optimization
            assertPlan(session, "select b, max(c), min(c) from metadata_optimize_partition_evolution group by b",
                    anyTree(strictTableScan("metadata_optimize_partition_evolution", identityMap("b", "c"))));

            // Do not affect metadata optimization on original partition columns
            assertQuery(session, "select distinct a, b from metadata_optimize_partition_evolution",
                    "values(1, '1001'), (2, '1001'), (3, '1002'), (4, '1002'), (5, '1001'), (6, '1002')");
            assertPlan(session, "select distinct a, b from metadata_optimize_partition_evolution",
                    anyTree(values(ImmutableList.of("a", "b"),
                            ImmutableList.of(
                                    ImmutableList.of(new LongLiteral("1"), new StringLiteral("1001")),
                                    ImmutableList.of(new LongLiteral("2"), new StringLiteral("1001")),
                                    ImmutableList.of(new LongLiteral("3"), new StringLiteral("1002")),
                                    ImmutableList.of(new LongLiteral("4"), new StringLiteral("1002")),
                                    ImmutableList.of(new LongLiteral("5"), new StringLiteral("1001")),
                                    ImmutableList.of(new LongLiteral("6"), new StringLiteral("1002"))))));

            // Only non-filterPushDown could run on Iceberg Java Connector
            if (!Boolean.valueOf(enabled)) {
                assertQuery(session, "select distinct a, b, c from metadata_optimize_partition_evolution",
                        "values(1, '1001', NULL), (2, '1001', NULL), (3, '1002', NULL), (4, '1002', NULL), (5, '1001', 5), (6, '1002', 6)");
            }

            // New added partition column is not supported for metadata optimization
            assertPlan(session, "select distinct a, b, c from metadata_optimize_partition_evolution",
                    anyTree(strictTableScan("metadata_optimize_partition_evolution", identityMap("a", "b", "c"))));

            // Do not affect metadata optimization on original partition columns
            assertQuery(session, "select min(a), max(a), min(b), max(b) from metadata_optimize_partition_evolution", "values(1, 6, '1001', '1002')");
            assertPlan(session, "select min(a), max(a), min(b), max(b) from metadata_optimize_partition_evolution",
                    anyNot(AggregationNode.class, strictProject(
                            ImmutableMap.of(
                                    "min(a)", expression("1"),
                                    "max(a)", expression("6"),
                                    "min(b)", expression("1001"),
                                    "max(b)", expression("1002")),
                            anyTree(values()))));

            // Only non-filterPushDown could run on Iceberg Java Connector
            if (!Boolean.valueOf(enabled)) {
                assertQuery(session, "select min(b), max(b), min(c), max(c) from metadata_optimize_partition_evolution", "values('1001', '1002', 5, 6)");
            }

            // New added partition column is not supported for metadata optimization
            assertPlan(session, "select min(b), max(b), min(c), max(c) from metadata_optimize_partition_evolution",
                    anyTree(strictTableScan("metadata_optimize_partition_evolution", identityMap("b", "c"))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS metadata_optimize_partition_evolution");
        }
    }

    @Test(dataProvider = "push_down_filter_enabled")
    public void testMetadataQueryOptimizationWithLimit(boolean enabled)
    {
        QueryRunner queryRunner = getQueryRunner();
        Session sessionWithOptimizeMetadataQueries = getSessionWithOptimizeMetadataQueries(enabled);
        try {
            queryRunner.execute("CREATE TABLE test_metadata_query_optimization_with_limit(a varchar, b int, c int) WITH (partitioning = ARRAY['b', 'c'])");
            queryRunner.execute("INSERT INTO test_metadata_query_optimization_with_limit VALUES" +
                    " ('1001', 1, 1), ('1002', 1, 1), ('1003', 1, 1)," +
                    " ('1004', 1, 2), ('1005', 1, 2), ('1006', 1, 2)," +
                    " ('1007', 2, 1), ('1008', 2, 1), ('1009', 2, 1)");

            // Could do metadata optimization when `limit` existing above `aggregation`
            assertQuery(sessionWithOptimizeMetadataQueries, "select distinct b, c from test_metadata_query_optimization_with_limit order by c desc limit 3",
                    "values(1, 2), (1, 1), (2, 1)");
            assertPlan(sessionWithOptimizeMetadataQueries, "select distinct b, c from test_metadata_query_optimization_with_limit order by c desc limit 3",
                    anyTree(values(ImmutableList.of("b", "c"),
                            ImmutableList.of(
                                    ImmutableList.of(new LongLiteral("1"), new LongLiteral("2")),
                                    ImmutableList.of(new LongLiteral("1"), new LongLiteral("1")),
                                    ImmutableList.of(new LongLiteral("2"), new LongLiteral("1"))))));

            // Should not do metadata optimization when `limit` existing below `aggregation`
            // Only non-filterPushDown could run on Iceberg Java Connector
            if (!Boolean.valueOf(enabled)) {
                assertQuery(sessionWithOptimizeMetadataQueries, "with tt as (select b, c from test_metadata_query_optimization_with_limit order by c desc limit 3) select b, min(c), max(c) from tt group by b",
                        "values(1, 2, 2)");
            }
            assertPlan(sessionWithOptimizeMetadataQueries, "with tt as (select b, c from test_metadata_query_optimization_with_limit order by c desc limit 3) select b, min(c), max(c) from tt group by b",
                    anyTree(strictTableScan("test_metadata_query_optimization_with_limit", identityMap("b", "c"))));
        }
        finally {
            queryRunner.execute("DROP TABLE if exists test_metadata_query_optimization_with_limit");
        }
    }

    @Test(dataProvider = "push_down_filter_enabled")
    public void testMetadataQueryOptimizationWithMetadataEnforcedPredicate(boolean enabled)
    {
        QueryRunner queryRunner = getQueryRunner();
        Session sessionWithOptimizeMetadataQueries = getSessionWithOptimizeMetadataQueries(enabled);
        try {
            queryRunner.execute("CREATE TABLE test_with_metadata_enforced_filter(a varchar, b int, c int) WITH (partitioning = ARRAY['b', 'c'])");
            queryRunner.execute("INSERT INTO test_with_metadata_enforced_filter VALUES" +
                    " ('1001', 1, 1), ('1002', 1, 2), ('1003', 1, 3)," +
                    " ('1007', 2, 1), ('1008', 2, 2), ('1009', 2, 3)");

            // Could do metadata optimization when filtering by all metadata-enforced predicate
            assertQuery(sessionWithOptimizeMetadataQueries, "select b, min(c), max(c) from test_with_metadata_enforced_filter" +
                            " where c > 1 group by b",
                    "values(1, 2, 3), (2, 2, 3)");
            assertPlan(sessionWithOptimizeMetadataQueries, "select b, min(c), max(c) from test_with_metadata_enforced_filter" +
                            " where c > 1 group by b",
                    anyTree(values(ImmutableList.of("b", "c"),
                            ImmutableList.of(
                                    ImmutableList.of(new LongLiteral("1"), new LongLiteral("2")),
                                    ImmutableList.of(new LongLiteral("1"), new LongLiteral("3")),
                                    ImmutableList.of(new LongLiteral("2"), new LongLiteral("2")),
                                    ImmutableList.of(new LongLiteral("2"), new LongLiteral("3"))))));

            // Another kind of metadata-enforced predicate which could not be push down, could do metadata optimization in such conditions as well
            // Only support when do not enable `filter_push_down`
            if (!Boolean.valueOf(enabled)) {
                assertQuery(sessionWithOptimizeMetadataQueries, "select b, min(c), max(c) from test_with_metadata_enforced_filter" +
                                " where b + c > 2 and b + c < 5 group by b",
                        "values(1, 2, 3), (2, 1, 2)");
                assertPlan(sessionWithOptimizeMetadataQueries, "select b, min(c), max(c) from test_with_metadata_enforced_filter" +
                                " where b + c > 2 and b + c < 5 group by b",
                        anyTree(filter("b + c > 2 and b + c < 5",
                                anyTree(values(ImmutableList.of("b", "c"),
                                        ImmutableList.of(
                                                ImmutableList.of(new LongLiteral("1"), new LongLiteral("1")),
                                                ImmutableList.of(new LongLiteral("1"), new LongLiteral("2")),
                                                ImmutableList.of(new LongLiteral("1"), new LongLiteral("3")),
                                                ImmutableList.of(new LongLiteral("2"), new LongLiteral("1")),
                                                ImmutableList.of(new LongLiteral("2"), new LongLiteral("2")),
                                                ImmutableList.of(new LongLiteral("2"), new LongLiteral("3"))))))));
            }
            else {
                assertPlan(sessionWithOptimizeMetadataQueries, "select b, min(c), max(c) from test_with_metadata_enforced_filter" +
                                " where b + c > 2 and b + c < 5 group by b",
                        anyTree(strictTableScan("test_with_metadata_enforced_filter", identityMap("b", "c"))));
            }
        }
        finally {
            queryRunner.execute("DROP TABLE if exists test_with_metadata_enforced_filter");
        }
    }

    @Test(dataProvider = "push_down_filter_enabled")
    public void testMetadataOptimizationWithNonMetadataEnforcedPredicate(boolean enabled)
    {
        QueryRunner queryRunner = getQueryRunner();
        Session sessionWithOptimizeMetadataQueries = getSessionWithOptimizeMetadataQueries(enabled);
        try {
            queryRunner.execute("CREATE TABLE test_with_non_metadata_enforced_filter(a row(r1 varchar ,r2 int), b int, c int, d varchar) WITH (partitioning = ARRAY['b', 'c'])");
            queryRunner.execute("INSERT INTO test_with_non_metadata_enforced_filter VALUES" +
                    " (('1001', 1), 1, 1, 'd001'), (('1002', 2), 1, 1, 'd002'), (('1003', 3), 1, 1, 'd003')," +
                    " (('1004', 4), 1, 2, 'd004'), (('1005', 5), 1, 2, 'd005'), (('1006', 6), 1, 2, 'd006')," +
                    " (('1007', 7), 2, 1, 'd007'), (('1008', 8), 2, 1, 'd008'), (('1009', 9), 2, 1, 'd009')");

            // Should not do metadata optimization when filtering by non-pushdown filter
            if (!enabled) {
                assertQuery(sessionWithOptimizeMetadataQueries, "select b, min(c), max(c) from test_with_non_metadata_enforced_filter" +
                                " where a.r1 >= '1003' and a.r1 <= '1007' group by b",
                        "values(1, 1, 2), (2, 1, 1)");
                assertPlan(sessionWithOptimizeMetadataQueries, "select b, min(c), max(c) from test_with_non_metadata_enforced_filter" +
                                " where a.r1 >= '1003' and a.r1 <= '1007' group by b",
                        anyTree(filter("a.r1 between '1003' and '1007'",
                                strictTableScan("test_with_non_metadata_enforced_filter", identityMap("a", "b", "c")))));

                assertQuery(sessionWithOptimizeMetadataQueries, "select b, min(c), max(c) from test_with_non_metadata_enforced_filter" +
                                " where d >= 'd003' and d <= 'd007' group by b",
                        "values(1, 1, 2), (2, 1, 1)");
                assertPlan(sessionWithOptimizeMetadataQueries, "select b, min(c), max(c) from test_with_non_metadata_enforced_filter" +
                                " where d >= 'd003' and d <= 'd007' group by b",
                        anyTree(filter("d between 'd003' and 'd007'",
                                strictTableScan("test_with_non_metadata_enforced_filter", identityMap("d", "b", "c")))));
            }
            else {
                assertPlan(sessionWithOptimizeMetadataQueries, "select b, min(c), max(c) from test_with_non_metadata_enforced_filter" +
                                " where a.r1 >= '1003' and a.r1 <= '1007' group by b",
                        anyTree(strictTableScan("test_with_non_metadata_enforced_filter", identityMap("b", "c"))));

                assertPlan(sessionWithOptimizeMetadataQueries, "select b, min(c), max(c) from test_with_non_metadata_enforced_filter" +
                                " where d >= 'd003' and d <= 'd007' group by b",
                        anyTree(strictTableScan("test_with_non_metadata_enforced_filter", identityMap("b", "c"))));
            }
        }
        finally {
            queryRunner.execute("DROP TABLE if exists test_with_non_metadata_enforced_filter");
        }
    }

    @Test(dataProvider = "push_down_filter_enabled")
    public void testMetadataQueryOptimizerOnRowDelete(boolean enabled)
    {
        QueryRunner queryRunner = getQueryRunner();
        Session session = getSessionWithOptimizeMetadataQueries(enabled);
        try {
            queryRunner.execute("create table metadata_optimize_on_row_delete(v1 int, v2 varchar, a int, b varchar)" +
                    " with(partitioning = ARRAY['a', 'b'])");
            queryRunner.execute("insert into metadata_optimize_on_row_delete values" +
                    " (1, '1001', 1, '1001')," +
                    " (2, '1002', 2, '1001')," +
                    " (3, '1003', 3, '1002')," +
                    " (4, '1004', 4, '1002')");

            assertUpdate("delete from metadata_optimize_on_row_delete where v1 >= 2", 3);

            // Only non-filterPushDown could run on Iceberg Java Connector
            if (!Boolean.valueOf(enabled)) {
                assertQuery(session, "select b, max(a), min(a) from metadata_optimize_on_row_delete group by b",
                        "values('1001', 1, 1)");
                assertQuery(session, "select distinct a, b from metadata_optimize_on_row_delete",
                        "values(1, '1001')");
                assertQuery(session, "select min(a), max(a), min(b), max(b) from metadata_optimize_on_row_delete",
                        "values(1, 1, '1001', '1001')");
            }

            // Skip metadata optimization when there existing delete files
            assertPlan(session, "select b, max(a), min(a) from metadata_optimize_on_row_delete group by b",
                    anyTree(strictTableScan("metadata_optimize_on_row_delete", identityMap("a", "b"))));
            assertPlan(session, "select distinct a, b from metadata_optimize_on_row_delete",
                    anyTree(strictTableScan("metadata_optimize_on_row_delete", identityMap("a", "b"))));
            assertPlan(session, "select min(a), max(a), min(b), max(b) from metadata_optimize_on_row_delete",
                    anyTree(strictTableScan("metadata_optimize_on_row_delete", identityMap("a", "b"))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS metadata_optimize_on_row_delete");
        }
    }

    @Test(dataProvider = "push_down_filter_enabled")
    public void testMetadataQueryOptimizerOnMetadataDelete(boolean enabled)
    {
        QueryRunner queryRunner = getQueryRunner();
        Session session = getSessionWithOptimizeMetadataQueries(enabled);
        try {
            queryRunner.execute("create table metadata_optimize_on_metadata_delete(v1 int, v2 varchar, a int, b varchar)" +
                    " with(partitioning = ARRAY['a', 'b'])");
            queryRunner.execute("insert into metadata_optimize_on_metadata_delete values" +
                    " (0, '1000', 0, '1001')," +
                    " (1, '1001', 1, '1001')," +
                    " (2, '1002', 2, '1001')," +
                    " (3, '1003', 3, '1002')," +
                    " (4, '1004', 4, '1002')");

            assertUpdate("delete from metadata_optimize_on_metadata_delete where a >= 2", 3);

            // Do not affect metadata optimization on metadata delete
            assertQuery(session, "select b, max(a), min(a) from metadata_optimize_on_metadata_delete group by b",
                    "values('1001', 1, 0)");
            assertPlan(session, "select b, max(a), min(a) from metadata_optimize_on_metadata_delete group by b",
                    anyTree(values(
                            ImmutableList.of("a", "b"),
                            ImmutableList.of(
                                    ImmutableList.of(new LongLiteral("0"), new StringLiteral("1001")),
                                    ImmutableList.of(new LongLiteral("1"), new StringLiteral("1001"))))));

            // Do not affect metadata optimization on metadata delete
            assertQuery(session, "select distinct a, b from metadata_optimize_on_metadata_delete",
                    "values(1, '1001'), (0, '1001')");
            assertPlan(session, "select distinct a, b from metadata_optimize_on_metadata_delete",
                    anyTree(values(
                            ImmutableList.of("a", "b"),
                            ImmutableList.of(
                                    ImmutableList.of(new LongLiteral("0"), new StringLiteral("1001")),
                                    ImmutableList.of(new LongLiteral("1"), new StringLiteral("1001"))))));

            // Do not affect metadata optimization on metadata delete
            assertQuery(session, "select min(a), max(b) from metadata_optimize_on_metadata_delete", "values(0, '1001')");
            assertPlan(session, "select min(a), max(b) from metadata_optimize_on_metadata_delete",
                    anyNot(AggregationNode.class, strictProject(
                            ImmutableMap.of("a", expression("0"), "b", expression("1001")),
                            anyTree(values()))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS metadata_optimize_on_metadata_delete");
        }
    }

    @Test(dataProvider = "push_down_filter_enabled")
    public void testFilterByUnmatchedValue(boolean enabled)
    {
        Session session = getSessionWithOptimizeMetadataQueries(enabled);
        String tableName = "test_filter_by_unmatched_value";
        assertUpdate("CREATE TABLE " + tableName + " (a varchar, b integer, r row(c int, d varchar)) WITH(partitioning = ARRAY['a'])");

        // query with normal column filter on empty table
        assertPlan(session, "select a, r from " + tableName + " where b = 1001",
                output(values("a", "r")));

        // query with partition column filter on empty table
        assertPlan(session, "select b, r from " + tableName + " where a = 'var3'",
                output(values("b", "r")));

        assertUpdate("INSERT INTO " + tableName + " VALUES ('var1', 1, (1001, 't1')), ('var1', 3, (1003, 't3'))", 2);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('var2', 8, (1008, 't8')), ('var2', 10, (1010, 't10'))", 2);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('var1', 2, (1002, 't2')), ('var1', 9, (1009, 't9'))", 2);

        // query with unmatched normal column filter
        assertPlan(session, "select a, r from " + tableName + " where b = 1001",
                output(values("a", "r")));

        // query with unmatched partition column filter
        assertPlan(session, "select b, r from " + tableName + " where a = 'var3'",
                output(values("b", "r")));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testFiltersWithPushdownDisable()
    {
        // The filter pushdown session property is disabled by default
        Session sessionWithoutFilterPushdown = getQueryRunner().getDefaultSession();

        assertUpdate("CREATE TABLE test_filters_with_pushdown_disable(id int, name varchar, r row(a int, b varchar)) with (partitioning = ARRAY['id'])");
        assertUpdate("INSERT INTO test_filters_with_pushdown_disable VALUES(10, 'adam', (10, 'adam')), (11, 'hd001', (11, 'hd001'))", 2);

        // Only identity partition column predicates, would be enforced totally by tableScan
        assertPlan(sessionWithoutFilterPushdown, "SELECT name, r FROM test_filters_with_pushdown_disable WHERE id = 10",
                output(exchange(
                        strictTableScan("test_filters_with_pushdown_disable", identityMap("name", "r")))),
                plan -> assertTableLayout(
                        plan,
                        "test_filters_with_pushdown_disable",
                        withColumnDomains(ImmutableMap.of(new Subfield(
                                        "id",
                                        ImmutableList.of()),
                                singleValue(INTEGER, 10L))),
                        TRUE_CONSTANT,
                        ImmutableSet.of("id")));

        // Only normal column predicates, would not be enforced by tableScan
        assertPlan(sessionWithoutFilterPushdown, "SELECT id, r FROM test_filters_with_pushdown_disable WHERE name = 'adam'",
                output(exchange(project(
                        filter("name='adam'",
                                strictTableScan("test_filters_with_pushdown_disable", identityMap("id", "name", "r")))))));

        // Only subfield column predicates, would not be enforced by tableScan
        assertPlan(sessionWithoutFilterPushdown, "SELECT id, name FROM test_filters_with_pushdown_disable WHERE r.a = 10",
                output(exchange(project(
                        filter("r.a=10",
                                strictTableScan("test_filters_with_pushdown_disable", identityMap("id", "name", "r")))))));

        // Predicates with identity partition column and normal column
        // The predicate was enforced partially by tableScan, so the filterNode drop it's filter condition `id=10`
        assertPlan(sessionWithoutFilterPushdown, "SELECT id, r FROM test_filters_with_pushdown_disable WHERE id = 10 and name = 'adam'",
                output(exchange(project(
                        ImmutableMap.of("id", expression("10")),
                        filter("name='adam'",
                                strictTableScan("test_filters_with_pushdown_disable", identityMap("name", "r")))))));

        // Predicates with identity partition column and subfield column
        // The predicate was enforced partially by tableScan, so the filterNode drop it's filter condition `id=10`
        assertPlan(sessionWithoutFilterPushdown, "SELECT id, name FROM test_filters_with_pushdown_disable WHERE id = 10 and r.b = 'adam'",
                output(exchange(project(
                        ImmutableMap.of("id", expression("10")),
                        filter("r.b='adam'",
                                strictTableScan("test_filters_with_pushdown_disable", identityMap("name", "r")))))));

        // Predicates expression `in` for identity partition columns could be enforced by iceberg table as well
        assertPlan(sessionWithoutFilterPushdown, "SELECT id, name FROM test_filters_with_pushdown_disable WHERE id in (1, 3, 5, 7, 9, 10) and r.b = 'adam'",
                output(exchange(project(
                        filter("r.b='adam'",
                                strictTableScan("test_filters_with_pushdown_disable", identityMap("id", "name", "r")))))));

        // When predicate simplification causing changes in the predicate, it could not be enforced by iceberg table
        String params = "(" + Joiner.on(", ").join(IntStream.rangeClosed(1, 50).mapToObj(i -> String.valueOf(2 * i + 1)).toArray()) + ")";
        assertPlan(sessionWithoutFilterPushdown, "SELECT name FROM test_filters_with_pushdown_disable WHERE id in " + params + " and r.b = 'adam'",
                output(exchange(project(
                        filter("r.b='adam' AND id in " + params,
                                strictTableScan("test_filters_with_pushdown_disable", identityMap("id", "name", "r")))))));

        // Add a new identity partitioned column for iceberg table
        assertUpdate("ALTER TABLE test_filters_with_pushdown_disable add column newpart bigint with (partitioning = 'identity')");
        assertUpdate("INSERT INTO test_filters_with_pushdown_disable VALUES(10, 'newman', (10, 'newman'), 1001)", 1);

        // Predicates with originally present identity partition column and newly added identity partition column
        // Only the predicate on originally present identity partition column could be enforced by tableScan
        assertPlan(sessionWithoutFilterPushdown, "SELECT id, name FROM test_filters_with_pushdown_disable WHERE id = 10 and newpart = 1001",
                output(exchange(project(
                        ImmutableMap.of("id", expression("10")),
                        filter("newpart=1001",
                                strictTableScan("test_filters_with_pushdown_disable", identityMap("name", "newpart")))))));

        assertUpdate("DROP TABLE test_filters_with_pushdown_disable");

        assertUpdate("CREATE TABLE test_filters_with_pushdown_disable(id int, name varchar, r row(a int, b varchar)) with (partitioning = ARRAY['id', 'truncate(name, 2)'])");
        assertUpdate("INSERT INTO test_filters_with_pushdown_disable VALUES (10, 'hd001', (10, 'newman'))", 1);

        // Predicates with non-identity partitioned column could not be enforced by tableScan
        assertPlan(sessionWithoutFilterPushdown, "SELECT id FROM test_filters_with_pushdown_disable WHERE name = 'hd001'",
                output(exchange(project(
                        filter("name='hd001'",
                                strictTableScan("test_filters_with_pushdown_disable", identityMap("id", "name")))))));

        // Predicates with identity partition column and non-identity partitioned column
        // Only the predicate on identity partition column could be enforced by tableScan
        assertPlan(sessionWithoutFilterPushdown, "SELECT id, r FROM test_filters_with_pushdown_disable WHERE id = 10 and name = 'hd001'",
                output(exchange(project(
                        ImmutableMap.of("id", expression("10")),
                        filter("name='hd001'",
                                strictTableScan("test_filters_with_pushdown_disable", identityMap("name", "r")))))));
        assertUpdate("DROP TABLE test_filters_with_pushdown_disable");
    }

    @Test
    public void testFilterPushdown()
    {
        Session sessionWithFilterPushdown = pushdownFilterEnabled();

        // Only domain predicates
        assertPlan("SELECT linenumber FROM lineitem WHERE partkey = 10",
                output(exchange(project(
                        filter("partkey=10",
                                strictTableScan("lineitem", identityMap("linenumber", "partkey")))))));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE partkey = 10",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        withColumnDomains(ImmutableMap.of(new Subfield(
                                "partkey",
                                ImmutableList.of()),
                                singleValue(BIGINT, 10L))),
                        TRUE_CONSTANT,
                        ImmutableSet.of("partkey")));

        assertPlan(sessionWithFilterPushdown, "SELECT partkey, linenumber FROM lineitem WHERE partkey = 10",
                output(exchange(
                        project(ImmutableMap.of("partkey", expression("10")),
                                strictTableScan("lineitem", identityMap("linenumber"))))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        withColumnDomains(ImmutableMap.of(new Subfield(
                                "partkey",
                                ImmutableList.of()),
                                singleValue(BIGINT, 10L))),
                        TRUE_CONSTANT,
                        ImmutableSet.of("partkey")));

        // Remaining predicate is NULL
        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE cardinality(NULL) > 0",
                output(values("linenumber")));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE orderkey > 10 AND cardinality(NULL) > 0",
                output(values("linenumber")));

        // Remaining predicate is always FALSE
        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE cardinality(ARRAY[1]) > 1",
                output(values("linenumber")));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE orderkey > 10 AND cardinality(ARRAY[1]) > 1",
                output(values("linenumber")));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE orderkey = 10 OR cardinality(ARRAY[1]) > 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        withColumnDomains(ImmutableMap.of(new Subfield(
                                        "orderkey",
                                        ImmutableList.of()),
                                singleValue(BIGINT, 10L))),
                        TRUE_CONSTANT,
                        ImmutableSet.of("orderkey")));

        // Remaining predicate is always TRUE
        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE cardinality(ARRAY[1]) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        TupleDomain.all(),
                        TRUE_CONSTANT,
                        ImmutableSet.of()));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE orderkey = 10 AND cardinality(ARRAY[1]) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        withColumnDomains(ImmutableMap.of(new Subfield(
                                        "orderkey",
                                        ImmutableList.of()),
                                singleValue(BIGINT, 10L))),
                        TRUE_CONSTANT,
                        ImmutableSet.of("orderkey")));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE orderkey = 10 OR cardinality(ARRAY[1]) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        TupleDomain.all(),
                        TRUE_CONSTANT,
                        ImmutableSet.of()));

        // TupleDomain predicate is always FALSE
        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE orderkey = 1 AND orderkey = 2",
                output(values("linenumber")));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE orderkey = 1 AND orderkey = 2 AND linenumber % 2 = 1",
                output(values("linenumber")));

        FunctionAndTypeManager functionAndTypeManager = getQueryRunner().getMetadata().getFunctionAndTypeManager();
        FunctionResolution functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());

        // orderkey % 2 = 1
        RowExpression remainingPredicate = new CallExpression(EQUAL.name(),
                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                BOOLEAN,
                ImmutableList.of(
                        new CallExpression("mod",
                                functionAndTypeManager.lookupFunction("mod", fromTypes(BIGINT, BIGINT)),
                                BIGINT,
                                ImmutableList.of(
                                        new VariableReferenceExpression(Optional.empty(), "orderkey", BIGINT),
                                        constant(2))),
                        constant(1)));

        // Only remaining predicate
        assertPlan("SELECT linenumber FROM lineitem WHERE mod(orderkey, 2) = 1",
                output(exchange(project(
                        filter("mod(orderkey, 2) = 1",
                                strictTableScan("lineitem", identityMap("linenumber", "orderkey")))))));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE mod(orderkey, 2) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        TupleDomain.all(),
                        remainingPredicate,
                        ImmutableSet.of("orderkey")));

        assertPlan(sessionWithFilterPushdown, "SELECT orderkey, linenumber FROM lineitem WHERE mod(orderkey, 2) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("orderkey", "linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        TupleDomain.all(),
                        remainingPredicate,
                        ImmutableSet.of("orderkey")));

        // A mix of domain and remaining predicates
        assertPlan("SELECT linenumber FROM lineitem WHERE partkey = 10 AND mod(orderkey, 2) = 1",
                output(exchange(project(
                        filter("partkey = 10 AND mod(orderkey, 2) = 1",
                                strictTableScan("lineitem", identityMap("linenumber", "orderkey", "partkey")))))));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE partkey = 10 AND mod(orderkey, 2) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        withColumnDomains(ImmutableMap.of(new Subfield(
                                "partkey",
                                ImmutableList.of()),
                                singleValue(BIGINT, 10L))),
                        remainingPredicate,
                        ImmutableSet.of("partkey", "orderkey")));

        assertPlan(sessionWithFilterPushdown, "SELECT partkey, orderkey, linenumber FROM lineitem WHERE partkey = 10 AND mod(orderkey, 2) = 1",
                output(exchange(
                        project(ImmutableMap.of("partkey", expression("10")),
                                strictTableScan("lineitem", identityMap("orderkey", "linenumber"))))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        withColumnDomains(ImmutableMap.of(new Subfield(
                                "partkey",
                                ImmutableList.of()),
                                singleValue(BIGINT, 10L))),
                        remainingPredicate,
                        ImmutableSet.of("partkey", "orderkey")));

        // (partkey = 10 OR orderkey = 5) AND (partkey = 5 OR orderkey = 10)
        RowExpression remainingPredicateForAndOrCombination = new SpecialFormExpression(AND,
                BOOLEAN,
                ImmutableList.of(
                        new SpecialFormExpression(OR,
                                BOOLEAN,
                                ImmutableList.of(
                                        new CallExpression(EQUAL.name(),
                                                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                                                BOOLEAN,
                                                ImmutableList.of(
                                                        new VariableReferenceExpression(Optional.empty(), "partkey", BIGINT),
                                                        constant(10))),
                                        new CallExpression(EQUAL.name(),
                                                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                                                BOOLEAN,
                                                ImmutableList.of(
                                                        new VariableReferenceExpression(Optional.empty(), "orderkey", BIGINT),
                                                        constant(5))))),
                        new SpecialFormExpression(OR,
                                BOOLEAN,
                                ImmutableList.of(
                                        new CallExpression(EQUAL.name(),
                                                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                                                BOOLEAN,
                                                ImmutableList.of(
                                                        new VariableReferenceExpression(Optional.empty(), "orderkey", BIGINT),
                                                        constant(10))),
                                        new CallExpression(EQUAL.name(),
                                                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                                                BOOLEAN,
                                                ImmutableList.of(
                                                        new VariableReferenceExpression(Optional.empty(), "partkey", BIGINT),
                                                        constant(5)))))));

        // A mix of OR, AND in domain predicates
        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE (partkey = 10 and orderkey = 10) OR (partkey = 5 AND orderkey = 5)",
                output(exchange(strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        withColumnDomains(ImmutableMap.of(
                                new Subfield("partkey", ImmutableList.of()),
                                multipleValues(BIGINT, ImmutableList.of(5L, 10L)),
                                new Subfield("orderkey", ImmutableList.of()),
                                multipleValues(BIGINT, ImmutableList.of(5L, 10L)))),
                        remainingPredicateForAndOrCombination,
                        ImmutableSet.of("partkey", "orderkey")));
    }

    @Test
    public void testPartitionPruning()
    {
        QueryRunner queryRunner = getQueryRunner();
        queryRunner.execute("CREATE TABLE test_partition_pruning WITH (partitioning = ARRAY['ds']) AS " +
                "SELECT orderkey, CAST(to_iso8601(date_add('DAY', orderkey % 7, date('2019-11-01'))) AS VARCHAR) AS ds FROM orders WHERE orderkey < 1000");

        Session sessionWithFilterPushdown = pushdownFilterEnabled();
        try {
            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE ds = '2019-11-01'",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", singleValue(VARCHAR, utf8Slice("2019-11-01"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE date(ds) = date('2019-11-01')",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", singleValue(VARCHAR, utf8Slice("2019-11-01"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE date(ds) BETWEEN date('2019-11-02') AND date('2019-11-04')",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-02", "2019-11-03", "2019-11-04"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE ds < '2019-11-05'",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-01", "2019-11-02", "2019-11-03", "2019-11-04"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE date(ds) > date('2019-11-02')",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-03", "2019-11-04", "2019-11-05", "2019-11-06", "2019-11-07"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE ds < '2019-11-05' AND date(ds) > date('2019-11-02')",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-03", "2019-11-04"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE ds < '2019-11-05' OR ds = '2019-11-07'",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-01", "2019-11-02", "2019-11-03", "2019-11-04", "2019-11-07"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE (ds < '2019-11-05' AND date(ds) > date('2019-11-02')) OR (ds > '2019-11-05' AND ds < '2019-11-07')",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-03", "2019-11-04", "2019-11-06"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE ds = '2019-11-01' AND orderkey = 7",
                    anyTree(tableScanWithConstraint(
                            "test_partition_pruning",
                            ImmutableMap.of(
                                    "ds", singleValue(VARCHAR, utf8Slice("2019-11-01")),
                                    "orderkey", singleValue(BIGINT, 7L)))),
                    plan -> assertTableLayout(
                            plan,
                            "test_partition_pruning",
                            withColumnDomains(ImmutableMap.of(new Subfield(
                                            "orderkey",
                                            ImmutableList.of()),
                                    singleValue(BIGINT, 7L))),
                            TRUE_CONSTANT,
                            ImmutableSet.of("orderkey")));
        }
        finally {
            queryRunner.execute("DROP TABLE test_partition_pruning");
        }
    }

    @Test
    public void testParquetDereferencePushDown()
    {
        assertUpdate("CREATE TABLE test_pushdown_nestedcolumn_parquet(" +
                "id bigint, " +
                "x row(a bigint, b varchar, c double, d row(d1 bigint, d2 double))," +
                "y array(row(a bigint, b varchar, c double, d row(d1 bigint, d2 double)))) " +
                "with (format = 'PARQUET')");
        assertUpdate("INSERT INTO test_pushdown_nestedcolumn_parquet(id, x) VALUES(1, (11, 'abcd', 1.1, (1, 5.0)))", 1);

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

    private RowExpression constant(long value)
    {
        return new ConstantExpression(value, BIGINT);
    }

    private static Map<String, String> identityMap(String... values)
    {
        return Arrays.stream(values).collect(toImmutableMap(Functions.identity(), Functions.identity()));
    }

    private void assertTableLayout(Plan plan, String tableName, TupleDomain<Subfield> domainPredicate, RowExpression remainingPredicate, Set<String> predicateColumnNames)
    {
        TableScanNode tableScan = searchFrom(plan.getRoot())
                .where(node -> isTableScanNode(node, tableName))
                .findOnlyElement();

        assertTrue(tableScan.getTable().getLayout().isPresent());
        IcebergTableLayoutHandle layoutHandle = (IcebergTableLayoutHandle) tableScan.getTable().getLayout().get();

        assertEquals(layoutHandle.getPredicateColumns().keySet(), predicateColumnNames);
        assertEquals(layoutHandle.getDomainPredicate(), domainPredicate);
        assertEquals(layoutHandle.getRemainingPredicate(), remainingPredicate);
    }

    private static boolean isTableScanNode(PlanNode node, String tableName)
    {
        return node instanceof TableScanNode && ((IcebergTableHandle) ((TableScanNode) node).getTable().getConnectorHandle()).getIcebergTableName().getTableName().equals(tableName);
    }

    private Session pushdownFilterEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .build();
    }

    private Session withParquetDereferencePushDownEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PUSHDOWN_DEREFERENCE_ENABLED, "true")
                .setCatalogSessionProperty(ICEBERG_CATALOG, PARQUET_DEREFERENCE_PUSHDOWN_ENABLED, "true")
                .build();
    }

    private Session withoutParquetDereferencePushDownEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PUSHDOWN_DEREFERENCE_ENABLED, "false")
                .setCatalogSessionProperty(ICEBERG_CATALOG, PARQUET_DEREFERENCE_PUSHDOWN_ENABLED, "false")
                .build();
    }

    protected Session getSessionWithOptimizeMetadataQueries(boolean enabled)
    {
        return Session.builder(super.getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, String.valueOf(enabled))
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
        assertPlan(withoutParquetDereferencePushDownEnabled(), query, anyTree(tableScan(tableName, requiredSubfields)));
    }

    private static PlanMatchPattern tableScan(String expectedTableName, Map<String, Set<Subfield>> expectedRequiredSubfields)
    {
        return PlanMatchPattern.tableScan(expectedTableName).with(new IcebergTableScanMatcher(expectedRequiredSubfields));
    }

    private static List<Slice> utf8Slices(String... values)
    {
        return Arrays.stream(values).map(Slices::utf8Slice).collect(toImmutableList());
    }

    private static PlanMatchPattern tableScanWithConstraint(String tableName, Map<String, Domain> expectedConstraint)
    {
        return PlanMatchPattern.tableScan(tableName).with(new Matcher()
        {
            @Override
            public boolean shapeMatches(PlanNode node)
            {
                return node instanceof TableScanNode;
            }

            @Override
            public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
            {
                TableScanNode tableScan = (TableScanNode) node;
                TupleDomain<String> constraint = tableScan.getCurrentConstraint()
                        .transform(IcebergColumnHandle.class::cast)
                        .transform(IcebergColumnHandle::getName);

                if (!expectedConstraint.equals(constraint.getDomains().get())) {
                    return NO_MATCH;
                }

                return match();
            }
        });
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

            TupleDomain<ColumnHandle> tupleDomain = layoutHandle.getDomainPredicate()
                    .transform(subfield -> isEntireColumn(subfield) ? subfield.getRootName() : null)
                    .transform(layoutHandle.getPredicateColumns()::get)
                    .transform(ColumnHandle.class::cast);

            Optional<List<TupleDomain.ColumnDomain<ColumnHandle>>> columnDomains = tupleDomain.getColumnDomains();
            Set<String> actualPredicateColumns = ImmutableSet.of();
            if (columnDomains.isPresent()) {
                actualPredicateColumns = columnDomains.get().stream()
                        .map(TupleDomain.ColumnDomain::getColumn)
                        .filter(c -> c instanceof IcebergColumnHandle)
                        .map(c -> ((IcebergColumnHandle) c).getName())
                        .collect(Collectors.toSet());
            }

            if (!Objects.equals(actualPredicateColumns, predicateColumns)
                    || !Objects.equals(tupleDomain, predicate)) {
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
