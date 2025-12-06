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
import com.facebook.presto.Session.SessionBuilder;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
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
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP;
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
import static com.facebook.presto.hive.MetadataUtils.isEntireColumn;
import static com.facebook.presto.iceberg.IcebergColumnHandle.getSynthesizedIcebergColumnHandle;
import static com.facebook.presto.iceberg.IcebergColumnHandle.isPushedDownSubfield;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PARQUET_DEREFERENCE_PUSHDOWN_ENABLED;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isPushdownFilterEnabled;
import static com.facebook.presto.parquet.ParquetTypeUtils.pushdownColumnNameForSubfield;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyNot;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.callDistributedProcedure;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filterWithDecimal;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableFinish;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
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
        return IcebergQueryRunner.builder()
                .setExtraProperties(ImmutableMap.of("experimental.pushdown-subfields-enabled", "true"))
                .build()
                .getQueryRunner();
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
    public void testThoroughlyPushdownForTableWithUnsupportedSpecsIncludingNoData()
    {
        // The filter pushdown session property is disabled by default
        Session sessionWithoutFilterPushdown = getQueryRunner().getDefaultSession();
        assertEquals(isPushdownFilterEnabled(sessionWithoutFilterPushdown.toConnectorSession(new ConnectorId(ICEBERG_CATALOG))), false);

        String tableName = "test_empty_partition_spec_table";
        try {
            // Create a table with no partition
            assertUpdate("CREATE TABLE " + tableName + " (a INTEGER, b VARCHAR) WITH (\"format-version\" = '1')");

            // Do not insert data, and evaluate the partition spec by adding a partition column `c`
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN c INTEGER WITH (partitioning = 'identity')");

            // Insert data under the new partition spec
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, '1001', 1), (2, '1002', 2), (3, '1003', 3), (4, '1004', 4)", 4);

            // Only identity partition column predicates, would be enforced totally by tableScan
            assertPlan(sessionWithoutFilterPushdown, "SELECT a, b FROM " + tableName + " WHERE c > 2",
                    output(exchange(
                            strictTableScan(tableName, identityMap("a", "b")))),
                    plan -> assertTableLayout(
                            plan,
                            tableName,
                            withColumnDomains(ImmutableMap.of(new Subfield(
                                            "c",
                                            ImmutableList.of()),
                                    Domain.create(ValueSet.ofRanges(greaterThan(INTEGER, 2L)), false))),
                            TRUE_CONSTANT,
                            ImmutableSet.of("c")));
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, '1001', 1), (2, '1002', 2), (3, '1003', 3), (4, '1004', 4)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testThoroughlyPushdownForTableWithUnsupportedSpecsWhoseDataAllDeleted()
    {
        // The filter pushdown session property is disabled by default
        Session sessionWithoutFilterPushdown = getQueryRunner().getDefaultSession();
        assertEquals(isPushdownFilterEnabled(sessionWithoutFilterPushdown.toConnectorSession(new ConnectorId(ICEBERG_CATALOG))), false);

        String tableName = "test_data_deleted_partition_spec_table";
        try {
            // Create a table with partition column `a`, and insert some data under this partition spec
            assertUpdate("CREATE TABLE " + tableName + " (a INTEGER, b VARCHAR) WITH (\"format-version\" = '1', partitioning = ARRAY['a'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, '1001'), (2, '1002')", 2);

            // Then evaluate the partition spec by adding a partition column `c`, and insert some data under the new partition spec
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN c INTEGER WITH (partitioning = 'identity')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, '1003', 3), (4, '1004', 4), (5, '1005', 5)", 3);

            // The predicate was enforced partially by tableScan, filter on `c` could not be thoroughly pushed down, so the filterNode drop it's filter condition `a > 2`
            assertPlan(sessionWithoutFilterPushdown, "SELECT b FROM " + tableName + " WHERE a > 2 and c = 4",
                    output(exchange(project(
                            filter("c = 4",
                                    strictTableScan(tableName, identityMap("b", "c")))))));
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, '1001', NULL), (2, '1002', NULL), (3, '1003', 3), (4, '1004', 4), (5, '1005', 5)");

            // Do metadata delete on column `a`, because all partition specs contains partition column `a`
            assertUpdate("DELETE FROM " + tableName + " WHERE a IN (1, 2)", 2);

            // Only identity partition column predicates, would be enforced totally by tableScan
            assertPlan(sessionWithoutFilterPushdown, "SELECT b FROM " + tableName + " WHERE a > 2 and c = 4",
                    output(exchange(
                            strictTableScan(tableName, identityMap("b")))),
                    plan -> assertTableLayout(
                            plan,
                            tableName,
                            withColumnDomains(ImmutableMap.of(
                                    new Subfield(
                                            "a",
                                            ImmutableList.of()),
                                    Domain.create(ValueSet.ofRanges(greaterThan(INTEGER, 2L)), false),
                                    new Subfield(
                                            "c",
                                            ImmutableList.of()),
                                    singleValue(INTEGER, 4L))),
                            TRUE_CONSTANT,
                            ImmutableSet.of("a", "c")));
            assertQuery("SELECT * FROM " + tableName, "VALUES (3, '1003', 3), (4, '1004', 4), (5, '1005', 5)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testCallDistributedProcedureOnPartitionedTable()
    {
        String tableName = "partition_table_for_call_distributed_procedure";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar) with (partitioning = ARRAY['c1'])");
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (2, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(3, 'foo'), (4, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(5, 'foo'), (6, 'bar')", 2);

            assertPlan(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s')", tableName, getSession().getSchema().get()),
                    output(tableFinish(exchange(REMOTE_STREAMING, GATHER,
                            callDistributedProcedure(
                                    exchange(LOCAL, GATHER,
                                            exchange(REMOTE_STREAMING, REPARTITION,
                                                    strictTableScan(tableName, identityMap("c1", "c2")))))))));

            // Do not support the filter that couldn't be enforced totally by tableScan
            assertQueryFails(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c2 > ''bar''')", tableName, getSession().getSchema().get()),
                    "Unexpected FilterNode found in plan; probably connector was not able to handle provided WHERE expression");

            // Support the filter that could be enforced totally by tableScan
            assertPlan(getSession(), format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c1 > 3')", tableName, getSession().getSchema().get()),
                    output(tableFinish(exchange(REMOTE_STREAMING, GATHER,
                            callDistributedProcedure(
                                    exchange(LOCAL, GATHER,
                                            exchange(REMOTE_STREAMING, REPARTITION,
                                                    strictTableScan(tableName, identityMap("c1", "c2")))))))),
                    plan -> assertTableLayout(
                            plan,
                            tableName,
                            withColumnDomains(ImmutableMap.of(
                                    new Subfield(
                                            "c1",
                                            ImmutableList.of()),
                                    Domain.create(ValueSet.ofRanges(greaterThan(INTEGER, 3L)), false))),
                            TRUE_CONSTANT,
                            ImmutableSet.of("c1")));

            // Support filter conditions that are always false, which cause the underlying TableScanNode to be optimized into an empty ValuesNode
            assertPlan(getSession(), format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => '1 > 2')", tableName, getSession().getSchema().get()),
                    output(tableFinish(exchange(REMOTE_STREAMING, GATHER,
                            callDistributedProcedure(
                                    exchange(LOCAL, GATHER,
                                            exchange(REMOTE_STREAMING, REPARTITION,
                                                    values(ImmutableList.of("c1", "c2"),
                                                            ImmutableList.of()))))))));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @DataProvider(name = "timezones")
    public Object[][] timezones()
    {
        return new Object[][] {
                {"UTC", true},
                {"America/Los_Angeles", true},
                {"Asia/Shanghai", true},
                {"None", false}};
    }

    @Test(dataProvider = "timezones")
    public void testHourTransform(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);

        try {
            assertUpdate(session, "CREATE TABLE test_hour_transform (d TIMESTAMP, b BIGINT) WITH (partitioning = ARRAY['hour(d)'])");
            assertUpdate(session, "INSERT INTO test_hour_transform values" +
                    "(NULL, 0), " +
                    "(TIMESTAMP '1984-12-08 00:00:10', 1), " +
                    "(TIMESTAMP '2020-01-08 12:00:01', 2)", 3);

            assertPlan(session, "SELECT * FROM test_hour_transform WHERE d IS NOT NULL",
                    thoroughlyPushdown(strictTableScan("test_hour_transform", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_hour_transform WHERE d IS NULL",
                    thoroughlyPushdown(strictTableScan("test_hour_transform", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_hour_transform WHERE d >= DATE '2015-05-15'",
                    thoroughlyPushdown(strictTableScan("test_hour_transform", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_hour_transform WHERE d >= TIMESTAMP '2015-05-15 12:00:00'",
                    thoroughlyPushdown(strictTableScan("test_hour_transform", identityMap("d", "b"))));

            assertPlan(session, "SELECT * FROM test_hour_transform WHERE d >= TIMESTAMP '2015-05-15 12:00:00.001'",
                    notThoroughlyPushdown("d >= TIMESTAMP '2015-05-15 12:00:00.001'",
                            strictTableScan("test_hour_transform", identityMap("d", "b"))));
        }
        finally {
            assertUpdate(session, "DROP TABLE test_hour_transform");
        }
    }

    @Test(dataProvider = "timezones")
    public void testDayTransformDate(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);

        try {
            assertUpdate(session, "CREATE TABLE test_day_transform_date (d DATE, b BIGINT) WITH (partitioning = ARRAY['day(d)'])");

            String values = "VALUES " +
                    "(NULL, 101)," +
                    "(DATE '1969-01-01', 10), " +
                    "(DATE '1969-12-31', 11), " +
                    "(DATE '1970-01-01', 1), " +
                    "(DATE '1970-03-04', 2), " +
                    "(DATE '2015-01-01', 3), " +
                    "(DATE '2015-01-13', 4), " +
                    "(DATE '2015-01-13', 5), " +
                    "(DATE '2015-05-15', 6), " +
                    "(DATE '2015-05-15', 7), " +
                    "(DATE '2020-02-21', 8), " +
                    "(DATE '2020-02-21', 9)";
            assertUpdate(session, "INSERT INTO test_day_transform_date " + values, 12);
            assertQuery(session, "SELECT * FROM test_day_transform_date", values);

            String expected = "VALUES " +
                    "(NULL, 1, NULL, NULL), " +
                    "(DATE '1969-01-01', 1, DATE '1969-01-01', DATE '1969-01-01'), " +
                    "(DATE '1969-12-31', 1, DATE '1969-12-31', DATE '1969-12-31'), " +
                    "(DATE '1970-01-01', 1, DATE '1970-01-01', DATE '1970-01-01'), " +
                    "(DATE '1970-03-04', 1, DATE '1970-03-04', DATE '1970-03-04'), " +
                    "(DATE '2015-01-01', 1, DATE '2015-01-01', DATE '2015-01-01'), " +
                    "(DATE '2015-01-13', 2, DATE '2015-01-13', DATE '2015-01-13'), " +
                    "(DATE '2015-05-15', 2, DATE '2015-05-15', DATE '2015-05-15'), " +
                    "(DATE '2020-02-21', 2, DATE '2020-02-21', DATE '2020-02-21')";
            assertQuery(session, "SELECT d_day, row_count, d.min, d.max FROM \"test_day_transform_date$partitions\"",
                    expected);

            // Exercise non-pushdownable predicates
            assertQuery(session, "SELECT * FROM test_day_transform_date WHERE day_of_week(d) = 3 AND b % 7 = 3",
                    "VALUES (DATE '1969-01-01', 10)");

            assertPlan(session, "SELECT * FROM test_day_transform_date WHERE d IS NOT NULL",
                    thoroughlyPushdown(strictTableScan("test_day_transform_date", identityMap("d", "b"))));

            assertPlan(session, "SELECT * FROM test_day_transform_date WHERE d IS NULL",
                    thoroughlyPushdown(strictTableScan("test_day_transform_date", identityMap("d", "b"))));

            assertPlan(session, "SELECT * FROM test_day_transform_date WHERE d >= DATE '2015-01-13'",
                    thoroughlyPushdown(strictTableScan("test_day_transform_date", identityMap("d", "b"))));

            assertPlan(session, "SELECT * FROM test_day_transform_date WHERE CAST(d AS date) >= DATE '2015-01-13'",
                    thoroughlyPushdown(strictTableScan("test_day_transform_date", identityMap("d", "b"))));

            assertPlan(session, "SELECT * FROM test_day_transform_date WHERE d >= cast(TIMESTAMP '2015-01-13 00:00:00' as date)",
                    thoroughlyPushdown(strictTableScan("test_day_transform_date", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_day_transform_date WHERE d > cast(TIMESTAMP '2015-01-13 00:00:00.001' as date)",
                    thoroughlyPushdown(strictTableScan("test_day_transform_date", identityMap("d", "b"))));
        }
        finally {
            assertUpdate(session, "DROP TABLE test_day_transform_date");
        }
    }

    @Test(dataProvider = "timezones")
    public void testDayTransformTimestamp(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);

        try {
            assertUpdate("CREATE TABLE test_day_transform_timestamp (d TIMESTAMP, b BIGINT) WITH (partitioning = ARRAY['day(d)'])");

            String values = "VALUES " +
                    "(NULL, 101)," +
                    "(TIMESTAMP '1969-12-25 15:13:12.876', 8)," +
                    "(TIMESTAMP '1969-12-30 18:47:33.345', 9)," +
                    "(TIMESTAMP '1969-12-31 00:00:00.000', 10)," +
                    "(TIMESTAMP '1969-12-31 05:06:07.234', 11)," +
                    "(TIMESTAMP '1970-01-01 12:03:08.456', 12)," +
                    "(TIMESTAMP '2015-01-01 10:01:23.123', 1)," +
                    "(TIMESTAMP '2015-01-01 11:10:02.987', 2)," +
                    "(TIMESTAMP '2015-01-01 12:55:00.456', 3)," +
                    "(TIMESTAMP '2015-05-15 13:05:01.234', 4)," +
                    "(TIMESTAMP '2015-05-15 14:21:02.345', 5)," +
                    "(TIMESTAMP '2020-02-21 15:11:11.876', 6)," +
                    "(TIMESTAMP '2020-02-21 16:12:12.654', 7)";
            assertUpdate(session, "INSERT INTO test_day_transform_timestamp " + values, 13);
            assertQuery(session, "SELECT * FROM test_day_transform_timestamp", values);

            String expected = "VALUES " +
                    "(NULL, 1, NULL, NULL), " +
                    "(DATE '1969-12-25', 1, TIMESTAMP '1969-12-25 15:13:12.876', TIMESTAMP '1969-12-25 15:13:12.876'), " +
                    "(DATE '1969-12-30', 1, TIMESTAMP '1969-12-30 18:47:33.345', TIMESTAMP '1969-12-30 18:47:33.345'), " +
                    "(DATE '1969-12-31', 2, TIMESTAMP '1969-12-31 00:00:00.000', TIMESTAMP '1969-12-31 05:06:07.234'), " +
                    "(DATE '1970-01-01', 1, TIMESTAMP '1970-01-01 12:03:08.456', TIMESTAMP '1970-01-01 12:03:08.456'), " +
                    "(DATE '2015-01-01', 3, TIMESTAMP '2015-01-01 10:01:23.123', TIMESTAMP '2015-01-01 12:55:00.456'), " +
                    "(DATE '2015-05-15', 2, TIMESTAMP '2015-05-15 13:05:01.234', TIMESTAMP '2015-05-15 14:21:02.345'), " +
                    "(DATE '2020-02-21', 2, TIMESTAMP '2020-02-21 15:11:11.876', TIMESTAMP '2020-02-21 16:12:12.654')";

            assertQuery(session, "SELECT d_day, row_count, d.min, d.max FROM \"test_day_transform_timestamp$partitions\"", expected);

            // Exercise non-pushdownable predicates
            assertQuery(session,
                    "SELECT * FROM test_day_transform_timestamp WHERE day_of_week(d) = 3 AND b % 7 = 3",
                    "VALUES (TIMESTAMP '1969-12-31 00:00:00.000', 10)");

            assertPlan(session, "SELECT * FROM test_day_transform_timestamp WHERE d IS NOT NULL",
                    thoroughlyPushdown(strictTableScan("test_day_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_day_transform_timestamp WHERE d IS NULL",
                    thoroughlyPushdown(strictTableScan("test_day_transform_timestamp", identityMap("d", "b"))));

            assertPlan(session, "SELECT * FROM test_day_transform_timestamp WHERE d >= cast(DATE '2015-05-15' as TIMESTAMP)",
                    thoroughlyPushdown(strictTableScan("test_day_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_day_transform_timestamp WHERE d >= DATE '2015-05-15'",
                    thoroughlyPushdown(strictTableScan("test_day_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_day_transform_timestamp WHERE d >= TIMESTAMP '2015-05-15 00:00:00'",
                    thoroughlyPushdown(strictTableScan("test_day_transform_timestamp", identityMap("d", "b"))));

            assertPlan(session, "SELECT * FROM test_day_transform_timestamp WHERE d >= TIMESTAMP '2015-05-15 00:00:00.001'",
                    notThoroughlyPushdown("d >= TIMESTAMP '2015-05-15 00:00:00.001'",
                            strictTableScan("test_day_transform_timestamp", identityMap("d", "b"))));
        }
        finally {
            assertUpdate("DROP TABLE test_day_transform_timestamp");
        }
    }

    @Test(dataProvider = "timezones")
    public void testMonthTransformDate(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);

        try {
            assertUpdate("CREATE TABLE test_month_transform_date (d DATE, b BIGINT) WITH (partitioning = ARRAY['month(d)'])");

            String values = "VALUES " +
                    "(NULL, 101)," +
                    "(DATE '1969-11-13', 1)," +
                    "(DATE '1969-12-01', 2)," +
                    "(DATE '1969-12-02', 3)," +
                    "(DATE '1969-12-31', 4)," +
                    "(DATE '1970-01-01', 5), " +
                    "(DATE '1970-05-13', 6), " +
                    "(DATE '1970-12-31', 7), " +
                    "(DATE '2020-01-01', 8), " +
                    "(DATE '2020-06-16', 9), " +
                    "(DATE '2020-06-28', 10), " +
                    "(DATE '2020-06-06', 11), " +
                    "(DATE '2020-07-18', 12), " +
                    "(DATE '2020-07-28', 13), " +
                    "(DATE '2020-12-31', 14)";
            assertUpdate("INSERT INTO test_month_transform_date " + values, 15);
            assertQuery("SELECT * FROM test_month_transform_date", values);

            assertQuery(
                    "SELECT d_month, row_count, d.min, d.max FROM \"test_month_transform_date$partitions\"",
                    "VALUES " +
                            "(NULL, 1, NULL, NULL), " +
                            "(-2, 1, DATE '1969-11-13', DATE '1969-11-13'), " +
                            "(-1, 3, DATE '1969-12-01', DATE '1969-12-31'), " +
                            "(0, 1, DATE '1970-01-01', DATE '1970-01-01'), " +
                            "(4, 1, DATE '1970-05-13', DATE '1970-05-13'), " +
                            "(11, 1, DATE '1970-12-31', DATE '1970-12-31'), " +
                            "(600, 1, DATE '2020-01-01', DATE '2020-01-01'), " +
                            "(605, 3, DATE '2020-06-06', DATE '2020-06-28'), " +
                            "(606, 2, DATE '2020-07-18', DATE '2020-07-28'), " +
                            "(611, 1, DATE '2020-12-31', DATE '2020-12-31')");

            // Exercise non-pushdownable predicates
            assertQuery(
                    "SELECT * FROM test_month_transform_date WHERE day_of_week(d) = 7 AND b % 7 = 3",
                    "VALUES (DATE '2020-06-28', 10)");

            assertPlan(session, "SELECT * FROM test_month_transform_date WHERE d IS NOT NULL",
                    thoroughlyPushdown(strictTableScan("test_month_transform_date", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_month_transform_date WHERE d IS NULL",
                    thoroughlyPushdown(strictTableScan("test_month_transform_date", identityMap("d", "b"))));

            assertPlan(session, "SELECT * FROM test_month_transform_date WHERE d >= DATE '2020-06-01'",
                    thoroughlyPushdown(strictTableScan("test_month_transform_date", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_month_transform_date WHERE d >= DATE '2020-06-02'",
                    notThoroughlyPushdown("d >= DATE '2020-06-02'",
                            strictTableScan("test_month_transform_date", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_month_transform_date WHERE CAST(d AS date) >= DATE '2020-06-01'",
                    thoroughlyPushdown(strictTableScan("test_month_transform_date", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_month_transform_date WHERE CAST(d AS date) >= DATE '2020-06-02'",
                    notThoroughlyPushdown("d >= DATE '2020-06-02'",
                            strictTableScan("test_month_transform_date", identityMap("d", "b"))));

            assertPlan(session, "SELECT * FROM test_month_transform_date WHERE d >= cast(TIMESTAMP '2015-06-01 00:00:00' as date)",
                    thoroughlyPushdown(strictTableScan("test_month_transform_date", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_month_transform_date WHERE d > cast(TIMESTAMP '2015-05-01 00:00:00.001' as date)",
                    notThoroughlyPushdown("d > DATE '2015-05-01'",
                            strictTableScan("test_month_transform_date", identityMap("d", "b"))));
        }
        finally {
            assertUpdate("DROP TABLE test_month_transform_date");
        }
    }

    @Test(dataProvider = "timezones")
    public void testMonthTransformTimestamp(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);

        try {
            assertUpdate("CREATE TABLE test_month_transform_timestamp (d TIMESTAMP, b BIGINT) WITH (partitioning = ARRAY['month(d)'])");

            String values = "VALUES " +
                    "(NULL, 101)," +
                    "(TIMESTAMP '1969-11-15 15:13:12.876', 8)," +
                    "(TIMESTAMP '1969-11-19 18:47:33.345', 9)," +
                    "(TIMESTAMP '1969-12-01 00:00:00.000', 10)," +
                    "(TIMESTAMP '1969-12-01 05:06:07.234', 11)," +
                    "(TIMESTAMP '1970-01-01 12:03:08.456', 12)," +
                    "(TIMESTAMP '2015-01-01 10:01:23.123', 1)," +
                    "(TIMESTAMP '2015-01-01 11:10:02.987', 2)," +
                    "(TIMESTAMP '2015-01-01 12:55:00.456', 3)," +
                    "(TIMESTAMP '2015-05-15 13:05:01.234', 4)," +
                    "(TIMESTAMP '2015-05-15 14:21:02.345', 5)," +
                    "(TIMESTAMP '2020-02-21 15:11:11.876', 6)," +
                    "(TIMESTAMP '2020-02-21 16:12:12.654', 7)";
            assertUpdate("INSERT INTO test_month_transform_timestamp " + values, 13);
            assertQuery("SELECT * FROM test_month_transform_timestamp", values);

            String expected = "VALUES " +
                    "(NULL, 1, NULL, NULL), " +
                    "(-2, 2, TIMESTAMP '1969-11-15 15:13:12.876', TIMESTAMP '1969-11-19 18:47:33.345'), " +
                    "(-1, 2, TIMESTAMP '1969-12-01 00:00:00.000', TIMESTAMP '1969-12-01 05:06:07.234'), " +
                    "(0, 1, TIMESTAMP '1970-01-01 12:03:08.456', TIMESTAMP '1970-01-01 12:03:08.456'), " +
                    "(540, 3, TIMESTAMP '2015-01-01 10:01:23.123', TIMESTAMP '2015-01-01 12:55:00.456'), " +
                    "(544, 2, TIMESTAMP '2015-05-15 13:05:01.234', TIMESTAMP '2015-05-15 14:21:02.345'), " +
                    "(601, 2, TIMESTAMP '2020-02-21 15:11:11.876', TIMESTAMP '2020-02-21 16:12:12.654')";

            assertQuery("SELECT d_month, row_count, d.min, d.max FROM \"test_month_transform_timestamp$partitions\"", expected);

            // Exercise non-pushdownable predicates
            assertQuery(
                    "SELECT * FROM test_month_transform_timestamp WHERE day_of_week(d) = 1 AND b % 7 = 3",
                    "VALUES (TIMESTAMP '1969-12-01 00:00:00.000', 10)");

            assertPlan(session, "SELECT * FROM test_month_transform_timestamp WHERE d IS NOT NULL",
                    thoroughlyPushdown(strictTableScan("test_month_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_month_transform_timestamp WHERE d IS NULL",
                    thoroughlyPushdown(strictTableScan("test_month_transform_timestamp", identityMap("d", "b"))));

            assertPlan(session, "SELECT * FROM test_month_transform_timestamp WHERE d >= DATE '2015-05-01'",
                    thoroughlyPushdown(strictTableScan("test_month_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_month_transform_timestamp WHERE d >= DATE '2015-05-02'",
                    notThoroughlyPushdown("d >= TIMESTAMP '2015-05-02 00:00:00.000'",
                            strictTableScan("test_month_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_month_transform_timestamp WHERE d >= cast(DATE '2015-05-01' as TIMESTAMP)",
                    thoroughlyPushdown(strictTableScan("test_month_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_month_transform_timestamp WHERE d >= cast(DATE '2015-05-02' as TIMESTAMP)",
                    notThoroughlyPushdown("d >= TIMESTAMP '2015-05-02 00:00:00.000'",
                            strictTableScan("test_month_transform_timestamp", identityMap("d", "b"))));

            assertPlan(session, "SELECT * FROM test_month_transform_timestamp WHERE d >= TIMESTAMP '2015-05-01 00:00:00'",
                    thoroughlyPushdown(strictTableScan("test_month_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_month_transform_timestamp WHERE d >= TIMESTAMP '2015-05-01 00:00:00.001'",
                    notThoroughlyPushdown("d >= TIMESTAMP '2015-05-01 00:00:00.001'",
                            strictTableScan("test_month_transform_timestamp", identityMap("d", "b"))));
        }
        finally {
            assertUpdate("DROP TABLE test_month_transform_timestamp");
        }
    }

    @Test(dataProvider = "timezones")
    public void testYearTransformDate(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);

        try {
            assertUpdate("CREATE TABLE test_year_transform_date (d DATE, b BIGINT) WITH (partitioning = ARRAY['year(d)'])");

            String values = "VALUES " +
                    "(NULL, 101)," +
                    "(DATE '1968-10-13', 1), " +
                    "(DATE '1969-01-01', 2), " +
                    "(DATE '1969-03-15', 3), " +
                    "(DATE '1970-01-01', 4), " +
                    "(DATE '1970-03-05', 5), " +
                    "(DATE '2015-01-01', 6), " +
                    "(DATE '2015-06-16', 7), " +
                    "(DATE '2015-07-28', 8), " +
                    "(DATE '2016-05-15', 9), " +
                    "(DATE '2016-06-06', 10), " +
                    "(DATE '2020-02-21', 11), " +
                    "(DATE '2020-11-10', 12)";
            assertUpdate("INSERT INTO test_year_transform_date " + values, 13);
            assertQuery("SELECT * FROM test_year_transform_date", values);

            assertQuery(
                    "SELECT d_year, row_count, d.min, d.max FROM \"test_year_transform_date$partitions\"",
                    "VALUES " +
                            "(NULL, 1, NULL, NULL), " +
                            "(-2, 1, DATE '1968-10-13', DATE '1968-10-13'), " +
                            "(-1, 2, DATE '1969-01-01', DATE '1969-03-15'), " +
                            "(0, 2, DATE '1970-01-01', DATE '1970-03-05'), " +
                            "(45, 3, DATE '2015-01-01', DATE '2015-07-28'), " +
                            "(46, 2, DATE '2016-05-15', DATE '2016-06-06'), " +
                            "(50, 2, DATE '2020-02-21', DATE '2020-11-10')");

            // Exercise non-pushdownable predicates
            assertQuery(
                    "SELECT * FROM test_year_transform_date WHERE day_of_week(d) = 1 AND b % 7 = 3",
                    "VALUES (DATE '2016-06-06', 10)");

            assertPlan(session, "SELECT * FROM test_year_transform_date WHERE d IS NOT NULL",
                    thoroughlyPushdown(strictTableScan("test_year_transform_date", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_year_transform_date WHERE d IS NULL",
                    thoroughlyPushdown(strictTableScan("test_year_transform_date", identityMap("d", "b"))));

            assertPlan(session, "SELECT * FROM test_year_transform_date WHERE d >= DATE '2015-01-01'",
                    thoroughlyPushdown(strictTableScan("test_year_transform_date", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_year_transform_date WHERE d >= DATE '2015-01-02'",
                    notThoroughlyPushdown("d >= DATE '2015-01-02'",
                            strictTableScan("test_year_transform_date", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_year_transform_date WHERE CAST(d AS date) >= DATE '2015-01-01'",
                    thoroughlyPushdown(strictTableScan("test_year_transform_date", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_year_transform_date WHERE CAST(d AS date) >= DATE '2015-01-02'",
                    notThoroughlyPushdown("d >= DATE '2015-01-02'",
                            strictTableScan("test_year_transform_date", identityMap("d", "b"))));

            assertPlan(session, "SELECT * FROM test_year_transform_date WHERE d >= cast(TIMESTAMP '2015-01-01 00:00:00' as date)",
                    thoroughlyPushdown(strictTableScan("test_year_transform_date", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_year_transform_date WHERE d > cast(TIMESTAMP '2015-01-01 00:00:00.000001' as date)",
                    notThoroughlyPushdown("d > DATE '2015-01-01'",
                            strictTableScan("test_year_transform_date", identityMap("d", "b"))));
        }
        finally {
            assertUpdate("DROP TABLE test_year_transform_date");
        }
    }

    @Test(dataProvider = "timezones")
    public void testYearTransformTimestamp(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);

        try {
            assertUpdate("CREATE TABLE test_year_transform_timestamp (d TIMESTAMP, b BIGINT) WITH (partitioning = ARRAY['year(d)'])");

            String values = "VALUES " +
                    "(NULL, 101)," +
                    "(TIMESTAMP '1968-03-15 15:13:12.876', 1)," +
                    "(TIMESTAMP '1968-11-19 18:47:33.345', 2)," +
                    "(TIMESTAMP '1969-01-01 00:00:00.000', 3)," +
                    "(TIMESTAMP '1969-01-01 05:06:07.234', 4)," +
                    "(TIMESTAMP '1970-01-18 12:03:08.456', 5)," +
                    "(TIMESTAMP '1970-03-14 10:01:23.123', 6)," +
                    "(TIMESTAMP '1970-08-19 11:10:02.987', 7)," +
                    "(TIMESTAMP '1970-12-31 12:55:00.456', 8)," +
                    "(TIMESTAMP '2015-05-15 13:05:01.234', 9)," +
                    "(TIMESTAMP '2015-09-15 14:21:02.345', 10)," +
                    "(TIMESTAMP '2020-02-21 15:11:11.876', 11)," +
                    "(TIMESTAMP '2020-08-21 16:12:12.654', 12)";
            assertUpdate("INSERT INTO test_year_transform_timestamp " + values, 13);
            assertQuery("SELECT * FROM test_year_transform_timestamp", values);

            String expected = "VALUES " +
                    "(NULL, 1, NULL, NULL), " +
                    "(-2, 2, TIMESTAMP '1968-03-15 15:13:12.876', TIMESTAMP '1968-11-19 18:47:33.345'), " +
                    "(-1, 2, TIMESTAMP '1969-01-01 00:00:00.000', TIMESTAMP '1969-01-01 05:06:07.234'), " +
                    "(0, 4, TIMESTAMP '1970-01-18 12:03:08.456', TIMESTAMP '1970-12-31 12:55:00.456'), " +
                    "(45, 2, TIMESTAMP '2015-05-15 13:05:01.234', TIMESTAMP '2015-09-15 14:21:02.345'), " +
                    "(50, 2, TIMESTAMP '2020-02-21 15:11:11.876', TIMESTAMP '2020-08-21 16:12:12.654')";
            assertQuery("SELECT d_year, row_count, d.min, d.max FROM \"test_year_transform_timestamp$partitions\"", expected);

            // Exercise non-pushdownable predicates
            assertQuery(
                    "SELECT * FROM test_year_transform_timestamp WHERE day_of_week(d) = 2 AND b % 7 = 3",
                    "VALUES (TIMESTAMP '2015-09-15 14:21:02.345', 10)");

            assertPlan(session, "SELECT * FROM test_year_transform_timestamp WHERE d IS NOT NULL",
                    thoroughlyPushdown(strictTableScan("test_year_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_year_transform_timestamp WHERE d IS NULL",
                    thoroughlyPushdown(strictTableScan("test_year_transform_timestamp", identityMap("d", "b"))));

            assertPlan(session, "SELECT * FROM test_year_transform_timestamp WHERE d >= DATE '2015-01-01'",
                    thoroughlyPushdown(strictTableScan("test_year_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_year_transform_timestamp WHERE d >= DATE '2015-01-02'",
                    notThoroughlyPushdown("d >= TIMESTAMP '2015-01-02 00:00:00.000'",
                            strictTableScan("test_year_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_year_transform_timestamp WHERE d >= CAST(DATE '2015-01-01' as TIMESTAMP)",
                    thoroughlyPushdown(strictTableScan("test_year_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_year_transform_timestamp WHERE d >= CAST(DATE '2015-01-02' as TIMESTAMP)",
                    notThoroughlyPushdown("d >= TIMESTAMP '2015-01-02 00:00:00.000'",
                            strictTableScan("test_year_transform_timestamp", identityMap("d", "b"))));

            assertPlan(session, "SELECT * FROM test_year_transform_timestamp WHERE d >= TIMESTAMP '2015-01-01 00:00:00'",
                    thoroughlyPushdown(strictTableScan("test_year_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_year_transform_timestamp WHERE d >= TIMESTAMP '2015-01-01 00:00:00.001'",
                    notThoroughlyPushdown("d >= TIMESTAMP '2015-01-01 00:00:00.001'",
                            strictTableScan("test_year_transform_timestamp", identityMap("d", "b"))));
        }
        finally {
            assertUpdate("DROP TABLE test_year_transform_timestamp");
        }
    }

    @Test
    public void testTruncateTextTransform()
    {
        try {
            assertUpdate("CREATE TABLE test_truncate_text_transform (d VARCHAR, b BIGINT) WITH (partitioning = ARRAY['truncate(d, 2)'])");
            String select = "SELECT d_trunc, row_count, d.min, d.max FROM \"test_truncate_text_transform$partitions\"";

            assertUpdate("INSERT INTO test_truncate_text_transform VALUES" +
                    "(NULL, 101)," +
                    "('abcd', 1)," +
                    "('abxy', 2)," +
                    "('ab598', 3)," +
                    "('Kielce', 4)," +
                    "('Kiev', 5)," +
                    "('Greece', 6)," +
                    "('Grozny', 7)", 8);

            assertQuery("SELECT d_trunc FROM \"test_truncate_text_transform$partitions\"", "VALUES NULL, 'ab', 'Ki', 'Gr'");

            assertQuery("SELECT b FROM test_truncate_text_transform WHERE substr(d, 1, 2) = 'ab'", "VALUES 1, 2, 3");
            assertQuery(select + " WHERE d_trunc = 'ab'",
                    "VALUES ('ab', 3, 'ab598', 'abxy')");

            assertQuery("SELECT b FROM test_truncate_text_transform WHERE substr(d, 1, 2) = 'Ki'", "VALUES 4, 5");
            assertQuery(select + " WHERE d_trunc = 'Ki'",
                    "VALUES ('Ki', 2, 'Kielce', 'Kiev')");

            assertQuery("SELECT b FROM test_truncate_text_transform WHERE substr(d, 1, 2) = 'Gr'", "VALUES 6, 7");
            assertQuery(select + " WHERE d_trunc = 'Gr'",
                    "VALUES ('Gr', 2, 'Greece', 'Grozny')");

            // Exercise non-pushdownable predicates
            assertQuery(
                    "SELECT * FROM test_truncate_text_transform WHERE length(d) = 4 AND b % 7 = 2",
                    "VALUES ('abxy', 2)");

            assertPlan("SELECT * FROM test_truncate_text_transform WHERE d IS NOT NULL",
                    thoroughlyPushdown(strictTableScan("test_truncate_text_transform", identityMap("d", "b"))));
            assertPlan("SELECT * FROM test_truncate_text_transform WHERE d IS NULL",
                    thoroughlyPushdown(strictTableScan("test_truncate_text_transform", identityMap("d", "b"))));

            // TODO: subsume partition boundary filters on varchar
            assertPlan("SELECT * FROM test_truncate_text_transform WHERE d >= 'ab'",
                    notThoroughlyPushdown("d >= 'ab'",
                            strictTableScan("test_truncate_text_transform", identityMap("d", "b"))));

            // TODO: subsume prefix-checking LIKE with truncate().
            assertPlan("SELECT * FROM test_truncate_text_transform WHERE d LIKE 'ab%'",
                    notThoroughlyPushdown("substr(d, 1, 2) = 'ab'",
                            strictTableScan("test_truncate_text_transform", identityMap("d", "b"))));

            // condition to long to subsume, we use truncate(2)
            assertPlan("SELECT * FROM test_truncate_text_transform WHERE d >= 'abc'",
                    notThoroughlyPushdown("d >= 'abc'",
                            strictTableScan("test_truncate_text_transform", identityMap("d", "b"))));

            // condition to long to subsume, we use truncate(2)
            assertPlan("SELECT * FROM test_truncate_text_transform WHERE d LIKE 'abc%'",
                    notThoroughlyPushdown("substr(d, 1, 3) = 'abc'",
                            strictTableScan("test_truncate_text_transform", identityMap("d", "b"))));
        }
        finally {
            assertUpdate("DROP TABLE test_truncate_text_transform");
        }
    }

    @Test
    public void testTruncateIntegerTransform()
    {
        testTruncateIntegerTransform("integer");
        testTruncateIntegerTransform("bigint");
    }

    private void testTruncateIntegerTransform(String dataType)
    {
        String table = format("test_truncate_%s_transform", dataType);
        try {
            assertUpdate(format("CREATE TABLE " + table + " (d %s, b BIGINT) WITH (partitioning = ARRAY['truncate(d, 10)'])", dataType));
            String select = "SELECT d_trunc, row_count, d.min, d.max FROM \"" + table + "$partitions\"";

            assertUpdate("INSERT INTO " + table + " VALUES" +
                    "(NULL, 101)," +
                    "(0, 1)," +
                    "(1, 2)," +
                    "(5, 3)," +
                    "(9, 4)," +
                    "(10, 5)," +
                    "(11, 6)," +
                    "(120, 7)," +
                    "(121, 8)," +
                    "(123, 9)," +
                    "(-1, 10)," +
                    "(-5, 11)," +
                    "(-10, 12)," +
                    "(-11, 13)," +
                    "(-123, 14)," +
                    "(-130, 15)", 16);

            assertQuery("SELECT d_trunc FROM \"" + table + "$partitions\"", "VALUES NULL, 0, 10, 120, -10, -20, -130");

            assertQuery("SELECT b FROM " + table + " WHERE d IN (0, 1, 5, 9)", "VALUES 1, 2, 3, 4");
            assertQuery(select + " WHERE d_trunc = 0",
                    "VALUES (0, 4, 0, 9)");

            assertQuery("SELECT b FROM " + table + " WHERE d IN (10, 11)", "VALUES 5, 6");
            assertQuery(select + " WHERE d_trunc = 10",
                    "VALUES (10, 2, 10, 11)");

            assertQuery("SELECT b FROM " + table + " WHERE d IN (120, 121, 123)", "VALUES 7, 8, 9");
            assertQuery(select + " WHERE d_trunc = 120",
                    "VALUES (120, 3, 120, 123)");

            assertQuery("SELECT b FROM " + table + " WHERE d IN (-1, -5, -10)", "VALUES 10, 11, 12");
            assertQuery(select + " WHERE d_trunc = -10",
                    "VALUES (-10, 3, -10, -1)");

            assertQuery("SELECT b FROM " + table + " WHERE d = -11", "VALUES 13");
            assertQuery(select + " WHERE d_trunc = -20",
                    "VALUES (-20, 1, -11, -11)");

            assertQuery("SELECT b FROM " + table + " WHERE d IN (-123, -130)", "VALUES 14, 15");
            assertQuery(select + " WHERE d_trunc = -130",
                    "VALUES (-130, 2, -130, -123)");

            // Exercise non-pushdownable predicates
            assertQuery("SELECT * FROM " + table + " WHERE d % 10 = -1 AND b % 7 = 3",
                    "VALUES (-1, 10)");

            assertPlan("SELECT * FROM " + table + " WHERE d IS NOT NULL",
                    thoroughlyPushdown(strictTableScan(table, identityMap("d", "b"))));
            assertPlan("SELECT * FROM " + table + " WHERE d IS NULL",
                    thoroughlyPushdown(strictTableScan(table, identityMap("d", "b"))));

            assertPlan("SELECT * FROM " + table + " WHERE d >= 10",
                    thoroughlyPushdown(strictTableScan(table, identityMap("d", "b"))));

            assertPlan("SELECT * FROM " + table + " WHERE d > 10",
                    notThoroughlyPushdown("d > 10",
                            strictTableScan(table, identityMap("d", "b"))));
            assertPlan("SELECT * FROM " + table + " WHERE d >= 11",
                    notThoroughlyPushdown("d >= 11",
                            strictTableScan(table, identityMap("d", "b"))));
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    public void testTruncateShortDecimalTransform()
    {
        try {
            assertUpdate("CREATE TABLE test_truncate_decimal_transform (d DECIMAL(9, 2), b BIGINT) WITH (partitioning = ARRAY['truncate(d, 10)'])");
            String select = "SELECT d_trunc, row_count, d.min, d.max FROM \"test_truncate_decimal_transform$partitions\"";

            assertUpdate("INSERT INTO test_truncate_decimal_transform VALUES" +
                    "(NULL, 101)," +
                    "(12.34, 1)," +
                    "(12.30, 2)," +
                    "(12.29, 3)," +
                    "(0.05, 4)," +
                    "(-0.05, 5)", 6);

            assertQuery("SELECT d_trunc FROM \"test_truncate_decimal_transform$partitions\"", "VALUES NULL, 12.30, 12.20, 0.00, -0.10");

            assertQuery("SELECT b FROM test_truncate_decimal_transform WHERE d IN (12.34, 12.30)", "VALUES 1, 2");
            assertQuery(select + " WHERE d_trunc = 12.30",
                    "VALUES (12.30, 2, 12.30, 12.34)");

            assertQuery("SELECT b FROM test_truncate_decimal_transform WHERE d = 12.29", "VALUES 3");
            assertQuery(select + " WHERE d_trunc = 12.20",
                    "VALUES (12.20, 1, 12.29, 12.29)");

            assertQuery("SELECT b FROM test_truncate_decimal_transform WHERE d = 0.05", "VALUES 4");
            assertQuery(select + " WHERE d_trunc = 0.00",
                    "VALUES (0.00, 1, 0.05, 0.05)");

            assertQuery("SELECT b FROM test_truncate_decimal_transform WHERE d = -0.05", "VALUES 5");
            assertQuery(select + " WHERE d_trunc = -0.10",
                    "VALUES (-0.10, 1, -0.05, -0.05)");

            // Exercise non-pushdownable predicates
            assertQuery("SELECT * FROM test_truncate_decimal_transform WHERE d * 100 % 10 = 9 AND b % 7 = 3",
                    "VALUES (12.29, 3)");

            assertPlan("SELECT * FROM test_truncate_decimal_transform WHERE d IS NOT NULL",
                    thoroughlyPushdown(strictTableScan("test_truncate_decimal_transform", identityMap("d", "b"))));
            assertPlan("SELECT * FROM test_truncate_decimal_transform WHERE d IS NULL",
                    thoroughlyPushdown(strictTableScan("test_truncate_decimal_transform", identityMap("d", "b"))));

            // TODO: subsume partition boundary filters on decimals
            assertPlan("SELECT * FROM test_truncate_decimal_transform WHERE d >= 12.20",
                    notThoroughlyPushdown(true, "d >= 12.20",
                            strictTableScan("test_truncate_decimal_transform", identityMap("d", "b"))));

            assertPlan("SELECT * FROM test_truncate_decimal_transform WHERE d > 12.20",
                    notThoroughlyPushdown(true, "d > 12.20",
                            strictTableScan("test_truncate_decimal_transform", identityMap("d", "b"))));
            assertPlan("SELECT * FROM test_truncate_decimal_transform WHERE d >= 12.21",
                    notThoroughlyPushdown(true, "d >= 12.21",
                            strictTableScan("test_truncate_decimal_transform", identityMap("d", "b"))));
        }
        finally {
            assertUpdate("DROP TABLE test_truncate_decimal_transform");
        }
    }

    @Test
    public void testTruncateLongDecimalTransform()
    {
        try {
            assertUpdate("CREATE TABLE test_truncate_long_decimal_transform (d DECIMAL(20, 2), b BIGINT) WITH (partitioning = ARRAY['truncate(d, 10)'])");
            String select = "SELECT d_trunc, row_count, d.min, d.max FROM \"test_truncate_long_decimal_transform$partitions\"";

            assertUpdate("INSERT INTO test_truncate_long_decimal_transform VALUES" +
                    "(NULL, 101)," +
                    "(12.34, 1)," +
                    "(12.30, 2)," +
                    "(11111111111111112.29, 3)," +
                    "(0.05, 4)," +
                    "(-0.05, 5)", 6);

            assertQuery("SELECT d_trunc FROM \"test_truncate_long_decimal_transform$partitions\"", "VALUES NULL, 12.30, 11111111111111112.20, 0.00, -0.10");

            assertQuery("SELECT b FROM test_truncate_long_decimal_transform WHERE d IN (12.34, 12.30)", "VALUES 1, 2");
            assertQuery(select + " WHERE d_trunc = 12.30",
                    "VALUES (12.30, 2, 12.30, 12.34)");

            assertQuery("SELECT b FROM test_truncate_long_decimal_transform WHERE d = 11111111111111112.29", "VALUES 3");
            assertQuery(select + " WHERE d_trunc = 11111111111111112.20",
                    "VALUES (11111111111111112.20, 1, 11111111111111112.29, 11111111111111112.29)");

            assertQuery("SELECT b FROM test_truncate_long_decimal_transform WHERE d = 0.05", "VALUES 4");
            assertQuery(select + " WHERE d_trunc = 0.00",
                    "VALUES (0.00, 1, 0.05, 0.05)");

            assertQuery("SELECT b FROM test_truncate_long_decimal_transform WHERE d = -0.05", "VALUES 5");
            assertQuery(select + " WHERE d_trunc = -0.10",
                    "VALUES (-0.10, 1, -0.05, -0.05)");

            // Exercise non-pushdownable predicates
            assertQuery("SELECT * FROM test_truncate_long_decimal_transform WHERE d * 100 % 10 = 9 AND b % 7 = 3",
                    "VALUES (11111111111111112.29, 3)");

            assertPlan("SELECT * FROM test_truncate_long_decimal_transform WHERE d IS NOT NULL",
                    thoroughlyPushdown(strictTableScan("test_truncate_long_decimal_transform", identityMap("d", "b"))));
            assertPlan("SELECT * FROM test_truncate_long_decimal_transform WHERE d IS NULL",
                    thoroughlyPushdown(strictTableScan("test_truncate_long_decimal_transform", identityMap("d", "b"))));

            // TODO: subsume partition boundary filters on decimals
            assertPlan("SELECT * FROM test_truncate_long_decimal_transform WHERE d >= 12.20",
                    notThoroughlyPushdown(true, "d >= 12.20",
                            strictTableScan("test_truncate_long_decimal_transform", identityMap("d", "b"))));

            assertPlan("SELECT * FROM test_truncate_long_decimal_transform WHERE d > 12.20",
                    notThoroughlyPushdown(true, "d > 12.20",
                            strictTableScan("test_truncate_long_decimal_transform", identityMap("d", "b"))));
            assertPlan("SELECT * FROM test_truncate_long_decimal_transform WHERE d >= 12.21",
                    notThoroughlyPushdown(true, "d >= 12.21",
                            strictTableScan("test_truncate_long_decimal_transform", identityMap("d", "b"))));
        }
        finally {
            assertUpdate("DROP TABLE test_truncate_long_decimal_transform");
        }
    }

    @Test
    public void testBucketTransform()
    {
        testBucketTransformForType("DATE", "DATE '2020-05-19'", "DATE '2020-08-19'", "DATE '2020-11-19'");
        testBucketTransformForType("VARCHAR", "CAST('abcd' AS VARCHAR)", "CAST('mommy' AS VARCHAR)", "CAST('abxy' AS VARCHAR)");
        testBucketTransformForType("INTEGER", "10", "12", "20");
        testBucketTransformForType("BIGINT", "CAST(100000000 AS BIGINT)", "CAST(200000002 AS BIGINT)", "CAST(400000001 AS BIGINT)");
    }

    private void testBucketTransformForType(
            String type,
            String value,
            String greaterValueInSameBucket,
            String valueInOtherBucket)
    {
        String tableName = format("test_bucket_transform%s", type.toLowerCase(ENGLISH));

        try {
            assertUpdate(format("CREATE TABLE %s (d %s) WITH (partitioning = ARRAY['bucket(d, 2)'])", tableName, type));
            assertUpdate(format("INSERT INTO %s VALUES (NULL), (%s), (%s), (%s)", tableName, value, greaterValueInSameBucket, valueInOtherBucket), 4);
            assertQuery(format("SELECT * FROM %s", tableName), format("VALUES (NULL), (%s), (%s), (%s)", value, greaterValueInSameBucket, valueInOtherBucket));
            assertQuery(format("SELECT * FROM %s WHERE d <= %s AND (rand() = 42 OR d != %s)", tableName, value, valueInOtherBucket),
                    "VALUES " + value);
            assertQuery(format("SELECT * FROM %s WHERE d >= %s AND (rand() = 42 OR d != %s)", tableName, greaterValueInSameBucket, valueInOtherBucket),
                    "VALUES " + greaterValueInSameBucket);

            String selectFromPartitions = format("SELECT d_bucket, row_count FROM \"%s$partitions\"", tableName);
            assertQuery(selectFromPartitions + " WHERE d_bucket = 0", format("VALUES(0, %d)", 2));
            assertQuery(selectFromPartitions + " WHERE d_bucket = 1", format("VALUES(1, %d)", 1));

            assertPlan("SELECT * FROM " + tableName + " WHERE d IS NULL",
                    thoroughlyPushdown(strictTableScan(tableName, identityMap("d"))));
            assertPlan("SELECT * FROM " + tableName + " WHERE d IS NOT NULL",
                    notThoroughlyPushdown("d IS NOT NULL",
                            strictTableScan(tableName, identityMap("d"))));

            // Bucketing transform doesn't allow comparison filter elimination
            assertPlan("SELECT * FROM " + tableName + " WHERE d >= " + value,
                    notThoroughlyPushdown("d >= " + value,
                            strictTableScan(tableName, identityMap("d"))));
            assertPlan("SELECT * FROM " + tableName + " WHERE d >= " + greaterValueInSameBucket,
                    notThoroughlyPushdown("d >= " + greaterValueInSameBucket,
                            strictTableScan(tableName, identityMap("d"))));
            assertPlan("SELECT * FROM " + tableName + " WHERE d >= " + valueInOtherBucket,
                    notThoroughlyPushdown("d >= " + valueInOtherBucket,
                            strictTableScan(tableName, identityMap("d"))));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
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
                "with (\"write.format.default\" = 'PARQUET')");
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

    // TODO: the following @Ignore test cases could work after optimizer implementing left function unwrap()
    // See https://github.com/prestodb/presto/issues/22244
    @Ignore
    @Test(dataProvider = "timezones")
    public void testDayTransformDateWithLeftFunctionUnwrap(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);

        try {
            assertUpdate(session, "CREATE TABLE test_day_transform_date (d DATE, b BIGINT) WITH (partitioning = ARRAY['day(d)'])");

            String values = "VALUES " +
                    "(NULL, 101)," +
                    "(DATE '1969-01-01', 10), " +
                    "(DATE '1969-12-31', 11), " +
                    "(DATE '1970-01-01', 1), " +
                    "(DATE '1970-03-04', 2), " +
                    "(DATE '2015-01-01', 3), " +
                    "(DATE '2015-01-13', 4), " +
                    "(DATE '2015-01-13', 5), " +
                    "(DATE '2015-05-15', 6), " +
                    "(DATE '2015-05-15', 7), " +
                    "(DATE '2020-02-21', 8), " +
                    "(DATE '2020-02-21', 9)";
            assertUpdate(session, "INSERT INTO test_day_transform_date " + values, 12);
            assertQuery(session, "SELECT * FROM test_day_transform_date", values);

            // date()
            assertPlan(session, "SELECT * FROM test_day_transform_date WHERE date(d) = DATE '2015-01-13'",
                    thoroughlyPushdown(strictTableScan("test_day_transform_date", identityMap("d", "b"))));

            // year()
            assertPlan(session, "SELECT * FROM test_day_transform_date WHERE year(d) = 2015",
                    thoroughlyPushdown(strictTableScan("test_day_transform_date", identityMap("d", "b"))));

            // date_trunc
            assertPlan(session, "SELECT * FROM test_day_transform_date WHERE date_trunc('day', d) = DATE '2015-01-13'",
                    thoroughlyPushdown(strictTableScan("test_day_transform_date", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_day_transform_date WHERE date_trunc('month', d) = DATE '2015-01-01'",
                    thoroughlyPushdown(strictTableScan("test_day_transform_date", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_day_transform_date WHERE date_trunc('year', d) = DATE '2015-01-01'",
                    thoroughlyPushdown(strictTableScan("test_day_transform_date", identityMap("d", "b"))));
        }
        finally {
            assertUpdate(session, "DROP TABLE test_day_transform_date");
        }
    }

    @Ignore
    @Test(dataProvider = "timezones")
    public void testDayTransformTimestampWithLeftFunctionUnwrap(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);

        try {
            assertUpdate("CREATE TABLE test_day_transform_timestamp (d TIMESTAMP, b BIGINT) WITH (partitioning = ARRAY['day(d)'])");

            String values = "VALUES " +
                    "(NULL, 101)," +
                    "(TIMESTAMP '1969-12-25 15:13:12.876', 8)," +
                    "(TIMESTAMP '1969-12-30 18:47:33.345', 9)," +
                    "(TIMESTAMP '1969-12-31 00:00:00.000', 10)," +
                    "(TIMESTAMP '1969-12-31 05:06:07.234', 11)," +
                    "(TIMESTAMP '1970-01-01 12:03:08.456', 12)," +
                    "(TIMESTAMP '2015-01-01 10:01:23.123', 1)," +
                    "(TIMESTAMP '2015-01-01 11:10:02.987', 2)," +
                    "(TIMESTAMP '2015-01-01 12:55:00.456', 3)," +
                    "(TIMESTAMP '2015-05-15 13:05:01.234', 4)," +
                    "(TIMESTAMP '2015-05-15 14:21:02.345', 5)," +
                    "(TIMESTAMP '2020-02-21 15:11:11.876', 6)," +
                    "(TIMESTAMP '2020-02-21 16:12:12.654', 7)";
            assertUpdate(session, "INSERT INTO test_day_transform_timestamp " + values, 13);
            assertQuery(session, "SELECT * FROM test_day_transform_timestamp", values);

            // date()
            assertPlan(session, "SELECT * FROM test_day_transform_timestamp WHERE date(d) = DATE '2015-05-15'",
                    thoroughlyPushdown(strictTableScan("test_day_transform_timestamp", identityMap("d", "b"))));

            // year()
            assertPlan(session, "SELECT * FROM test_day_transform_timestamp WHERE year(d) = 2015",
                    thoroughlyPushdown(strictTableScan("test_day_transform_timestamp", identityMap("d", "b"))));

            // date_trunc
            assertPlan(session, "SELECT * FROM test_day_transform_timestamp WHERE date_trunc('day', d) = DATE '2015-05-15'",
                    thoroughlyPushdown(strictTableScan("test_day_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_day_transform_timestamp WHERE date_trunc('month', d) = DATE '2015-05-01'",
                    thoroughlyPushdown(strictTableScan("test_day_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_day_transform_timestamp WHERE date_trunc('year', d) = DATE '2015-01-01'",
                    thoroughlyPushdown(strictTableScan("test_day_transform_timestamp", identityMap("d", "b"))));
        }
        finally {
            assertUpdate("DROP TABLE test_day_transform_timestamp");
        }
    }

    @Ignore
    @Test(dataProvider = "timezones")
    public void testMonthTransformDateWithLeftFunctionUnwrap(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);

        try {
            assertUpdate("CREATE TABLE test_month_transform_date (d DATE, b BIGINT) WITH (partitioning = ARRAY['month(d)'])");

            String values = "VALUES " +
                    "(NULL, 101)," +
                    "(DATE '1969-11-13', 1)," +
                    "(DATE '1969-12-01', 2)," +
                    "(DATE '1969-12-02', 3)," +
                    "(DATE '1969-12-31', 4)," +
                    "(DATE '1970-01-01', 5), " +
                    "(DATE '1970-05-13', 6), " +
                    "(DATE '1970-12-31', 7), " +
                    "(DATE '2020-01-01', 8), " +
                    "(DATE '2020-06-16', 9), " +
                    "(DATE '2020-06-28', 10), " +
                    "(DATE '2020-06-06', 11), " +
                    "(DATE '2020-07-18', 12), " +
                    "(DATE '2020-07-28', 13), " +
                    "(DATE '2020-12-31', 14)";
            assertUpdate("INSERT INTO test_month_transform_date " + values, 15);
            assertQuery("SELECT * FROM test_month_transform_date", values);

            // year()
            assertPlan(session, "SELECT * FROM test_month_transform_date WHERE year(d) = 2015",
                    thoroughlyPushdown(strictTableScan("test_month_transform_date", identityMap("d", "b"))));

            // date_trunc
            assertPlan(session, "SELECT * FROM test_month_transform_date WHERE date_trunc('month', d) = DATE '2015-01-01'",
                    thoroughlyPushdown(strictTableScan("test_month_transform_date", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_month_transform_date WHERE date_trunc('year', d) = DATE '2015-01-01'",
                    thoroughlyPushdown(strictTableScan("test_month_transform_date", identityMap("d", "b"))));
        }
        finally {
            assertUpdate("DROP TABLE test_month_transform_date");
        }
    }

    @Ignore
    @Test(dataProvider = "timezones")
    public void testMonthTransformTimestampWithLeftFunctionUnwrap(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);

        try {
            assertUpdate("CREATE TABLE test_month_transform_timestamp (d TIMESTAMP, b BIGINT) WITH (partitioning = ARRAY['month(d)'])");

            String values = "VALUES " +
                    "(NULL, 101)," +
                    "(TIMESTAMP '1969-11-15 15:13:12.876', 8)," +
                    "(TIMESTAMP '1969-11-19 18:47:33.345', 9)," +
                    "(TIMESTAMP '1969-12-01 00:00:00.000', 10)," +
                    "(TIMESTAMP '1969-12-01 05:06:07.234', 11)," +
                    "(TIMESTAMP '1970-01-01 12:03:08.456', 12)," +
                    "(TIMESTAMP '2015-01-01 10:01:23.123', 1)," +
                    "(TIMESTAMP '2015-01-01 11:10:02.987', 2)," +
                    "(TIMESTAMP '2015-01-01 12:55:00.456', 3)," +
                    "(TIMESTAMP '2015-05-15 13:05:01.234', 4)," +
                    "(TIMESTAMP '2015-05-15 14:21:02.345', 5)," +
                    "(TIMESTAMP '2020-02-21 15:11:11.876', 6)," +
                    "(TIMESTAMP '2020-02-21 16:12:12.654', 7)";
            assertUpdate("INSERT INTO test_month_transform_timestamp " + values, 13);
            assertQuery("SELECT * FROM test_month_transform_timestamp", values);

            // year()
            assertPlan(session, "SELECT * FROM test_month_transform_timestamp WHERE year(d) = 2015",
                    thoroughlyPushdown(strictTableScan("test_month_transform_timestamp", identityMap("d", "b"))));

            // date_trunc
            assertPlan(session, "SELECT * FROM test_month_transform_timestamp WHERE date_trunc('month', d) = DATE '2015-05-01'",
                    thoroughlyPushdown(strictTableScan("test_month_transform_timestamp", identityMap("d", "b"))));
            assertPlan(session, "SELECT * FROM test_month_transform_timestamp WHERE date_trunc('year', d) = DATE '2015-01-01'",
                    thoroughlyPushdown(strictTableScan("test_month_transform_timestamp", identityMap("d", "b"))));
        }
        finally {
            assertUpdate("DROP TABLE test_month_transform_timestamp");
        }
    }

    @Ignore
    @Test(dataProvider = "timezones")
    public void testYearTransformDateWithLeftFunctionUnwrap(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);

        try {
            assertUpdate("CREATE TABLE test_year_transform_date (d DATE, b BIGINT) WITH (partitioning = ARRAY['year(d)'])");

            String values = "VALUES " +
                    "(NULL, 101)," +
                    "(DATE '1968-10-13', 1), " +
                    "(DATE '1969-01-01', 2), " +
                    "(DATE '1969-03-15', 3), " +
                    "(DATE '1970-01-01', 4), " +
                    "(DATE '1970-03-05', 5), " +
                    "(DATE '2015-01-01', 6), " +
                    "(DATE '2015-06-16', 7), " +
                    "(DATE '2015-07-28', 8), " +
                    "(DATE '2016-05-15', 9), " +
                    "(DATE '2016-06-06', 10), " +
                    "(DATE '2020-02-21', 11), " +
                    "(DATE '2020-11-10', 12)";
            assertUpdate("INSERT INTO test_year_transform_date " + values, 13);
            assertQuery("SELECT * FROM test_year_transform_date", values);

            // year()
            assertPlan(session, "SELECT * FROM test_year_transform_date WHERE year(d) = 2015",
                    thoroughlyPushdown(strictTableScan("test_year_transform_date", identityMap("d", "b"))));

            // date_trunc
            assertPlan(session, "SELECT * FROM test_year_transform_date WHERE date_trunc('year', d) = DATE '2015-01-01'",
                    thoroughlyPushdown(strictTableScan("test_year_transform_date", identityMap("d", "b"))));
        }
        finally {
            assertUpdate("DROP TABLE test_year_transform_date");
        }
    }

    @Ignore
    @Test(dataProvider = "timezones")
    public void testYearTransformTimestampWithLeftFunctionUnwrap(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);
        try {
            assertUpdate("CREATE TABLE test_year_transform_timestamp (d TIMESTAMP, b BIGINT) WITH (partitioning = ARRAY['year(d)'])");

            String values = "VALUES " +
                    "(NULL, 101)," +
                    "(TIMESTAMP '1968-03-15 15:13:12.876', 1)," +
                    "(TIMESTAMP '1968-11-19 18:47:33.345', 2)," +
                    "(TIMESTAMP '1969-01-01 00:00:00.000', 3)," +
                    "(TIMESTAMP '1969-01-01 05:06:07.234', 4)," +
                    "(TIMESTAMP '1970-01-18 12:03:08.456', 5)," +
                    "(TIMESTAMP '1970-03-14 10:01:23.123', 6)," +
                    "(TIMESTAMP '1970-08-19 11:10:02.987', 7)," +
                    "(TIMESTAMP '1970-12-31 12:55:00.456', 8)," +
                    "(TIMESTAMP '2015-05-15 13:05:01.234', 9)," +
                    "(TIMESTAMP '2015-09-15 14:21:02.345', 10)," +
                    "(TIMESTAMP '2020-02-21 15:11:11.876', 11)," +
                    "(TIMESTAMP '2020-08-21 16:12:12.654', 12)";
            assertUpdate("INSERT INTO test_year_transform_timestamp " + values, 13);
            assertQuery("SELECT * FROM test_year_transform_timestamp", values);

            // year()
            assertPlan(session, "SELECT * FROM test_year_transform_timestamp WHERE year(d) = 2015",
                    thoroughlyPushdown(strictTableScan("test_year_transform_timestamp", identityMap("d", "b"))));

            // date_trunc
            assertPlan(session, "SELECT * FROM test_year_transform_timestamp WHERE date_trunc('year', d) = DATE '2015-01-01'",
                    thoroughlyPushdown(strictTableScan("test_year_transform_timestamp", identityMap("d", "b"))));
        }
        finally {
            assertUpdate("DROP TABLE test_year_transform_timestamp");
        }
    }

    private static PlanMatchPattern thoroughlyPushdown(PlanMatchPattern source)
    {
        return anyTree(anyNot(FilterNode.class, source));
    }

    private static PlanMatchPattern notThoroughlyPushdown(String expectedPredicate, PlanMatchPattern source)
    {
        return notThoroughlyPushdown(false, expectedPredicate, source);
    }

    private static PlanMatchPattern notThoroughlyPushdown(boolean isDecimalFilter, String expectedPredicate, PlanMatchPattern source)
    {
        if (isDecimalFilter) {
            return anyTree(filterWithDecimal(expectedPredicate, source));
        }
        else {
            return anyTree(filter(expectedPredicate, source));
        }
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

    private Session sessionForTimezone(String zoneId, boolean legacyTimestamp)
    {
        SessionBuilder sessionBuilder = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, String.valueOf(legacyTimestamp));
        if (legacyTimestamp) {
            sessionBuilder.setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zoneId));
        }
        return sessionBuilder.build();
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
