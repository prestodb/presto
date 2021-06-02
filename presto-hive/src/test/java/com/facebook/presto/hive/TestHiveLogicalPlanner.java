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

import com.facebook.presto.Session;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.ArrayType;
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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_METADATA_QUERIES;
import static com.facebook.presto.SystemSessionProperties.PUSHDOWN_DEREFERENCE_ENABLED;
import static com.facebook.presto.SystemSessionProperties.QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.predicate.Domain.create;
import static com.facebook.presto.common.predicate.Domain.multipleValues;
import static com.facebook.presto.common.predicate.Domain.notNull;
import static com.facebook.presto.common.predicate.Domain.singleValue;
import static com.facebook.presto.common.predicate.Range.greaterThan;
import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.common.predicate.ValueSet.ofRanges;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.HiveColumnHandle.isPushedDownSubfield;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.COLLECT_COLUMN_STATISTICS_ON_WRITE;
import static com.facebook.presto.hive.HiveSessionProperties.PARQUET_DEREFERENCE_PUSHDOWN_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.PARTIAL_AGGREGATION_PUSHDOWN_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.PARTIAL_AGGREGATION_PUSHDOWN_FOR_VARIABLE_LENGTH_DATATYPES_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.RANGE_FILTERS_ON_SUBSCRIPTS_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.SHUFFLE_PARTITIONED_COLUMNS_FOR_TABLE_WRITE;
import static com.facebook.presto.hive.TestHiveIntegrationSmokeTest.assertRemoteExchangesCount;
import static com.facebook.presto.parquet.ParquetTypeUtils.pushdownColumnNameForSubfield;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anySymbol;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveLogicalPlanner
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS, LINE_ITEM, CUSTOMER, NATION),
                ImmutableMap.of("experimental.pushdown-subfields-enabled", "true"),
                Optional.empty());
    }

    @Test
    public void testRepeatedFilterPushdown()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE orders_partitioned WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, '2019-11-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-11-02' as ds FROM orders WHERE orderkey < 1000");

            queryRunner.execute("CREATE TABLE lineitem_unpartitioned AS " +
                    "SELECT orderkey, linenumber, shipmode, '2019-11-01' as ds FROM lineitem WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, linenumber, shipmode, '2019-11-02' as ds FROM lineitem WHERE orderkey < 1000 ");

            TupleDomain<String> ordersDomain = withColumnDomains(ImmutableMap.of(
                    "orderpriority", singleValue(createVarcharType(15), utf8Slice("1-URGENT"))));

            TupleDomain<String> lineitemDomain = withColumnDomains(ImmutableMap.of(
                    "shipmode", singleValue(createVarcharType(10), utf8Slice("MAIL")),
                    "ds", singleValue(createVarcharType(10), utf8Slice("2019-11-02"))));

            assertPlan(pushdownFilterEnabled(),
                    "WITH a AS (\n" +
                            "    SELECT ds, orderkey\n" +
                            "    FROM orders_partitioned\n" +
                            "    WHERE orderpriority = '1-URGENT' AND ds > '2019-11-01'\n" +
                            "),\n" +
                            "b AS (\n" +
                            "    SELECT ds, orderkey, linenumber\n" +
                            "    FROM lineitem_unpartitioned\n" +
                            "    WHERE shipmode = 'MAIL'\n" +
                            ")\n" +
                            "SELECT * FROM a LEFT JOIN b ON a.ds = b.ds",
                    anyTree(node(JoinNode.class,
                            anyTree(tableScan("orders_partitioned", ordersDomain, TRUE_CONSTANT, ImmutableSet.of("orderpriority"))),
                            anyTree(tableScan("lineitem_unpartitioned", lineitemDomain, TRUE_CONSTANT, ImmutableSet.of("shipmode", "ds"))))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS orders_partitioned");
            queryRunner.execute("DROP TABLE IF EXISTS lineitem_unpartitioned");
        }
    }

    @Test
    public void testPushdownFilter()
    {
        Session pushdownFilterEnabled = pushdownFilterEnabled();

        // Only domain predicates
        assertPlan("SELECT linenumber FROM lineitem WHERE partkey = 10",
                output(exchange(project(
                        filter("partkey = 10",
                                strictTableScan("lineitem", identityMap("linenumber", "partkey")))))));

        assertPlan(pushdownFilterEnabled, "SELECT linenumber FROM lineitem WHERE partkey = 10",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(plan, "lineitem", withColumnDomains(ImmutableMap.of(new Subfield("partkey", ImmutableList.of()), singleValue(BIGINT, 10L))), TRUE_CONSTANT, ImmutableSet.of("partkey")));

        assertPlan(pushdownFilterEnabled, "SELECT partkey, linenumber FROM lineitem WHERE partkey = 10",
                output(exchange(
                        strictTableScan("lineitem", identityMap("partkey", "linenumber")))),
                plan -> assertTableLayout(plan, "lineitem", withColumnDomains(ImmutableMap.of(new Subfield("partkey", ImmutableList.of()), singleValue(BIGINT, 10L))), TRUE_CONSTANT, ImmutableSet.of("partkey")));

        // Only remaining predicate
        assertPlan("SELECT linenumber FROM lineitem WHERE mod(orderkey, 2) = 1",
                output(exchange(project(
                        filter("mod(orderkey, 2) = 1",
                                strictTableScan("lineitem", identityMap("linenumber", "orderkey")))))));
        // Remaining predicate is NULL
        assertPlan(pushdownFilterEnabled, "SELECT linenumber FROM lineitem WHERE cardinality(NULL) > 0",
                output(values("linenumber")));

        assertPlan(pushdownFilterEnabled, "SELECT linenumber FROM lineitem WHERE orderkey > 10 AND cardinality(NULL) > 0",
                output(values("linenumber")));

        // Remaining predicate is always FALSE
        assertPlan(pushdownFilterEnabled, "SELECT linenumber FROM lineitem WHERE cardinality(ARRAY[1]) > 1",
                output(values("linenumber")));

        assertPlan(pushdownFilterEnabled, "SELECT linenumber FROM lineitem WHERE orderkey > 10 AND cardinality(ARRAY[1]) > 1",
                output(values("linenumber")));

        // TupleDomain predicate is always FALSE
        assertPlan(pushdownFilterEnabled, "SELECT linenumber FROM lineitem WHERE orderkey = 1 AND orderkey = 2",
                output(values("linenumber")));

        assertPlan(pushdownFilterEnabled, "SELECT linenumber FROM lineitem WHERE orderkey = 1 AND orderkey = 2 AND linenumber % 2 = 1",
                output(values("linenumber")));

        FunctionAndTypeManager functionAndTypeManager = getQueryRunner().getMetadata().getFunctionAndTypeManager();
        FunctionResolution functionResolution = new FunctionResolution(functionAndTypeManager);
        RowExpression remainingPredicate = new CallExpression(EQUAL.name(),
                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                BOOLEAN,
                ImmutableList.of(
                        new CallExpression("mod",
                                functionAndTypeManager.lookupFunction("mod", fromTypes(BIGINT, BIGINT)),
                                BIGINT,
                                ImmutableList.of(
                                        new VariableReferenceExpression("orderkey", BIGINT),
                                        constant(2))),
                        constant(1)));

        assertPlan(pushdownFilterEnabled, "SELECT linenumber FROM lineitem WHERE mod(orderkey, 2) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(plan, "lineitem", TupleDomain.all(), remainingPredicate, ImmutableSet.of("orderkey")));

        assertPlan(pushdownFilterEnabled, "SELECT orderkey, linenumber FROM lineitem WHERE mod(orderkey, 2) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("orderkey", "linenumber")))),
                plan -> assertTableLayout(plan, "lineitem", TupleDomain.all(), remainingPredicate, ImmutableSet.of("orderkey")));

        // A mix of domain and remaining predicates
        assertPlan("SELECT linenumber FROM lineitem WHERE partkey = 10 AND mod(orderkey, 2) = 1",
                output(exchange(project(
                        filter("partkey = 10 AND mod(orderkey, 2) = 1",
                                strictTableScan("lineitem", identityMap("linenumber", "orderkey", "partkey")))))));

        assertPlan(pushdownFilterEnabled, "SELECT linenumber FROM lineitem WHERE partkey = 10 AND mod(orderkey, 2) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(plan, "lineitem", withColumnDomains(ImmutableMap.of(new Subfield("partkey", ImmutableList.of()), singleValue(BIGINT, 10L))), remainingPredicate, ImmutableSet.of("partkey", "orderkey")));

        assertPlan(pushdownFilterEnabled, "SELECT partkey, orderkey, linenumber FROM lineitem WHERE partkey = 10 AND mod(orderkey, 2) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("partkey", "orderkey", "linenumber")))),
                plan -> assertTableLayout(plan, "lineitem", withColumnDomains(ImmutableMap.of(new Subfield("partkey", ImmutableList.of()), singleValue(BIGINT, 10L))), remainingPredicate, ImmutableSet.of("partkey", "orderkey")));
    }

    @Test
    public void testPartitionPruning()
    {
        QueryRunner queryRunner = getQueryRunner();
        queryRunner.execute("CREATE TABLE test_partition_pruning WITH (partitioned_by = ARRAY['ds']) AS " +
                "SELECT orderkey, CAST(to_iso8601(date_add('DAY', orderkey % 7, date('2019-11-01'))) AS VARCHAR) AS ds FROM orders WHERE orderkey < 1000");

        Session pushdownFilterEnabled = pushdownFilterEnabled();
        try {
            assertPlan(pushdownFilterEnabled, "SELECT * FROM test_partition_pruning WHERE ds = '2019-11-01'",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", singleValue(VARCHAR, utf8Slice("2019-11-01"))))));

            assertPlan(pushdownFilterEnabled, "SELECT * FROM test_partition_pruning WHERE date(ds) = date('2019-11-01')",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", singleValue(VARCHAR, utf8Slice("2019-11-01"))))));

            assertPlan(pushdownFilterEnabled, "SELECT * FROM test_partition_pruning WHERE date(ds) BETWEEN date('2019-11-02') AND date('2019-11-04')",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-02", "2019-11-03", "2019-11-04"))))));

            assertPlan(pushdownFilterEnabled, "SELECT * FROM test_partition_pruning WHERE ds < '2019-11-05'",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-01", "2019-11-02", "2019-11-03", "2019-11-04"))))));

            assertPlan(pushdownFilterEnabled, "SELECT * FROM test_partition_pruning WHERE date(ds) > date('2019-11-02')",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-03", "2019-11-04", "2019-11-05", "2019-11-06", "2019-11-07"))))));

            assertPlan(pushdownFilterEnabled, "SELECT * FROM test_partition_pruning WHERE ds < '2019-11-05' AND date(ds) > date('2019-11-02')",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-03", "2019-11-04"))))));
        }
        finally {
            queryRunner.execute("DROP TABLE test_partition_pruning");
        }
    }

    @Test
    public void testOptimizeMetadataQueries()
    {
        QueryRunner queryRunner = getQueryRunner();
        Session optimizeMetadataQueries = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(OPTIMIZE_METADATA_QUERIES, Boolean.toString(true))
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, Boolean.toString(true))
                .build();

        queryRunner.execute(
                "CREATE TABLE test_optimize_metadata_queries WITH (partitioned_by = ARRAY['ds']) AS " +
                        "SELECT orderkey, CAST(to_iso8601(date_add('DAY', orderkey % 7, date('2020-10-01'))) AS VARCHAR) AS ds FROM orders WHERE orderkey < 1000");
        queryRunner.execute(
                "CREATE TABLE test_optimize_metadata_queries_multiple_partition_columns WITH (partitioned_by = ARRAY['ds', 'value']) AS " +
                        "SELECT orderkey, CAST(to_iso8601(date_add('DAY', orderkey % 7, date('2020-10-01'))) AS VARCHAR) AS ds, 1 AS value FROM orders WHERE orderkey < 1000");

        try {
            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT DISTINCT ds FROM test_optimize_metadata_queries",
                    anyTree(values(
                            ImmutableList.of("ds"),
                            ImmutableList.of(
                                    ImmutableList.of(new StringLiteral("2020-10-01")),
                                    ImmutableList.of(new StringLiteral("2020-10-02")),
                                    ImmutableList.of(new StringLiteral("2020-10-03")),
                                    ImmutableList.of(new StringLiteral("2020-10-04")),
                                    ImmutableList.of(new StringLiteral("2020-10-05")),
                                    ImmutableList.of(new StringLiteral("2020-10-06")),
                                    ImmutableList.of(new StringLiteral("2020-10-07"))))));
            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT DISTINCT ds FROM test_optimize_metadata_queries WHERE ds > '2020-10-04'",
                    anyTree(values(
                            ImmutableList.of("ds"),
                            ImmutableList.of(
                                    ImmutableList.of(new StringLiteral("2020-10-05")),
                                    ImmutableList.of(new StringLiteral("2020-10-06")),
                                    ImmutableList.of(new StringLiteral("2020-10-07"))))));
            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT DISTINCT ds FROM test_optimize_metadata_queries WHERE ds = '2020-10-04' AND orderkey > 200",
                    anyTree(tableScan(
                            "test_optimize_metadata_queries",
                            withColumnDomains(ImmutableMap.of("orderkey", Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 200L)), false))),
                            TRUE_CONSTANT,
                            ImmutableSet.of("orderkey"))));
            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT DISTINCT ds FROM test_optimize_metadata_queries WHERE ds = '2020-10-04' AND orderkey > 200",
                    anyTree(tableScan(
                            "test_optimize_metadata_queries",
                            withColumnDomains(ImmutableMap.of("orderkey", Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 200L)), false))),
                            TRUE_CONSTANT,
                            ImmutableSet.of("orderkey"))));

            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT DISTINCT ds FROM test_optimize_metadata_queries_multiple_partition_columns",
                    anyTree(values(
                            ImmutableList.of("ds"),
                            ImmutableList.of(
                                    ImmutableList.of(new StringLiteral("2020-10-01")),
                                    ImmutableList.of(new StringLiteral("2020-10-02")),
                                    ImmutableList.of(new StringLiteral("2020-10-03")),
                                    ImmutableList.of(new StringLiteral("2020-10-04")),
                                    ImmutableList.of(new StringLiteral("2020-10-05")),
                                    ImmutableList.of(new StringLiteral("2020-10-06")),
                                    ImmutableList.of(new StringLiteral("2020-10-07"))))));
            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT DISTINCT ds FROM test_optimize_metadata_queries_multiple_partition_columns WHERE ds > '2020-10-04'",
                    anyTree(values(
                            ImmutableList.of("ds"),
                            ImmutableList.of(
                                    ImmutableList.of(new StringLiteral("2020-10-05")),
                                    ImmutableList.of(new StringLiteral("2020-10-06")),
                                    ImmutableList.of(new StringLiteral("2020-10-07"))))));
            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT DISTINCT ds FROM test_optimize_metadata_queries_multiple_partition_columns WHERE ds = '2020-10-04' AND orderkey > 200",
                    anyTree(tableScan(
                            "test_optimize_metadata_queries_multiple_partition_columns",
                            withColumnDomains(ImmutableMap.of("orderkey", Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 200L)), false))),
                            TRUE_CONSTANT,
                            ImmutableSet.of("orderkey"))));
            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT ds, MAX(value) FROM test_optimize_metadata_queries_multiple_partition_columns WHERE ds > '2020-10-04' GROUP BY ds",
                    anyTree(values(
                            ImmutableList.of("ds", "value"),
                            ImmutableList.of(
                                    ImmutableList.of(new StringLiteral("2020-10-05"), new LongLiteral("1")),
                                    ImmutableList.of(new StringLiteral("2020-10-06"), new LongLiteral("1")),
                                    ImmutableList.of(new StringLiteral("2020-10-07"), new LongLiteral("1"))))));
            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT MAX(ds), MAX(value) FROM test_optimize_metadata_queries_multiple_partition_columns WHERE ds > '2020-10-04'",
                    anyTree(
                            project(
                                    ImmutableMap.of(
                                            "max", expression("'2020-10-07'"),
                                            "max_2", expression("1")),
                                    any(values()))));
            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT MAX(value), MAX(ds) FROM test_optimize_metadata_queries_multiple_partition_columns WHERE ds > '2020-10-04'",
                    anyTree(
                            project(
                                    ImmutableMap.of(
                                            "max", expression("1"),
                                            "max_2", expression("'2020-10-07'")),
                                    any(values()))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_optimize_metadata_queries");
            queryRunner.execute("DROP TABLE IF EXISTS test_optimize_metadata_queries_multiple_partition_columns");
        }
    }

    @Test
    public void testMetadataAggregationFolding()
    {
        QueryRunner queryRunner = getQueryRunner();
        Session optimizeMetadataQueries = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(OPTIMIZE_METADATA_QUERIES, Boolean.toString(true))
                .build();
        Session shufflePartitionColumns = Session.builder(this.getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(HIVE_CATALOG, SHUFFLE_PARTITIONED_COLUMNS_FOR_TABLE_WRITE, Boolean.toString(true))
                .build();

        queryRunner.execute(
                shufflePartitionColumns,
                "CREATE TABLE test_metadata_aggregation_folding WITH (partitioned_by = ARRAY['ds']) AS " +
                        "SELECT orderkey, CAST(to_iso8601(date_add('DAY', orderkey % 7, date('2020-07-01'))) AS VARCHAR) AS ds FROM orders WHERE orderkey < 1000");
        queryRunner.execute(
                shufflePartitionColumns,
                "CREATE TABLE test_metadata_aggregation_folding_more_partitions WITH (partitioned_by = ARRAY['ds']) AS " +
                        "SELECT orderkey, CAST(to_iso8601(date_add('DAY', orderkey % 200, date('2020-07-01'))) AS VARCHAR) AS ds FROM orders WHERE orderkey < 1000");
        queryRunner.execute(
                shufflePartitionColumns,
                "CREATE TABLE test_metadata_aggregation_folding_null_partitions WITH (partitioned_by = ARRAY['ds']) AS " +
                        "SELECT orderkey, CAST(to_iso8601(date_add('DAY', orderkey % 7, date('2020-07-01'))) AS VARCHAR) AS ds FROM orders WHERE orderkey < 1000");
        queryRunner.execute(
                shufflePartitionColumns,
                "INSERT INTO test_metadata_aggregation_folding_null_partitions SELECT 0 as orderkey, null AS ds");

        try {
            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT * FROM test_metadata_aggregation_folding WHERE ds = (SELECT max(ds) from test_metadata_aggregation_folding)",
                    anyTree(
                            join(INNER, ImmutableList.of(),
                                    tableScan("test_metadata_aggregation_folding", getSingleValueColumnDomain("ds", "2020-07-07"), TRUE_CONSTANT, ImmutableSet.of("ds")),
                                    anyTree(any()))));
            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT * FROM test_metadata_aggregation_folding WHERE ds = (SELECT min(ds) from test_metadata_aggregation_folding)",
                    anyTree(
                            join(INNER, ImmutableList.of(),
                                    tableScan("test_metadata_aggregation_folding", getSingleValueColumnDomain("ds", "2020-07-01"), TRUE_CONSTANT, ImmutableSet.of("ds")),
                                    anyTree(any()))));

            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT * FROM test_metadata_aggregation_folding_more_partitions WHERE ds = (SELECT max(ds) from test_metadata_aggregation_folding_more_partitions)",
                    anyTree(
                            join(INNER, ImmutableList.of(),
                                    tableScan("test_metadata_aggregation_folding_more_partitions", getSingleValueColumnDomain("ds", "2021-01-16"), TRUE_CONSTANT, ImmutableSet.of("ds")),
                                    anyTree(any()))));
            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT * FROM test_metadata_aggregation_folding_more_partitions WHERE ds = (SELECT min(ds) from test_metadata_aggregation_folding_more_partitions)",
                    anyTree(
                            join(INNER, ImmutableList.of(),
                                    tableScan("test_metadata_aggregation_folding_more_partitions", getSingleValueColumnDomain("ds", "2020-07-01"), TRUE_CONSTANT, ImmutableSet.of("ds")),
                                    anyTree(any()))));

            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT * FROM test_metadata_aggregation_folding WHERE ds = (SELECT max(ds) from test_metadata_aggregation_folding_null_partitions)",
                    anyTree(
                            join(INNER, ImmutableList.of(),
                                    tableScan("test_metadata_aggregation_folding", getSingleValueColumnDomain("ds", "2020-07-07"), TRUE_CONSTANT, ImmutableSet.of("ds")),
                                    anyTree(any()))));
            assertPlan(
                    optimizeMetadataQueries,
                    "SELECT * FROM test_metadata_aggregation_folding WHERE ds = (SELECT min(ds) from test_metadata_aggregation_folding_null_partitions)",
                    anyTree(
                            join(INNER, ImmutableList.of(),
                                    tableScan("test_metadata_aggregation_folding", getSingleValueColumnDomain("ds", "2020-07-01"), TRUE_CONSTANT, ImmutableSet.of("ds")),
                                    anyTree(any()))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_metadata_aggregation_folding");
            queryRunner.execute("DROP TABLE IF EXISTS test_metadata_aggregation_folding_more_partitions");
            queryRunner.execute("DROP TABLE IF EXISTS test_metadata_aggregation_folding_null_partitions");
        }
    }

    private static TupleDomain<String> getSingleValueColumnDomain(String column, String value)
    {
        return withColumnDomains(ImmutableMap.of(column, singleValue(VARCHAR, utf8Slice(value))));
    }

    private static List<Slice> utf8Slices(String... values)
    {
        return Arrays.stream(values).map(Slices::utf8Slice).collect(toImmutableList());
    }

    private static PlanMatchPattern tableScanWithConstraint(String tableName, Map<String, Domain> expectedConstraint)
    {
        return PlanMatchPattern.tableScan(tableName).with(new Matcher() {
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
                        .transform(HiveColumnHandle.class::cast)
                        .transform(HiveColumnHandle::getName);

                if (!expectedConstraint.equals(constraint.getDomains().get())) {
                    return NO_MATCH;
                }

                return match();
            }
        });
    }

    @Test
    public void testPushdownFilterOnSubfields()
    {
        assertUpdate("CREATE TABLE test_pushdown_filter_on_subfields(" +
                         "id bigint, " +
                         "a array(bigint), " +
                         "b map(varchar, bigint), " +
                         "c row(" +
                             "a bigint, " +
                             "b row(x bigint), " +
                             "c array(bigint), " +
                             "d map(bigint, bigint), " +
                             "e map(varchar, bigint)))");

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE a[1] = 1",
                        ImmutableMap.of(new Subfield("a[1]"), singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields where a[1 + 1] = 1",
                        ImmutableMap.of(new Subfield("a[2]"), singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT *  FROM test_pushdown_filter_on_subfields WHERE b['foo'] = 1",
                        ImmutableMap.of(new Subfield("b[\"foo\"]"), singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE b[concat('f','o', 'o')] = 1",
                        ImmutableMap.of(new Subfield("b[\"foo\"]"), singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE c.a = 1",
                        ImmutableMap.of(new Subfield("c.a"), singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE c.b.x = 1",
                        ImmutableMap.of(new Subfield("c.b.x"), singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE c.c[5] = 1",
                        ImmutableMap.of(new Subfield("c.c[5]"), singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE c.d[5] = 1",
                        ImmutableMap.of(new Subfield("c.d[5]"), singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE c.e[concat('f', 'o', 'o')] = 1",
                        ImmutableMap.of(new Subfield("c.e[\"foo\"]"), singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE c.e['foo'] = 1",
                        ImmutableMap.of(new Subfield("c.e[\"foo\"]"), singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE c.a IS NOT NULL AND c.c IS NOT NULL",
                ImmutableMap.of(new Subfield("c.a"), notNull(BIGINT), new Subfield("c.c"), notNull(new ArrayType(BIGINT))));

        // TupleDomain predicate is always FALSE
        assertPlan(pushdownFilterEnabled(), "SELECT id FROM test_pushdown_filter_on_subfields WHERE c.a = 1 AND c.a = 2",
                output(values("id")));

        assertUpdate("DROP TABLE test_pushdown_filter_on_subfields");
    }

    @Test
    public void testPushdownArraySubscripts()
    {
        assertUpdate("CREATE TABLE test_pushdown_array_subscripts(id bigint, " +
                "a array(bigint), " +
                "b array(array(varchar)), " +
                "y array(row(a bigint, b varchar, c double, d row(d1 bigint, d2 double))), " +
                "z array(array(row(p bigint, e row(e1 bigint, e2 varchar)))))");

        assertPushdownSubscripts("test_pushdown_array_subscripts");

        // Unnest
        assertPushdownSubfields("SELECT t.b, a[1] FROM test_pushdown_array_subscripts CROSS JOIN UNNEST(b) as t(b)", "test_pushdown_array_subscripts",
                ImmutableMap.of("a", toSubfields("a[1]")));

        assertPushdownSubfields("SELECT t.b, a[1] FROM test_pushdown_array_subscripts CROSS JOIN UNNEST(b[1]) as t(b)", "test_pushdown_array_subscripts",
                ImmutableMap.of("a", toSubfields("a[1]"), "b", toSubfields("b[1]")));

        assertPushdownSubfields("SELECT t.b[2], a[1] FROM test_pushdown_array_subscripts CROSS JOIN UNNEST(b) as t(b)", "test_pushdown_array_subscripts",
                ImmutableMap.of("a", toSubfields("a[1]"), "b", toSubfields("b[*][2]")));

        assertPushdownSubfields("SELECT id, grouping(index), sum(length(b[1][2])) FROM test_pushdown_array_subscripts CROSS JOIN UNNEST(a) as t(index) GROUP BY grouping sets ((index, id), (index))", "test_pushdown_array_subscripts",
                ImmutableMap.of("b", toSubfields("b[1][2]")));

        assertPushdownSubfields("SELECT id, b[1] FROM test_pushdown_array_subscripts CROSS JOIN UNNEST(a) as t(unused)", "test_pushdown_array_subscripts",
                ImmutableMap.of("b", toSubfields("b[1]")));

        // No subfield pruning
        assertPushdownSubfields("SELECT array_sort(a)[1] FROM test_pushdown_array_subscripts", "test_pushdown_array_subscripts",
                ImmutableMap.of());

        assertPushdownSubfields("SELECT id FROM test_pushdown_array_subscripts CROSS JOIN UNNEST(a) as t(index) WHERE a[1] > 10 AND cardinality(b[index]) = 2", "test_pushdown_array_subscripts",
                ImmutableMap.of());

        assertUpdate("DROP TABLE test_pushdown_array_subscripts");
    }

    @Test
    public void testPushdownMapSubscripts()
    {
        assertUpdate("CREATE TABLE test_pushdown_map_subscripts(id bigint, " +
                "a map(bigint, bigint), " +
                "b map(bigint, map(bigint, varchar)), " +
                "c map(varchar, bigint), \n" +
                "y map(bigint, row(a bigint, b varchar, c double, d row(d1 bigint, d2 double)))," +
                "z map(bigint, map(bigint, row(p bigint, e row(e1 bigint, e2 varchar)))))");

        assertPushdownSubscripts("test_pushdown_map_subscripts");

        // Unnest
        assertPushdownSubfields("SELECT t.b, a[1] FROM test_pushdown_map_subscripts CROSS JOIN UNNEST(b) as t(k, b)", "test_pushdown_map_subscripts",
                ImmutableMap.of("a", toSubfields("a[1]")));

        assertPushdownSubfields("SELECT t.b, a[1] FROM test_pushdown_map_subscripts CROSS JOIN UNNEST(b[1]) as t(k, b)", "test_pushdown_map_subscripts",
                ImmutableMap.of("a", toSubfields("a[1]"), "b", toSubfields("b[1]")));

        assertPushdownSubfields("SELECT t.b[2], a[1] FROM test_pushdown_map_subscripts CROSS JOIN UNNEST(b) as t(k, b)", "test_pushdown_map_subscripts",
                ImmutableMap.of("a", toSubfields("a[1]"), "b", toSubfields("b[*][2]")));

        assertPushdownSubfields("SELECT id, b[1] FROM test_pushdown_map_subscripts CROSS JOIN UNNEST(a) as t(unused_k, unused_v)", "test_pushdown_map_subscripts",
                ImmutableMap.of("b", toSubfields("b[1]")));

        // Map with varchar keys
        assertPushdownSubfields("SELECT c['cat'] FROM test_pushdown_map_subscripts", "test_pushdown_map_subscripts",
                ImmutableMap.of("c", toSubfields("c[\"cat\"]")));

        assertPushdownSubfields("SELECT c[JSON_EXTRACT_SCALAR(JSON_PARSE('{}'),'$.a')] FROM test_pushdown_map_subscripts", "test_pushdown_map_subscripts",
                ImmutableMap.of());

        assertPushdownSubfields("SELECT mod(c['cat'], 2) FROM test_pushdown_map_subscripts WHERE c['dog'] > 10", "test_pushdown_map_subscripts",
                ImmutableMap.of("c", toSubfields("c[\"cat\"]", "c[\"dog\"]")));

        // No subfield pruning
        assertPushdownSubfields("SELECT map_keys(a)[1] FROM test_pushdown_map_subscripts", "test_pushdown_map_subscripts",
                ImmutableMap.of());

        assertUpdate("DROP TABLE test_pushdown_map_subscripts");
    }

    private void assertPushdownSubscripts(String tableName)
    {
        // Filter and project
        assertPushdownSubfields(format("SELECT a[1] FROM %s", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]")));

        assertPushdownSubfields(format("SELECT a[1] + 10 FROM %s", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]")));

        assertPushdownSubfields(format("SELECT a[1] + mod(a[2], 3) FROM %s", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]", "a[2]")));

        assertPushdownSubfields(format("SELECT a[1] FROM %s WHERE a[2] > 10", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]", "a[2]")));

        assertPushdownSubfields(format("SELECT a[1] FROM %s WHERE mod(a[2], 3) = 1", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]", "a[2]")));

        assertPushdownSubfields(format("SELECT a[1], b[2][3] FROM %s", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]"), "b", toSubfields("b[2][3]")));

        assertPushdownSubfields(format("SELECT cardinality(b[1]), b[1][2] FROM %s", tableName), tableName,
                ImmutableMap.of("b", toSubfields("b[1]")));

        assertPushdownSubfields(format("CREATE TABLE x AS SELECT id, a[1] as a1 FROM %s", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]")));

        assertPushdownSubfields(format("CREATE TABLE x AS SELECT id FROM %s WHERE a[1] > 10", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]")));

        assertPushdownSubfields(format("SELECT a[1] FROM %s ORDER BY id LIMIT 1", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]")));

        // Sort
        assertPushdownSubfields(format("SELECT a[1] FROM %s ORDER BY a[2]", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]", "a[2]")));

        // Join
        assertPlan(format("SELECT l.orderkey, a.a[1] FROM lineitem l, %s a WHERE l.linenumber = a.id", tableName),
                anyTree(
                        node(JoinNode.class,
                                anyTree(tableScan("lineitem", ImmutableMap.of())),
                                anyTree(tableScan(tableName, ImmutableMap.of("a", toSubfields("a[1]")))))));

        assertPlan(format("SELECT l.orderkey, a.a[1] FROM lineitem l, %s a WHERE l.linenumber = a.id AND a.a[2] > 10", tableName),
                anyTree(
                        node(JoinNode.class,
                                anyTree(tableScan("lineitem", ImmutableMap.of())),
                                anyTree(tableScan(tableName, ImmutableMap.of("a", toSubfields("a[1]", "a[2]")))))));

        // Semi join
        assertPlan(format("SELECT a[1] FROM %s WHERE a[2] IN (SELECT a[3] FROM %s)", tableName, tableName),
                anyTree(node(SemiJoinNode.class,
                        anyTree(tableScan(tableName, ImmutableMap.of("a", toSubfields("a[1]", "a[2]")))),
                        anyTree(tableScan(tableName, ImmutableMap.of("a", toSubfields("a[3]")))))));

        // Aggregation
        assertPushdownSubfields(format("SELECT id, min(a[1]) FROM %s GROUP BY 1", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]")));

        assertPushdownSubfields(format("SELECT id, min(a[1]) FROM %s GROUP BY 1, a[2]", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]", "a[2]")));

        assertPushdownSubfields(format("SELECT id, min(a[1]) FROM %s GROUP BY 1 HAVING max(a[2]) > 10", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]", "a[2]")));

        assertPushdownSubfields(format("SELECT id, min(mod(a[1], 3)) FROM %s GROUP BY 1", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]")));

        assertPushdownSubfields(format("SELECT id, min(a[1]) FILTER (WHERE a[2] > 10) FROM %s GROUP BY 1", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]", "a[2]")));

        assertPushdownSubfields(format("SELECT id, min(a[1] + length(b[2][3])) * avg(a[4]) FROM %s GROUP BY 1", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]", "a[4]"), "b", toSubfields("b[2][3]")));

        assertPushdownSubfields(format("SELECT min(a[1]) FROM %s GROUP BY id", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]")));

        assertPushdownSubfields(format("SELECT arbitrary(y[1]).a FROM %s GROUP BY id", tableName), tableName,
                ImmutableMap.of("y", toSubfields("y[1].a")));

        assertPushdownSubfields(format("SELECT arbitrary(y[1]).d.d1 FROM %s GROUP BY id", tableName), tableName,
                ImmutableMap.of("y", toSubfields("y[1].d.d1")));

        assertPushdownSubfields(format("SELECT arbitrary(y[2].d).d1 FROM %s GROUP BY id", tableName), tableName,
                ImmutableMap.of("y", toSubfields("y[2].d.d1")));

        assertPushdownSubfields(format("SELECT arbitrary(y[3].d.d1) FROM %s GROUP BY id", tableName), tableName,
                ImmutableMap.of("y", toSubfields("y[3].d.d1")));

        assertPushdownSubfields(format("SELECT arbitrary(z[1][2]).e.e1 FROM %s GROUP BY id", tableName), tableName,
                ImmutableMap.of("z", toSubfields("z[1][2].e.e1")));

        assertPushdownSubfields(format("SELECT arbitrary(z[2][3].e).e2 FROM %s GROUP BY id", tableName), tableName,
                ImmutableMap.of("z", toSubfields("z[2][3].e.e2")));

        // Union
        assertPlan(format("SELECT a[1] FROM %s UNION ALL SELECT a[2] FROM %s", tableName, tableName),
                anyTree(exchange(
                        anyTree(tableScan(tableName, ImmutableMap.of("a", toSubfields("a[1]")))),
                        anyTree(tableScan(tableName, ImmutableMap.of("a", toSubfields("a[2]")))))));

        assertPlan(format("SELECT a[1] FROM (SELECT * FROM %s UNION ALL SELECT * FROM %s)", tableName, tableName),
                anyTree(exchange(
                        anyTree(tableScan(tableName, ImmutableMap.of("a", toSubfields("a[1]")))),
                        anyTree(tableScan(tableName, ImmutableMap.of("a", toSubfields("a[1]")))))));

        assertPlan(format("SELECT a[1] FROM (SELECT * FROM %s WHERE a[2] > 10 UNION ALL SELECT * FROM %s)", tableName, tableName),
                anyTree(exchange(
                        anyTree(tableScan(tableName, ImmutableMap.of("a", toSubfields("a[1]", "a[2]")))),
                        anyTree(tableScan(tableName, ImmutableMap.of("a", toSubfields("a[1]")))))));

        // Except
        assertPlan(format("SELECT a[1] FROM %s EXCEPT SELECT a[2] FROM %s", tableName, tableName),
                anyTree(exchange(
                        anyTree(tableScan(tableName, ImmutableMap.of("a", toSubfields("a[1]")))),
                        anyTree(tableScan(tableName, ImmutableMap.of("a", toSubfields("a[2]")))))));

        // Intersect
        assertPlan(format("SELECT a[1] FROM %s INTERSECT SELECT a[2] FROM %s", tableName, tableName),
                anyTree(exchange(
                        anyTree(tableScan(tableName, ImmutableMap.of("a", toSubfields("a[1]")))),
                        anyTree(tableScan(tableName, ImmutableMap.of("a", toSubfields("a[2]")))))));

        // Window function
        assertPushdownSubfields(format("SELECT id, first_value(a[1]) over (partition by a[2] order by b[1][2]) FROM %s", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]", "a[2]"), "b", toSubfields("b[1][2]")));

        assertPushdownSubfields(format("SELECT count(*) over (partition by a[1] order by a[2] rows between a[3] preceding and a[4] preceding) FROM %s", tableName), tableName,
                ImmutableMap.of("a", toSubfields("a[1]", "a[2]", "a[3]", "a[4]")));

        // no subfield pruning
        assertPushdownSubfields(format("SELECT a[id] FROM %s", tableName), tableName,
                ImmutableMap.of());

        assertPushdownSubfields(format("SELECT a[1] FROM (SELECT DISTINCT * FROM %s) LIMIT 10", tableName), tableName,
                ImmutableMap.of());

        // No pass through subfield pruning
        assertPushdownSubfields(format("SELECT id, min(y[1]).a FROM %s GROUP BY 1", tableName), tableName,
                ImmutableMap.of("y", toSubfields("y[1]")));
        assertPushdownSubfields(format("SELECT id, min(y[1]).a, min(y[1].d).d1 FROM %s GROUP BY 1", tableName), tableName,
                ImmutableMap.of("y", toSubfields("y[1]")));
        assertPushdownSubfields(format("SELECT id, min(z[1][2]).e.e1 FROM %s GROUP BY 1", tableName), tableName,
                ImmutableMap.of("z", toSubfields("z[1][2]")));
    }

    @Test
    public void testPushdownSubfields()
    {
        assertUpdate("CREATE TABLE test_pushdown_struct_subfields(id bigint, x row(a bigint, b varchar, c double, d row(d1 bigint, d2 double)), y array(row(a bigint, b varchar, c double, d row(d1 bigint, d2 double))))");

        assertPushdownSubfields("SELECT t.a, t.d.d1, x.a FROM test_pushdown_struct_subfields CROSS JOIN UNNEST(y) as t(a, b, c, d)", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a"), "y", toSubfields("y[*].a", "y[*].d.d1")));

        assertPushdownSubfields("SELECT x.a, mod(x.d.d1, 2) FROM test_pushdown_struct_subfields", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a", "x.d.d1")));

        assertPushdownSubfields("SELECT x.d, mod(x.d.d1, 2), x.d.d2 FROM test_pushdown_struct_subfields", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.d")));

        assertPushdownSubfields("SELECT x.a FROM test_pushdown_struct_subfields WHERE x.b LIKE 'abc%'", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a", "x.b")));

        assertPushdownSubfields("SELECT x.a FROM test_pushdown_struct_subfields WHERE x.a > 10 AND x.b LIKE 'abc%'", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a", "x.b")));

        // Join
        Session session = getQueryRunner().getDefaultSession();
        assertPlan("SELECT l.orderkey, x.a, mod(x.d.d1, 2) FROM lineitem l, test_pushdown_struct_subfields a WHERE l.linenumber = a.id",
                anyTree(
                        node(JoinNode.class,
                                anyTree(tableScan("lineitem", ImmutableMap.of())),
                                anyTree(tableScan("test_pushdown_struct_subfields", ImmutableMap.of("x", toSubfields("x.a", "x.d.d1")))))));

        assertPlan("SELECT l.orderkey, x.a, mod(x.d.d1, 2) FROM lineitem l, test_pushdown_struct_subfields a WHERE l.linenumber = a.id AND x.a > 10",
                anyTree(
                        node(JoinNode.class,
                                anyTree(tableScan("lineitem", ImmutableMap.of())),
                                anyTree(tableScan("test_pushdown_struct_subfields", ImmutableMap.of("x", toSubfields("x.a", "x.d.d1")))))));

        // Aggregation
        assertPushdownSubfields("SELECT id, min(x.a) FROM test_pushdown_struct_subfields GROUP BY 1", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a")));

        assertPushdownSubfields("SELECT id, min(mod(x.a, 3)) FROM test_pushdown_struct_subfields GROUP BY 1", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a")));

        assertPushdownSubfields("SELECT id, min(x.a) FILTER (WHERE x.b LIKE 'abc%') FROM test_pushdown_struct_subfields GROUP BY 1", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a", "x.b")));

        assertPushdownSubfields("SELECT id, min(x.a + length(y[2].b)) * avg(x.d.d1) FROM test_pushdown_struct_subfields GROUP BY 1", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a", "x.d.d1"), "y", toSubfields("y[2].b")));

        assertPushdownSubfields("SELECT id, arbitrary(x.a) FROM test_pushdown_struct_subfields GROUP BY 1", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a")));

        assertPushdownSubfields("SELECT id, arbitrary(x).a FROM test_pushdown_struct_subfields GROUP BY 1", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a")));

        assertPushdownSubfields("SELECT id, arbitrary(x).d.d1 FROM test_pushdown_struct_subfields GROUP BY 1", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.d.d1")));

        assertPushdownSubfields("SELECT id, arbitrary(x.d).d1 FROM test_pushdown_struct_subfields GROUP BY 1", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.d.d1")));

        assertPushdownSubfields("SELECT id, arbitrary(x.d.d2) FROM test_pushdown_struct_subfields GROUP BY 1", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.d.d2")));

        // Unnest
        assertPushdownSubfields("SELECT t.a, t.d.d1, x.a FROM test_pushdown_struct_subfields CROSS JOIN UNNEST(y) as t(a, b, c, d)", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a"), "y", toSubfields("y[*].a", "y[*].d.d1")));

        assertPushdownSubfields("SELECT t.*, x.a FROM test_pushdown_struct_subfields CROSS JOIN UNNEST(y) as t(a, b, c, d)", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a"), "y", toSubfields("y[*].a", "y[*].b", "y[*].c", "y[*].d")));

        assertPushdownSubfields("SELECT id, x.a FROM test_pushdown_struct_subfields CROSS JOIN UNNEST(y) as t(a, b, c, d)", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a")));

        // Legacy unnest
        Session legacyUnnest = Session.builder(getSession()).setSystemProperty("legacy_unnest", "true").build();
        assertPushdownSubfields(legacyUnnest, "SELECT t.y.a, t.y.d.d1, x.a FROM test_pushdown_struct_subfields CROSS JOIN UNNEST(y) as t(y)", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a"), "y", toSubfields("y[*].a", "y[*].d.d1")));

        assertPushdownSubfields(legacyUnnest, "SELECT t.*, x.a FROM test_pushdown_struct_subfields CROSS JOIN UNNEST(y) as t(y)", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a")));

        assertPushdownSubfields(legacyUnnest, "SELECT id, x.a FROM test_pushdown_struct_subfields CROSS JOIN UNNEST(y) as t(y)", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a")));

        // Case sensitivity
        assertPushdownSubfields("SELECT x.a, x.b, x.A + 2 FROM test_pushdown_struct_subfields WHERE x.B LIKE 'abc%'", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a", "x.b")));

        // No pass-through subfield pruning
        assertPushdownSubfields("SELECT id, min(x.d).d1 FROM test_pushdown_struct_subfields GROUP BY 1", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.d")));

        assertPushdownSubfields("SELECT id, min(x.d).d1, min(x.d.d2) FROM test_pushdown_struct_subfields GROUP BY 1", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.d")));

        assertUpdate("DROP TABLE test_pushdown_struct_subfields");
    }

    @Test
    public void testPushdownSubfieldsAssorted()
    {
        assertUpdate("CREATE TABLE test_pushdown_subfields(" +
                "id bigint, " +
                "a array(bigint), " +
                "b map(bigint, bigint), " +
                "c map(varchar, bigint), " +
                "d row(d1 bigint, d2 array(bigint), d3 map(bigint, bigint), d4 row(x double, y double)), " +
                "w array(array(row(p bigint, e row(e1 bigint, e2 varchar)))), " +
                "x row(a bigint, b varchar, c double, d row(d1 bigint, d2 double)), " +
                "y array(row(a bigint, b varchar, c double, d row(d1 bigint, d2 double))), " +
                "z row(a bigint, b varchar, c double))");

        assertPushdownSubfields("SELECT id, a[1], mod(a[2], 3), b[10], c['cat'] + c['dog'], d.d1 * d.d2[5] / d.d3[2], d.d4.x FROM test_pushdown_subfields", "test_pushdown_subfields",
                ImmutableMap.of(
                        "a", toSubfields("a[1]", "a[2]"),
                        "b", toSubfields("b[10]"),
                        "c", toSubfields("c[\"cat\"]", "c[\"dog\"]"),
                        "d", toSubfields("d.d1", "d.d2[5]", "d.d3[2]", "d.d4.x")));

        assertPushdownSubfields("SELECT count(*) FROM test_pushdown_subfields WHERE a[1] > a[2] AND b[1] * c['cat'] = 5 AND d.d4.x IS NULL", "test_pushdown_subfields",
                ImmutableMap.of(
                        "a", toSubfields("a[1]", "a[2]"),
                        "b", toSubfields("b[1]"),
                        "c", toSubfields("c[\"cat\"]"),
                        "d", toSubfields("d.d4.x")));

        assertPushdownSubfields("SELECT a[1], cardinality(b), map_keys(c), k, v, d.d3[5] FROM test_pushdown_subfields CROSS JOIN UNNEST(c) as t(k, v)", "test_pushdown_subfields",
                ImmutableMap.of(
                        "a", toSubfields("a[1]"),
                        "d", toSubfields("d.d3[5]")));

        // Subfield pruning should pass-through arbitrary() function
        assertPushdownSubfields("SELECT id, " +
                        "arbitrary(x.a), " +
                        "arbitrary(x).a, " +
                        "arbitrary(x).d.d1, " +
                        "arbitrary(x.d).d1, " +
                        "arbitrary(x.d.d2), " +
                        "arbitrary(y[1]).a, " +
                        "arbitrary(y[1]).d.d1, " +
                        "arbitrary(y[2]).d.d1, " +
                        "arbitrary(y[3].d.d1), " +
                        "arbitrary(z).c, " +
                        "arbitrary(w[1][2]).e.e1, " +
                        "arbitrary(w[2][3].e.e2) " +
                        "FROM test_pushdown_subfields " +
                        "GROUP BY 1", "test_pushdown_subfields",
                ImmutableMap.of("x", toSubfields("x.a", "x.d.d1", "x.d.d2"),
                        "y", toSubfields("y[1].a", "y[1].d.d1", "y[2].d.d1", "y[3].d.d1"),
                        "z", toSubfields("z.c"),
                        "w", toSubfields("w[1][2].e.e1", "w[2][3].e.e2")));

        // Subfield pruning should not pass-through other aggregate functions e.g. min() function
        assertPushdownSubfields("SELECT id, " +
                        "min(x.d).d1, " +
                        "min(x.d.d2), " +
                        "min(z).c, " +
                        "min(z.b), " +
                        "min(y[1]).a, " +
                        "min(y[1]).d.d1, " +
                        "min(y[2].d.d1), " +
                        "min(w[1][2]).e.e1, " +
                        "min(w[2][3].e.e2) " +
                        "FROM test_pushdown_subfields " +
                        "GROUP BY 1", "test_pushdown_subfields",
                ImmutableMap.of("x", toSubfields("x.d"),
                        "y", toSubfields("y[1]", "y[2].d.d1"),
                        "w", toSubfields("w[1][2]", "w[2][3].e.e2")));

        assertUpdate("DROP TABLE test_pushdown_subfields");
    }

    @Test
    public void testPushdownFilterAndSubfields()
    {
        assertUpdate("CREATE TABLE test_pushdown_filter_and_subscripts(id bigint, a array(bigint), b array(array(varchar)))");

        Session pushdownFilterEnabled = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .build();

        assertPushdownSubfields("SELECT a[1] FROM test_pushdown_filter_and_subscripts WHERE a[2] > 10", "test_pushdown_filter_and_subscripts",
                ImmutableMap.of("a", toSubfields("a[1]", "a[2]")));

        assertPushdownSubfields(pushdownFilterEnabled, "SELECT a[1] FROM test_pushdown_filter_and_subscripts WHERE a[2] > 10", "test_pushdown_filter_and_subscripts",
                ImmutableMap.of("a", toSubfields("a[1]")));

        assertUpdate("DROP TABLE test_pushdown_filter_and_subscripts");
    }

    @Test
    public void testVirtualBucketing()
    {
        try {
            assertUpdate("CREATE TABLE test_virtual_bucket(a bigint, b bigint)");
            Session virtualBucketEnabled = Session.builder(getSession())
                    .setCatalogSessionProperty(HIVE_CATALOG, "virtual_bucket_count", "2")
                    .build();

            assertPlan(
                    virtualBucketEnabled,
                    "SELECT COUNT(DISTINCT(\"$path\")) FROM test_virtual_bucket",
                    anyTree(
                            exchange(REMOTE_STREAMING, GATHER, anyTree(
                                    tableScan("test_virtual_bucket", ImmutableMap.of())))),
                    assertRemoteExchangesCount(1, getSession(), (DistributedQueryRunner) getQueryRunner()));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_virtual_bucket");
        }
    }

    // TODO: plan verification https://github.com/prestodb/presto/issues/16031
    // TODO: enable all materialized view tests after https://github.com/prestodb/presto/pull/15996
    @Test(enabled = false)
    public void testMaterializedViewOptimization()
    {
        QueryRunner queryRunner = getQueryRunner();
        try {
            queryRunner.execute("CREATE TABLE orders_partitioned WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey > 1000");

            assertUpdate("CREATE MATERIALIZED VIEW test_orders_view WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT orderkey, orderpriority, ds FROM orders_partitioned");
            assertTrue(getQueryRunner().tableExists(getSession(), "test_orders_view"));
            assertUpdate("INSERT INTO test_orders_view(orderkey, orderpriority, ds) " +
                    "select orderkey, orderpriority, ds from orders_partitioned where ds='2020-01-01'", 255);

            String viewQuery = "SELECT orderkey from test_orders_view where orderkey <  10000";
            String baseQuery = "SELECT orderkey from orders_partitioned where orderkey <  10000";
            // getExplainPlan(viewQuery, LOGICAL);

            assertEquals(computeActual(viewQuery).getRowCount(), computeActual(baseQuery).getRowCount());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_orders_view");
            queryRunner.execute("DROP TABLE IF EXISTS orders_partitioned");
        }
    }

    @Test(enabled = false)
    public void testMaterializedViewOptimizationWithClause()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "test_orders_partitioned_with_clause";
        String view = "test_view_orders_partitioned_with_clause";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey > 1000", table));

            String viewPart = format(
                    "WITH X AS (SELECT orderkey, orderpriority, ds FROM %s), " +
                            "Y AS (SELECT orderkey, orderpriority, ds FROM X), " +
                            "Z AS (SELECT orderkey, orderpriority, ds FROM Y) " +
                            "SELECT orderkey, orderpriority, ds FROM Z",
                    table);

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS %s", view, viewPart));
            assertUpdate(format("INSERT INTO %s(orderkey, orderpriority, ds) %s where ds='2020-01-01'", view, viewPart), 255);

            String viewQuery = format("SELECT orderkey from %s where orderkey < 10000", view);
            String baseQuery = viewPart + " where orderkey < 10000";

            assertEquals(computeActual(viewQuery).getRowCount(), computeActual(baseQuery).getRowCount());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test(enabled = false)
    public void testMaterializedViewOptimizationFullyMaterialized()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned_fully_materialized";
        String view = "orders_view_fully_materialized";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey > 1000", table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT orderkey, orderpriority, ds FROM %s", view, table));
            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertUpdate(format("INSERT INTO %s(orderkey, orderpriority, ds) select orderkey, orderpriority, ds from %s", view, table), 15000);

            String viewQuery = format("SELECT orderkey from %s where orderkey <  10000", view);
            String baseQuery = format("SELECT orderkey from %s where orderkey <  10000", table);
            // getExplainPlan(viewQuery, LOGICAL);

            assertEquals(computeActual(viewQuery).getRowCount(), computeActual(baseQuery).getRowCount());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test(enabled = false)
    public void testMaterializedViewOptimizationNotMaterialized()
    {
        String baseTable = "orders_partitioned_not_materialized";
        String view = "orders_partitioned_view_not_materialized";

        QueryRunner queryRunner = getQueryRunner();
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey > 1000", baseTable));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT orderkey, orderpriority, ds FROM %s", view, baseTable));
            assertTrue(getQueryRunner().tableExists(getSession(), view));

            String viewQuery = format("SELECT orderkey from %s where orderkey <  10000", view);
            String baseQuery = format("SELECT orderkey from %s where orderkey <  10000", baseTable);
            // getExplainPlan(viewQuery, LOGICAL);

            assertEquals(computeActual(viewQuery).getRowCount(), computeActual(baseQuery).getRowCount());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + baseTable);
        }
    }

    @Test(enabled = false)
    public void testMaterializedViewOptimizationWithNullPartition()
    {
        QueryRunner queryRunner = getQueryRunner();
        String baseTable = "orders_partitioned_null_partition";
        String view = "orders_partitioned_view_null_partition";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 500 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey > 500 and orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, NULL as ds FROM orders WHERE orderkey > 1000 and orderkey < 1500", baseTable));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, ds FROM %s", view, baseTable));

            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertUpdate(format("INSERT INTO %s(orderkey, orderpriority, ds) " +
                    "select orderkey, orderpriority, ds from %s where ds='2020-01-01'", view, baseTable), 127);

            String viewQuery = format("SELECT orderkey from %s where orderkey <  10000", view);
            String baseQuery = format("SELECT orderkey from %s  where orderkey <  10000", baseTable);
            // getExplainPlan(viewQuery, LOGICAL);

            assertEquals(computeActual(viewQuery).getRowCount(), computeActual(baseQuery).getRowCount());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + baseTable);
        }
    }

    @Test(enabled = false)
    public void testMaterializedViewWithLessGranularity()
    {
        QueryRunner queryRunner = getQueryRunner();
        String baseTable = "orders_partitioned_less_granularity";
        String view = "orders_partitioned_view_less_granularity";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['orderpriority', 'ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey > 1000", baseTable));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, ds FROM %s", view, baseTable));

            assertTrue(getQueryRunner().tableExists(getSession(), view));

            assertUpdate(format("INSERT INTO %s(orderkey, orderpriority, ds) " +
                    "select orderkey, orderpriority, ds from %s where ds='2020-01-01'", view, baseTable), 255);

            String viewQuery = format("SELECT orderkey from %s where orderkey <  10000", view);
            String baseQuery = format("SELECT orderkey from %s where orderkey <  10000", baseTable);
            // getExplainPlan(viewQuery, LOGICAL);

            assertEquals(computeActual(viewQuery).getRowCount(), computeActual(baseQuery).getRowCount());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + baseTable);
        }
    }

    @Test(enabled = false)
    public void testMaterializedViewForUnionAll()
    {
        QueryRunner queryRunner = getQueryRunner();
        String baseTable = "test_customer_union";
        String view = "test_customer_view_union";
        try {
            computeActual(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey']) " +
                    "AS SELECT custkey, name, address, nationkey FROM customer", baseTable));

            String baseQuery = format(
                    "SELECT name, custkey, nationkey FROM ( SELECT name, custkey, nationkey FROM %s WHERE custkey < 1000 UNION ALL " +
                            "SELECT name, custkey, nationkey FROM %s WHERE custkey >= 1000)", baseTable, baseTable);

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['nationkey']) " +
                    "AS %s", view, baseQuery));

            assertUpdate(format("INSERT INTO %s(name, custkey, nationkey) %s where nationkey < 10", view, baseQuery), 599);

            String viewQuery = format("SELECT name, custkey, nationkey from %s", view);
            assertEquals(computeActual(viewQuery).getRowCount(), computeActual(baseQuery).getRowCount());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + baseTable);
        }
    }

    @Test(enabled = false)
    public void testMaterializedViewForGroupingSet()
    {
        QueryRunner queryRunner = getQueryRunner();
        String baseTable = "test_lineitem_grouping_set";
        String view = "test_view_lineitem_grouping_set";
        try {
            computeActual(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['shipmode']) " +
                    "AS SELECT linenumber, quantity, shipmode FROM lineitem", baseTable));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['shipmode']) " +
                            "AS SELECT linenumber, SUM(DISTINCT CAST(quantity AS BIGINT)) quantity, shipmode FROM %s " +
                            "GROUP BY GROUPING SETS ((linenumber, shipmode), (shipmode))",
                    view, baseTable));

            assertUpdate(format("INSERT INTO %s(linenumber, quantity, shipmode) " +
                            "SELECT linenumber, SUM(DISTINCT CAST(quantity AS BIGINT)) quantity, shipmode FROM %s where shipmode='RAIL' " +
                            "GROUP BY GROUPING SETS ((linenumber, shipmode), (shipmode)) ",
                    view, baseTable), 8);

            String viewQuery = format("SELECT * FROM %s ", view);
            String baseQuery = format("SELECT linenumber, SUM(DISTINCT CAST(quantity AS BIGINT)) quantity, shipmode FROM %s " +
                    "GROUP BY GROUPING SETS ((linenumber, shipmode), (shipmode))", baseTable);
            assertEquals(computeActual(viewQuery).getRowCount(), computeActual(baseQuery).getRowCount());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + baseTable);
        }
    }

    @Test(enabled = false)
    public void testMaterializedViewWithDifferentPartitions()
    {
        QueryRunner queryRunner = getQueryRunner();
        String baseTable = "orders_partitioned_different_partitions";
        String view = "orders_partitioned_view_different_partitions";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'orderpriority']) AS " +
                    "SELECT orderkey, orderstatus, '2020-01-01' as ds, orderpriority FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderstatus, '2019-01-02' as ds, orderpriority FROM orders WHERE orderkey > 1000", baseTable));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds', 'orderstatus']) AS " +
                    "SELECT orderkey, orderpriority, ds, orderstatus FROM %s", view, baseTable));

            assertTrue(getQueryRunner().tableExists(getSession(), view));

            assertUpdate(format("INSERT INTO %s(orderkey, orderpriority, ds, orderstatus) " +
                    "select orderkey, orderpriority, ds, orderstatus from %s where ds='2020-01-01'", view, baseTable), 255);

            String viewQuery = format("SELECT orderkey from %s where orderkey <  10000", view);
            String baseQuery = format("SELECT orderkey from %s where orderkey <  10000", baseTable);
            // getExplainPlan(viewQuery, LOGICAL);

            assertEquals(computeActual(viewQuery).getRowCount(), computeActual(baseQuery).getRowCount());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + baseTable);
        }
    }

    @Test(enabled = false)
    public void testMaterializedViewJoinsWithOneTableAlias()
    {
        QueryRunner queryRunner = getQueryRunner();
        String view = "view_join_with_one_alias";
        String table1 = "nation_partitioned_join_with_one_alias";
        String table2 = "customer_partitioned_join_with_one_alias";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey', 'regionkey']) AS " +
                    "SELECT name, nationkey, regionkey FROM nation", table1));

            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey']) AS SELECT custkey," +
                    " name, mktsegment, nationkey FROM customer", table2));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['marketsegment', " +
                            "'nationkey', 'regionkey']) AS SELECT %s.name AS nationname, " +
                            "customer.custkey, customer.name AS customername, UPPER(customer.mktsegment) AS marketsegment, customer.nationkey, regionkey " +
                            "FROM %s JOIN %s customer ON (%s.nationkey = customer.nationkey)",
                    view, table1, table1, table2, table1));

            assertUpdate(format("INSERT INTO %s(nationname, custkey, customername, marketsegment, nationkey, regionkey) " +
                            "SELECT %s.name AS nationname, customer.custkey, customer.name AS customername, UPPER(customer.mktsegment) " +
                            "AS marketsegment, customer.nationkey, regionkey FROM %s JOIN %s customer ON (%s.nationkey = customer.nationkey) " +
                            "WHERE customer.nationkey != 24 and %s.regionkey != 1",
                    view, table1, table1, table2, table1, table1), 1200);

            String viewQuery = format("SELECT nationname, custkey from %s", view);
            String baseQuery = format("SELECT %s.name AS nationname, customer.custkey FROM %s JOIN %s customer ON (%s.nationkey = customer.nationkey)",
                    table1, table1, table2, table1);

            // getExplainPlan(viewQuery, LOGICAL);
            assertEquals(computeActual(viewQuery).getRowCount(), computeActual(baseQuery).getRowCount());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test(enabled = false)
    public void testMaterializedViewOptimizationWithDerivedFields()
    {
        QueryRunner queryRunner = getQueryRunner();
        String baseTable = "lineitem_partitioned_derived_fields";
        String view = "lineitem_partitioned_view_derived_fields";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'shipmode']) AS " +
                    "SELECT discount, extendedprice, '2020-01-01' as ds, shipmode FROM lineitem WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT discount, extendedprice, '2020-01-02' as ds, shipmode FROM lineitem WHERE orderkey > 1000", baseTable));

            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds', 'shipmode']) AS " +
                            "SELECT SUM(discount*extendedprice) as _discount_multi_extendedprice_, ds, shipmode FROM %s group by ds, shipmode",
                    view, baseTable));

            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertUpdate(format("INSERT INTO %s(_discount_multi_extendedprice_, ds, shipmode) " +
                            "select SUM(discount*extendedprice), ds, shipmode from %s where ds='2020-01-01' group by ds, shipmode",
                    view, baseTable), 7);

            String viewQuery = format("SELECT sum(_discount_multi_extendedprice_) from %s group by ds, shipmode", view);
            String baseQuery = format("SELECT sum(discount * extendedprice) as _discount_multi_extendedprice_ from %s group by ds, shipmode", baseTable);
            // getExplainPlan(viewQuery, LOGICAL);

            assertEquals(computeActual(viewQuery).getRowCount(), computeActual(baseQuery).getRowCount());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + baseTable);
        }
    }

    @Test(enabled = false)
    public void testMaterializedViewOptimizationWithDerivedFieldsWithAlias()
    {
        QueryRunner queryRunner = getQueryRunner();
        String baseTable = "lineitem_partitioned_derived_fields_with_alias";
        String view = "lineitem_partitioned_view_derived_fields_with_alias";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'shipmode']) AS " +
                    "SELECT discount, extendedprice, '2020-01-01' as ds, shipmode FROM lineitem WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT discount, extendedprice, '2020-01-02' as ds, shipmode FROM lineitem WHERE orderkey > 1000 ", baseTable));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds', 'view_shipmode']) " +
                    "AS SELECT SUM(discount*extendedprice) as _discount_multi_extendedprice_, ds, shipmode as view_shipmode " +
                    "FROM %s group by ds, shipmode", view, baseTable));

            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertUpdate(format("INSERT INTO %s(_discount_multi_extendedprice_, ds, view_shipmode) " +
                            "select SUM(discount*extendedprice), ds, shipmode from %s where ds='2020-01-01' group by ds, shipmode",
                    view, baseTable), 7);

            String viewQuery = format("SELECT sum(_discount_multi_extendedprice_) from %s group by ds", view);
            String baseQuery = format("SELECT sum(discount * extendedprice) as _discount_multi_extendedprice_ from %s group by ds", baseTable);
            // getExplainPlan(viewQuery, LOGICAL);

            assertEquals(computeActual(viewQuery).getRowCount(), computeActual(baseQuery).getRowCount());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + baseTable);
        }
    }

    @Test(enabled = false)
    public void testBaseToViewConversionWithDerivedFields()
    {
        Session queryOptimizationWithMaterializedView = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .build();
        QueryRunner queryRunner = getQueryRunner();
        String baseTable = "lineitem_partitioned_derived_fields";
        String view = "lineitem_partitioned_view_derived_fields";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'shipmode']) AS " +
                    "SELECT discount, extendedprice, '2020-01-01' as ds, shipmode FROM lineitem WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT discount, extendedprice, '2020-01-02' as ds, shipmode FROM lineitem WHERE orderkey > 1000", baseTable));
            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['mvds', 'shipmode']) AS " +
                            "SELECT SUM(discount*extendedprice+discount) as _discount_multi_extendedprice_ , MAX(discount*extendedprice) as _max_discount_multi_extendedprice_ , ds as mvds, shipmode FROM %s group by ds, shipmode",
                    view, baseTable));
            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertUpdate(format("INSERT INTO %s(_discount_multi_extendedprice_ , _max_discount_multi_extendedprice_ , mvds, shipmode) " +
                            "select SUM(discount*extendedprice), MAX(discount*extendedprice), ds, shipmode from %s where ds='2020-01-01' group by ds, shipmode",
                    view, baseTable), 7);
            String baseQuery = format("SELECT sum(discount * extendedprice + discount) as _discount_multi_extendedprice_ , MAX(discount*extendedprice) as _max_discount_multi_extendedprice_ , ds, shipmode as method from %s group by ds, shipmode", baseTable);
            queryRunner.execute(queryOptimizationWithMaterializedView, baseQuery);
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + baseTable);
        }
    }

    //FIXME: It does not work as column map in the materialized View metadata is not correct. It only contains 1 map instead of 2.
    // https://github.com/prestodb/presto/pull/15996
    @Test(enabled = false)
    public void testMaterializedViewForJoin()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table1 = "orders_key_partitioned_join";
        String table2 = "orders_price_partitioned_join";
        String view = "orders_view_join";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, '2019-01-02' as ds FROM orders WHERE orderkey > 1000", table1));

            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT totalprice, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT totalprice, '2019-01-02' as ds FROM orders WHERE orderkey > 1000", table2));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT t1.orderkey as view_orderkey, t2.totalprice as view_totalprice, t1.ds" +
                    " FROM %s t1 inner join  %s t2 ON t1.ds=t2.ds", view, table1, table2));

            assertTrue(queryRunner.tableExists(getSession(), view));

            assertUpdate(format("INSERT INTO %s(view_orderkey, view_totalprice, ds, view_orderpriority, view_orderstatus) " +
                    "SELECT SELECT t1.orderkey as view_orderkey, t2.totalprice as view_totalprice, t1.ds " +
                    " FROM %s t1 inner join %s t2 ON t1.ds=t2.ds" +
                    " where t1.ds='2020-01-01'", view, table1, table2), 65025);

            String viewQuery = format("SELECT view_orderkey, view_totalprice from %s where view_orderkey <  10000", view);
            // getExplainPlan(viewQuery, LOGICAL);
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test(enabled = false)
    public void testMaterializedViewOptimizationWithDoublePartition()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned_double_partition";
        String view = "orders_view_double_partition";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['totalprice']) AS " +
                    "SELECT orderkey, orderpriority, totalprice FROM orders WHERE orderkey < 10 ", table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['totalprice']) " +
                    "AS SELECT orderkey, orderpriority, totalprice FROM %s", view, table));
            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertUpdate(format("INSERT INTO %s(orderkey, orderpriority, totalprice) " +
                    "select orderkey, orderpriority, totalprice from %s where totalprice<65000", view, table), 3);

            String viwQuery = format("SELECT orderkey from %s where orderkey <  10000", view);
            String baseQuery = format("SELECT orderkey from %s where orderkey <  10000", table);
            // getExplainPlan(viewQuery, LOGICAL);

            assertEquals(computeActual(viwQuery).getRowCount(), computeActual(baseQuery).getRowCount());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test(enabled = false)
    public void testMaterializedViewForJoinWithMultiplePartitions()
    {
        QueryRunner queryRunner = getQueryRunner();
        String view = "order_view_join_with_multiple_partitions";
        String table1 = "orders_key_partitioned_join_with_multiple_partitions";
        String table2 = "orders_price_partitioned_join_with_multiple_partitions";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'orderpriority']) AS " +
                    "SELECT orderkey, '2020-01-01' as ds, orderpriority FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, '2019-01-02' as ds , orderpriority FROM orders WHERE orderkey > 1000 and orderkey < 2000", table1));

            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'orderstatus']) AS " +
                    "SELECT totalprice, '2020-01-01' as ds, orderstatus FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT totalprice, '2019-01-02' as ds, orderstatus FROM orders WHERE orderkey > 1000 and orderkey < 2000", table2));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds', 'view_orderpriority', 'view_orderstatus']) " +
                    "AS SELECT t1.orderkey as view_orderkey, t2.totalprice as view_totalprice, " +
                    "t1.ds as ds, t1.orderpriority as view_orderpriority, t2.orderstatus as view_orderstatus " +
                    " FROM %s t1 inner join %s t2 ON t1.ds=t2.ds", view, table1, table2));

            assertUpdate(format("INSERT INTO %s(view_orderkey, view_totalprice, ds, view_orderpriority, view_orderstatus) " +
                    "SELECT t1.orderkey as view_orderkey, t2.totalprice as view_totalprice, t1.ds as ds, t1.orderpriority as view_orderpriority, " +
                    "t2.orderstatus as view_orderstatus FROM %s t1 inner join %s t2 ON t1.ds=t2.ds" +
                    " where t1.ds='2020-01-01'", view, table1, table2), 65025);

            String viewQuery = format("SELECT view_orderkey from %s where view_orderkey <  10000", view);
            String baseQuery = format("SELECT t1.orderkey FROM %s t1" +
                    " inner join %s t2 ON t1.ds=t2.ds where t1.orderkey <  10000", table1, table2);

            // getExplainPlan(viewQuery, LOGICAL);
            assertEquals(computeActual(viewQuery).getRowCount(), computeActual(baseQuery).getRowCount());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test(enabled = false)
    public void testMaterialziedViewFullOuterJoin()
    {
        QueryRunner queryRunner = getQueryRunner();
        String view = "order_view_full_outer_join";
        String table1 = "orders_key_partitioned_full_outer_join";
        String table2 = "orders_price_partitioned_full_outer_join";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'orderpriority']) AS " +
                    "SELECT orderkey, '2020-01-01' as ds, orderpriority FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, '2019-01-02' as ds , orderpriority FROM orders WHERE orderkey > 1000 and orderkey < 2000", table1));

            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'orderstatus']) AS " +
                    "SELECT totalprice, '2020-01-01' as ds, orderstatus FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT totalprice, '2019-01-02' as ds, orderstatus FROM orders WHERE orderkey > 1000 and orderkey < 2000", table2));

            assertQueryFails(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds', 'view_orderpriority', 'view_orderstatus']) " +
                            "AS SELECT t1.orderkey as view_orderkey, t2.totalprice as view_totalprice, " +
                            "t1.ds as ds, t1.orderpriority as view_orderpriority, t2.orderstatus as view_orderstatus " +
                            " FROM %s t1 full outer join %s t2 ON t1.ds=t2.ds", view, table1, table2),
                    ".*Only inner join is supported for materialized view.*");
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    // Make sure subfield pruning doesn't interfere with cost-based optimizer
    @Test
    public void testPushdownSubfieldsAndJoinReordering()
    {
        Session collectStatistics = Session.builder(getSession())
                .setCatalogSessionProperty(HIVE_CATALOG, COLLECT_COLUMN_STATISTICS_ON_WRITE, "true")
                .build();

        getQueryRunner().execute(collectStatistics, "CREATE TABLE orders_ex AS SELECT orderkey, custkey, array[custkey] as keys FROM orders");

        try {
            Session joinReorderingOn = Session.builder(pushdownFilterEnabled())
                    .setSystemProperty(JOIN_REORDERING_STRATEGY, FeaturesConfig.JoinReorderingStrategy.AUTOMATIC.name())
                    .build();

            Session joinReorderingOff = Session.builder(pushdownFilterEnabled())
                    .setSystemProperty(JOIN_REORDERING_STRATEGY, FeaturesConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS.name())
                    .build();

            assertPlan(joinReorderingOff, "SELECT sum(custkey) FROM orders_ex o, lineitem l WHERE o.orderkey = l.orderkey",
                    anyTree(join(INNER, ImmutableList.of(equiJoinClause("o_orderkey", "l_orderkey")),
                            anyTree(PlanMatchPattern.tableScan("orders_ex", ImmutableMap.of("o_orderkey", "orderkey"))),
                            anyTree(PlanMatchPattern.tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey"))))));

            assertPlan(joinReorderingOff, "SELECT sum(keys[1]) FROM orders_ex o, lineitem l WHERE o.orderkey = l.orderkey",
                    anyTree(join(INNER, ImmutableList.of(equiJoinClause("o_orderkey", "l_orderkey")),
                            anyTree(PlanMatchPattern.tableScan("orders_ex", ImmutableMap.of("o_orderkey", "orderkey"))),
                            anyTree(PlanMatchPattern.tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey"))))));

            assertPlan(joinReorderingOn, "SELECT sum(custkey) FROM orders_ex o, lineitem l WHERE o.orderkey = l.orderkey",
                    anyTree(join(INNER, ImmutableList.of(equiJoinClause("l_orderkey", "o_orderkey")),
                            anyTree(PlanMatchPattern.tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey"))),
                            anyTree(PlanMatchPattern.tableScan("orders_ex", ImmutableMap.of("o_orderkey", "orderkey"))))));

            assertPlan(joinReorderingOn, "SELECT sum(keys[1]) FROM orders_ex o, lineitem l WHERE o.orderkey = l.orderkey",
                    anyTree(join(INNER, ImmutableList.of(equiJoinClause("l_orderkey", "o_orderkey")),
                            anyTree(PlanMatchPattern.tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey"))),
                            anyTree(PlanMatchPattern.tableScan("orders_ex", ImmutableMap.of("o_orderkey", "orderkey"))))));

            assertPlan(joinReorderingOff, "SELECT l.discount, l.orderkey, o.totalprice FROM lineitem l, orders o WHERE l.orderkey = o.orderkey AND l.quantity < 2 AND o.totalprice BETWEEN 0 AND 200000",
                    anyTree(
                            node(JoinNode.class,
                                    anyTree(tableScan("lineitem", ImmutableMap.of())),
                                    anyTree(tableScan("orders", ImmutableMap.of())))));

            assertPlan(joinReorderingOn, "SELECT l.discount, l.orderkey, o.totalprice FROM lineitem l, orders o WHERE l.orderkey = o.orderkey AND l.quantity < 2 AND o.totalprice BETWEEN 0 AND 200000",
                    anyTree(
                            node(JoinNode.class,
                                    anyTree(tableScan("orders", ImmutableMap.of())),
                                    anyTree(tableScan("lineitem", ImmutableMap.of())))));

            assertPlan(joinReorderingOff, "SELECT keys[1] FROM orders_ex o, lineitem l WHERE o.orderkey = l.orderkey AND o.keys[1] > 0",
                    anyTree(join(INNER, ImmutableList.of(equiJoinClause("o_orderkey", "l_orderkey")),
                            anyTree(PlanMatchPattern.tableScan("orders_ex", ImmutableMap.of("o_orderkey", "orderkey"))),
                            anyTree(PlanMatchPattern.tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey"))))));

            assertPlan(joinReorderingOn, "SELECT keys[1] FROM orders_ex o, lineitem l WHERE o.orderkey = l.orderkey AND o.keys[1] > 0",
                    anyTree(join(INNER, ImmutableList.of(equiJoinClause("l_orderkey", "o_orderkey")),
                            anyTree(PlanMatchPattern.tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey"))),
                            anyTree(PlanMatchPattern.tableScan("orders_ex", ImmutableMap.of("o_orderkey", "orderkey"))))));
        }
        finally {
            assertUpdate("DROP TABLE orders_ex");
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

        assertParquetDereferencePushDown("SELECT x.a FROM test_pushdown_nestedcolumn_parquet WHERE x.a > 10 AND x.b LIKE 'abc%'",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a", "x.b"),
                withColumnDomains(ImmutableMap.of(pushdownColumnNameForSubfield(nestedColumn("x.a")), create(ofRanges(greaterThan(BIGINT, 10L)), false))),
                ImmutableSet.of(pushdownColumnNameForSubfield(nestedColumn("x.a"))),
                TRUE_CONSTANT);

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
                                        withColumnDomains(ImmutableMap.of(pushdownColumnNameForSubfield(nestedColumn("x.a")), create(ofRanges(greaterThan(BIGINT, 10L)), false))),
                                        ImmutableSet.of(pushdownColumnNameForSubfield(nestedColumn("x.a"))),
                                        TRUE_CONSTANT)))));

        // Self-Join and Table scan assignments
        assertPlan(withParquetDereferencePushDownEnabled(), "SELECT t1.x.a, t2.x.a FROM test_pushdown_nestedcolumn_parquet t1, test_pushdown_nestedcolumn_parquet t2 where t1.id = t2.id",
                anyTree(
                        node(JoinNode.class,
                                anyTree(tableScanParquetDeferencePushDowns("test_pushdown_nestedcolumn_parquet", nestedColumnMap("x.a"))),
                                anyTree(tableScanParquetDeferencePushDowns("test_pushdown_nestedcolumn_parquet", nestedColumnMap("x_1.a"))))));

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
        assertParquetDereferencePushDown("SELECT id, x.d.d1 FROM test_pushdown_nestedcolumn_parquet WHERE x.d.d1 = 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d.d1"),
                withColumnDomains(ImmutableMap.of(
                        pushdownColumnNameForSubfield(nestedColumn("x.d.d1")), singleValue(BIGINT, 1L))),
                ImmutableSet.of(pushdownColumnNameForSubfield(nestedColumn("x.d.d1"))),
                TRUE_CONSTANT);

        assertParquetDereferencePushDown("SELECT id, x.d.d1 FROM test_pushdown_nestedcolumn_parquet WHERE x.d.d1 = 1 and x.d.d2 = 5.0",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d.d1", "x.d.d2"),
                withColumnDomains(ImmutableMap.of(
                        pushdownColumnNameForSubfield(nestedColumn("x.d.d1")), singleValue(BIGINT, 1L),
                        pushdownColumnNameForSubfield(nestedColumn("x.d.d2")), singleValue(DOUBLE, 5.0))),
                ImmutableSet.of(
                        pushdownColumnNameForSubfield(nestedColumn("x.d.d1")),
                        pushdownColumnNameForSubfield(nestedColumn("x.d.d2"))),
                TRUE_CONSTANT);

        assertUpdate("DROP TABLE test_pushdown_nestedcolumn_parquet");
    }

    @Test
    public void testBucketPruning()
    {
        QueryRunner queryRunner = getQueryRunner();
        queryRunner.execute("CREATE TABLE orders_bucketed WITH (bucket_count = 11, bucketed_by = ARRAY['orderkey']) AS " +
                "SELECT * FROM orders");

        try {
            assertPlan(getSession(), "SELECT * FROM orders_bucketed WHERE orderkey = 100",
                    anyTree(PlanMatchPattern.tableScan("orders_bucketed")),
                    plan -> assertBucketFilter(plan, "orders_bucketed", 11, ImmutableSet.of(1)));

            assertPlan(getSession(), "SELECT * FROM orders_bucketed WHERE orderkey = 100 OR orderkey = 101",
                    anyTree(PlanMatchPattern.tableScan("orders_bucketed")),
                    plan -> assertBucketFilter(plan, "orders_bucketed", 11, ImmutableSet.of(1, 2)));

            assertPlan(getSession(), "SELECT * FROM orders_bucketed WHERE orderkey IN (100, 101, 133)",
                    anyTree(PlanMatchPattern.tableScan("orders_bucketed")),
                    plan -> assertBucketFilter(plan, "orders_bucketed", 11, ImmutableSet.of(1, 2)));

            assertPlan(getSession(), "SELECT * FROM orders_bucketed",
                    anyTree(PlanMatchPattern.tableScan("orders_bucketed")),
                    plan -> assertNoBucketFilter(plan, "orders_bucketed", 11));

            assertPlan(getSession(), "SELECT * FROM orders_bucketed WHERE orderkey > 100",
                    anyTree(PlanMatchPattern.tableScan("orders_bucketed")),
                    plan -> assertNoBucketFilter(plan, "orders_bucketed", 11));

            assertPlan(getSession(), "SELECT * FROM orders_bucketed WHERE orderkey != 100",
                    anyTree(PlanMatchPattern.tableScan("orders_bucketed")),
                    plan -> assertNoBucketFilter(plan, "orders_bucketed", 11));
        }
        finally {
            queryRunner.execute("DROP TABLE orders_bucketed");
        }

        queryRunner.execute("CREATE TABLE orders_bucketed WITH (bucket_count = 11, bucketed_by = ARRAY['orderkey', 'custkey']) AS " +
                "SELECT * FROM orders");

        try {
            assertPlan(getSession(), "SELECT * FROM orders_bucketed WHERE orderkey = 101 AND custkey = 280",
                    anyTree(PlanMatchPattern.tableScan("orders_bucketed")),
                    plan -> assertBucketFilter(plan, "orders_bucketed", 11, ImmutableSet.of(1)));

            assertPlan(getSession(), "SELECT * FROM orders_bucketed WHERE orderkey IN (101, 71) AND custkey = 280",
                    anyTree(PlanMatchPattern.tableScan("orders_bucketed")),
                    plan -> assertBucketFilter(plan, "orders_bucketed", 11, ImmutableSet.of(1, 6)));

            assertPlan(getSession(), "SELECT * FROM orders_bucketed WHERE orderkey IN (101, 71) AND custkey IN (280, 34)",
                    anyTree(PlanMatchPattern.tableScan("orders_bucketed")),
                    plan -> assertBucketFilter(plan, "orders_bucketed", 11, ImmutableSet.of(1, 2, 6, 8)));

            assertPlan(getSession(), "SELECT * FROM orders_bucketed WHERE orderkey = 101 AND custkey = 280 AND orderstatus <> '0'",
                    anyTree(PlanMatchPattern.tableScan("orders_bucketed")),
                    plan -> assertBucketFilter(plan, "orders_bucketed", 11, ImmutableSet.of(1)));

            assertPlan(getSession(), "SELECT * FROM orders_bucketed WHERE orderkey = 101",
                    anyTree(PlanMatchPattern.tableScan("orders_bucketed")),
                    plan -> assertNoBucketFilter(plan, "orders_bucketed", 11));

            assertPlan(getSession(), "SELECT * FROM orders_bucketed WHERE custkey = 280",
                    anyTree(PlanMatchPattern.tableScan("orders_bucketed")),
                    plan -> assertNoBucketFilter(plan, "orders_bucketed", 11));

            assertPlan(getSession(), "SELECT * FROM orders_bucketed WHERE orderkey = 101 AND custkey > 280",
                    anyTree(PlanMatchPattern.tableScan("orders_bucketed")),
                    plan -> assertNoBucketFilter(plan, "orders_bucketed", 11));
        }
        finally {
            queryRunner.execute("DROP TABLE orders_bucketed");
        }
    }

    @Test
    public void testAddRequestedColumnsToLayout()
    {
        String tableName = "test_add_requested_columns_to_layout";
        assertUpdate(format("CREATE TABLE %s(" +
                "id bigint, " +
                "a row(d1 bigint, d2 array(bigint), d3 map(bigint, bigint), d4 row(x double, y double)), " +
                "b varchar )", tableName));

        try {
            assertPlan(getSession(), format("SELECT b FROM %s", tableName),
                    anyTree(PlanMatchPattern.tableScan(tableName)),
                    plan -> assertRequestedColumnsInLayout(plan, tableName, ImmutableSet.of("b")));

            assertPlan(getSession(), format("SELECT id, b FROM %s", tableName),
                    anyTree(PlanMatchPattern.tableScan(tableName)),
                    plan -> assertRequestedColumnsInLayout(plan, tableName, ImmutableSet.of("id", "b")));

            assertPlan(getSession(), format("SELECT id, a FROM %s", tableName),
                    anyTree(PlanMatchPattern.tableScan(tableName)),
                    plan -> assertRequestedColumnsInLayout(plan, tableName, ImmutableSet.of("id", "a")));

            assertPlan(getSession(), format("SELECT a.d1, a.d4.x FROM %s", tableName),
                    anyTree(PlanMatchPattern.tableScan(tableName)),
                    plan -> assertRequestedColumnsInLayout(plan, tableName, ImmutableSet.of("a.d1", "a.d4.x")));
        }
        finally {
            assertUpdate(format("DROP TABLE %s", tableName));
        }
    }

    @Test
    public void testPartialAggregatePushdown()
    {
        QueryRunner queryRunner = getQueryRunner();
        try {
            queryRunner.execute("CREATE TABLE orders_partitioned_parquet WITH (partitioned_by = ARRAY['ds'], format='PARQUET') AS " +
                    "SELECT orderkey, orderpriority, comment, '2019-11-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, comment, '2019-11-02' as ds FROM orders WHERE orderkey < 1000");

            Map<Optional<String>, ExpectedValueProvider<FunctionCall>> aggregations = ImmutableMap.of(Optional.of("count"),
                    PlanMatchPattern.functionCall("count", false, ImmutableList.of(anySymbol())));
            List<String> groupByKey = ImmutableList.of("count_star");
            assertPlan(partialAggregatePushdownEnabled(),
                    "select count(*) from orders_partitioned_parquet",
                    anyTree(aggregation(globalAggregation(), aggregations, ImmutableMap.of(), Optional.empty(), AggregationNode.Step.FINAL,
                            exchange(LOCAL, GATHER,
                                    new PlanMatchPattern[] {exchange(REMOTE_STREAMING, GATHER,
                                            new PlanMatchPattern[] {tableScan("orders_partitioned_parquet", ImmutableMap.of())})}))));

            aggregations = ImmutableMap.of(
                    Optional.of("count_1"),
                    PlanMatchPattern.functionCall("count", false, ImmutableList.of(anySymbol())),
                    Optional.of("max"),
                    PlanMatchPattern.functionCall("max", false, ImmutableList.of(anySymbol())),
                    Optional.of("min"),
                    PlanMatchPattern.functionCall("max", false, ImmutableList.of(anySymbol())));

            assertPlan(partialAggregatePushdownEnabled(),
                    "select count(orderkey), max(orderpriority), min(ds) from orders_partitioned_parquet",
                    anyTree(new PlanMatchPattern[] {aggregation(globalAggregation(), aggregations, ImmutableMap.of(), Optional.empty(), AggregationNode.Step.FINAL,
                            exchange(LOCAL, GATHER,
                                    new PlanMatchPattern[] {exchange(REMOTE_STREAMING, GATHER,
                                            new PlanMatchPattern[] {tableScan(
                                                    "orders_partitioned_parquet",
                                                    ImmutableMap.of("orderkey",
                                                            ImmutableSet.of(),
                                                            "orderpriority",
                                                            ImmutableSet.of(),
                                                            "ds",
                                                            ImmutableSet.of()))})}))}));

            // Negative tests
            assertPlan(partialAggregatePushdownEnabled(),
                    "select count(orderkey), max(orderpriority), min(ds) from orders_partitioned_parquet where orderkey = 100",
                    anyTree(PlanMatchPattern.tableScan("orders_partitioned_parquet")),
                    plan -> assertNoAggregatedColumns(plan, "orders_partitioned_parquet"));

            aggregations = ImmutableMap.of(
                    Optional.of("count_1"),
                    PlanMatchPattern.functionCall("count", false, ImmutableList.of(anySymbol())),
                    Optional.of("arbitrary"),
                    PlanMatchPattern.functionCall("arbitrary", false, ImmutableList.of(anySymbol())),
                    Optional.of("min"),
                    PlanMatchPattern.functionCall("max", false, ImmutableList.of(anySymbol())));
            assertPlan(partialAggregatePushdownEnabled(),
                    "select count(orderkey), arbitrary(orderpriority), min(ds) from orders_partitioned_parquet",
                    anyTree(PlanMatchPattern.tableScan("orders_partitioned_parquet")),
                    plan -> assertNoAggregatedColumns(plan, "orders_partitioned_parquet"));

            assertPlan(partialAggregatePushdownEnabled(),
                    "select count(orderkey), max(orderpriority), min(ds) from orders_partitioned_parquet where ds = '2019-11-01' and orderkey = 100",
                    anyTree(PlanMatchPattern.tableScan("orders_partitioned_parquet")),
                    plan -> assertNoAggregatedColumns(plan, "orders_partitioned_parquet"));

            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setCatalogSessionProperty(HIVE_CATALOG, PARTIAL_AGGREGATION_PUSHDOWN_ENABLED, "true")
                    .build();
            queryRunner.execute("CREATE TABLE variable_length_table(col1 varchar, col2 varchar(100), col3 varbinary) with (format='PARQUET')");
            queryRunner.execute("INSERT INTO variable_length_table values ('foo','bar',cast('baz' as varbinary))");
            assertPlan(session,
                    "select min(col1) from variable_length_table",
                    anyTree(PlanMatchPattern.tableScan("variable_length_table")),
                    plan -> assertNoAggregatedColumns(plan, "variable_length_table"));
            assertPlan(session,
                    "select max(col3) from variable_length_table",
                    anyTree(PlanMatchPattern.tableScan("variable_length_table")),
                    plan -> assertNoAggregatedColumns(plan, "variable_length_table"));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS orders_partitioned_parquet");
            queryRunner.execute("DROP TABLE IF EXISTS variable_length_table");
        }
    }

    private static Set<Subfield> toSubfields(String... subfieldPaths)
    {
        return Arrays.stream(subfieldPaths)
                .map(Subfield::new)
                .collect(toImmutableSet());
    }

    private void assertPushdownSubfields(String query, String tableName, Map<String, Set<Subfield>> requiredSubfields)
    {
        assertPlan(query, anyTree(tableScan(tableName, requiredSubfields)));
    }

    private void assertPushdownSubfields(Session session, String query, String tableName, Map<String, Set<Subfield>> requiredSubfields)
    {
        assertPlan(session, query, anyTree(tableScan(tableName, requiredSubfields)));
    }

    private static PlanMatchPattern tableScan(String expectedTableName, Map<String, Set<Subfield>> expectedRequiredSubfields)
    {
        return PlanMatchPattern.tableScan(expectedTableName).with(new HiveTableScanMatcher(expectedRequiredSubfields));
    }

    private static PlanMatchPattern tableScanParquetDeferencePushDowns(String expectedTableName, Map<String, Subfield> expectedDeferencePushDowns)
    {
        return PlanMatchPattern.tableScan(expectedTableName).with(new HiveParquetDereferencePushdownMatcher(expectedDeferencePushDowns, TupleDomain.all(), ImmutableSet.of(), TRUE_CONSTANT));
    }

    private static PlanMatchPattern tableScanParquetDeferencePushDowns(String expectedTableName, Map<String, Subfield> expectedDeferencePushDowns,
            TupleDomain<String> domainPredicate, Set<String> predicateColumns, RowExpression remainingPredicate)
    {
        return PlanMatchPattern.tableScan(expectedTableName).with(new HiveParquetDereferencePushdownMatcher(expectedDeferencePushDowns, domainPredicate, predicateColumns, remainingPredicate));
    }

    private static boolean isTableScanNode(PlanNode node, String tableName)
    {
        return node instanceof TableScanNode && ((HiveTableHandle) ((TableScanNode) node).getTable().getConnectorHandle()).getTableName().equals(tableName);
    }

    private void assertPushdownFilterOnSubfields(String query, Map<Subfield, Domain> predicateDomains)
    {
        String tableName = "test_pushdown_filter_on_subfields";
        assertPlan(pushdownFilterAndNestedColumnFilterEnabled(), query,
                output(exchange(PlanMatchPattern.tableScan(tableName))),
                plan -> assertTableLayout(
                        plan,
                        tableName,
                        withColumnDomains(predicateDomains),
                        TRUE_CONSTANT,
                        predicateDomains.keySet().stream().map(Subfield::getRootName).collect(toImmutableSet())));
    }

    private void assertParquetDereferencePushDown(String query, String tableName, Map<String, Subfield> expectedDeferencePushDowns)
    {
        assertParquetDereferencePushDown(withParquetDereferencePushDownEnabled(), query, tableName, expectedDeferencePushDowns);
    }

    private void assertParquetDereferencePushDown(String query, String tableName, Map<String, Subfield> expectedDeferencePushDowns, TupleDomain<String> domainPredicate,
            Set<String> predicateColumns, RowExpression remainingPredicate)
    {
        assertPlan(withParquetDereferencePushDownEnabled(), query,
                anyTree(tableScanParquetDeferencePushDowns(tableName, expectedDeferencePushDowns, domainPredicate, predicateColumns, remainingPredicate)));
    }

    private void assertParquetDereferencePushDown(Session session, String query, String tableName, Map<String, Subfield> expectedDeferencePushDowns)
    {
        assertPlan(session, query, anyTree(tableScanParquetDeferencePushDowns(tableName, expectedDeferencePushDowns)));
    }

    private Session pushdownFilterEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .build();
    }

    private Session pushdownFilterAndNestedColumnFilterEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .setCatalogSessionProperty(HIVE_CATALOG, RANGE_FILTERS_ON_SUBSCRIPTS_ENABLED, "true")
                .build();
    }

    private Session withParquetDereferencePushDownEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PUSHDOWN_DEREFERENCE_ENABLED, "true")
                .setCatalogSessionProperty(HIVE_CATALOG, PARQUET_DEREFERENCE_PUSHDOWN_ENABLED, "true")
                .build();
    }

    private Session partialAggregatePushdownEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(HIVE_CATALOG, PARTIAL_AGGREGATION_PUSHDOWN_ENABLED, "true")
                .setCatalogSessionProperty(HIVE_CATALOG, PARTIAL_AGGREGATION_PUSHDOWN_FOR_VARIABLE_LENGTH_DATATYPES_ENABLED, "true")
                .build();
    }

    private RowExpression constant(long value)
    {
        return new ConstantExpression(value, BIGINT);
    }

    private static Map<String, String> identityMap(String...values)
    {
        return Arrays.stream(values).collect(toImmutableMap(Functions.identity(), Functions.identity()));
    }

    private void assertTableLayout(Plan plan, String tableName, TupleDomain<Subfield> domainPredicate, RowExpression remainingPredicate, Set<String> predicateColumnNames)
    {
        TableScanNode tableScan = searchFrom(plan.getRoot())
                .where(node -> isTableScanNode(node, tableName))
                .findOnlyElement();

        assertTrue(tableScan.getTable().getLayout().isPresent());
        HiveTableLayoutHandle layoutHandle = (HiveTableLayoutHandle) tableScan.getTable().getLayout().get();

        assertEquals(layoutHandle.getPredicateColumns().keySet(), predicateColumnNames);
        assertEquals(layoutHandle.getDomainPredicate(), domainPredicate);
        assertEquals(layoutHandle.getRemainingPredicate(), remainingPredicate);
        assertEquals(layoutHandle.getRemainingPredicate(), remainingPredicate);
    }

    private void assertBucketFilter(Plan plan, String tableName, int readBucketCount, Set<Integer> bucketsToKeep)
    {
        TableScanNode tableScan = searchFrom(plan.getRoot())
                .where(node -> isTableScanNode(node, tableName))
                .findOnlyElement();

        assertTrue(tableScan.getTable().getLayout().isPresent());
        HiveTableLayoutHandle layoutHandle = (HiveTableLayoutHandle) tableScan.getTable().getLayout().get();

        assertTrue(layoutHandle.getBucketHandle().isPresent());
        assertTrue(layoutHandle.getBucketFilter().isPresent());
        assertEquals(layoutHandle.getBucketHandle().get().getReadBucketCount(), readBucketCount);
        assertEquals(layoutHandle.getBucketFilter().get().getBucketsToKeep(), bucketsToKeep);
    }

    private void assertNoBucketFilter(Plan plan, String tableName, int readBucketCount)
    {
        TableScanNode tableScan = searchFrom(plan.getRoot())
                .where(node -> isTableScanNode(node, tableName))
                .findOnlyElement();

        assertTrue(tableScan.getTable().getLayout().isPresent());
        HiveTableLayoutHandle layoutHandle = (HiveTableLayoutHandle) tableScan.getTable().getLayout().get();

        assertEquals(layoutHandle.getBucketHandle().get().getReadBucketCount(), readBucketCount);
        assertFalse(layoutHandle.getBucketFilter().isPresent());
    }

    private void assertNoAggregatedColumns(Plan plan, String tableName)
    {
        TableScanNode tableScan = searchFrom(plan.getRoot())
                .where(node -> isTableScanNode(node, tableName))
                .findOnlyElement();

        for (ColumnHandle columnHandle : tableScan.getAssignments().values()) {
            assertTrue(columnHandle instanceof HiveColumnHandle);
            HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) columnHandle;
            assertFalse(hiveColumnHandle.getColumnType() == HiveColumnHandle.ColumnType.AGGREGATED);
            assertFalse(hiveColumnHandle.getPartialAggregation().isPresent());
        }
    }

    private void assertRequestedColumnsInLayout(Plan plan, String tableName, Set<String> expectedRequestedColumns)
    {
        TableScanNode tableScan = searchFrom(plan.getRoot())
                .where(node -> isTableScanNode(node, tableName))
                .findOnlyElement();

        assertTrue(tableScan.getTable().getLayout().isPresent());
        HiveTableLayoutHandle layoutHandle = (HiveTableLayoutHandle) tableScan.getTable().getLayout().get();

        assertTrue(layoutHandle.getRequestedColumns().isPresent());
        Set<HiveColumnHandle> requestedColumns = layoutHandle.getRequestedColumns().get();

        List<String> actualRequestedColumns = new ArrayList<>();
        for (HiveColumnHandle column : requestedColumns) {
            if (!column.getRequiredSubfields().isEmpty()) {
                column.getRequiredSubfields().stream().map(Subfield::serialize).forEach(actualRequestedColumns::add);
            }
            else {
                actualRequestedColumns.add(column.getName());
            }
        }

        Set<String> requestedColumnsSet = ImmutableSet.copyOf(actualRequestedColumns);
        assertEquals(requestedColumnsSet.size(), actualRequestedColumns.size(), "There should be no duplicates in the requested column list");
        assertEquals(requestedColumnsSet, expectedRequestedColumns);
    }

    private static PlanMatchPattern tableScan(String tableName, TupleDomain<String> domainPredicate, RowExpression remainingPredicate, Set<String> predicateColumnNames)
    {
        return PlanMatchPattern.tableScan(tableName).with(new Matcher() {
            @Override
            public boolean shapeMatches(PlanNode node)
            {
                return node instanceof TableScanNode;
            }

            @Override
            public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
            {
                TableScanNode tableScan = (TableScanNode) node;

                Optional<ConnectorTableLayoutHandle> layout = tableScan.getTable().getLayout();

                if (!layout.isPresent()) {
                    return NO_MATCH;
                }

                HiveTableLayoutHandle layoutHandle = (HiveTableLayoutHandle) layout.get();

                if (!Objects.equals(layoutHandle.getPredicateColumns().keySet(), predicateColumnNames) ||
                        !Objects.equals(layoutHandle.getDomainPredicate(), domainPredicate.transform(Subfield::new)) ||
                        !Objects.equals(layoutHandle.getRemainingPredicate(), remainingPredicate)) {
                    return NO_MATCH;
                }

                return match();
            }
        });
    }

    private static final class HiveTableScanMatcher
            implements Matcher
    {
        private final Map<String, Set<Subfield>> requiredSubfields;

        private HiveTableScanMatcher(Map<String, Set<Subfield>> requiredSubfields)
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
                HiveColumnHandle hiveColumn = (HiveColumnHandle) column;
                String columnName = hiveColumn.getName();
                if (requiredSubfields.containsKey(columnName)) {
                    if (!requiredSubfields.get(columnName).equals(ImmutableSet.copyOf(hiveColumn.getRequiredSubfields()))) {
                        return NO_MATCH;
                    }
                }
                else {
                    if (!hiveColumn.getRequiredSubfields().isEmpty()) {
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

    private static final class HiveParquetDereferencePushdownMatcher
            implements Matcher
    {
        private final Map<String, Subfield> dereferenceColumns;
        private final TupleDomain<String> domainPredicate;
        private final Set<String> predicateColumns;
        private final RowExpression remainingPredicate;

        private HiveParquetDereferencePushdownMatcher(
                Map<String, Subfield> dereferenceColumns,
                TupleDomain<String> domainPredicate,
                Set<String> predicateColumns,
                RowExpression remainingPredicate)
        {
            this.dereferenceColumns = requireNonNull(dereferenceColumns, "dereferenceColumns is null");
            this.domainPredicate = requireNonNull(domainPredicate, "domainPredicate is null");
            this.predicateColumns = requireNonNull(predicateColumns, "predicateColumns is null");
            this.remainingPredicate = requireNonNull(remainingPredicate, "remainingPredicate is null");
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
                HiveColumnHandle hiveColumn = (HiveColumnHandle) column;
                String columnName = hiveColumn.getName();
                if (dereferenceColumns.containsKey(columnName)) {
                    if (hiveColumn.getColumnType() != SYNTHESIZED ||
                            hiveColumn.getRequiredSubfields().size() != 1 ||
                            !hiveColumn.getRequiredSubfields().get(0).equals(dereferenceColumns.get(columnName))) {
                        return NO_MATCH;
                    }
                    dereferenceColumns.remove(columnName);
                }
                else {
                    if (isPushedDownSubfield(hiveColumn)) {
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

            HiveTableLayoutHandle layoutHandle = (HiveTableLayoutHandle) layout.get();

            if (!Objects.equals(layoutHandle.getPredicateColumns().keySet(), predicateColumns) ||
                    !Objects.equals(layoutHandle.getDomainPredicate(), domainPredicate.transform(Subfield::new)) ||
                    !Objects.equals(layoutHandle.getRemainingPredicate(), remainingPredicate)) {
                return NO_MATCH;
            }

            return match();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("dereferenceColumns", dereferenceColumns)
                    .add("domainPredicate", domainPredicate)
                    .add("predicateColumns", predicateColumns)
                    .add("remainingPredicate", remainingPredicate)
                    .toString();
        }
    }

    private static Map<String, Subfield> nestedColumnMap(String... columns)
    {
        return Arrays.stream(columns).collect(Collectors.toMap(
                column -> pushdownColumnNameForSubfield(nestedColumn(column)),
                column -> nestedColumn(column)));
    }

    private static Subfield nestedColumn(String column)
    {
        return new Subfield(column);
    }
}
