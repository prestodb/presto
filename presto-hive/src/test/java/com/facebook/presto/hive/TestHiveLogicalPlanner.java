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
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static com.facebook.presto.hive.HiveSessionProperties.COLLECT_COLUMN_STATISTICS_ON_WRITE;
import static com.facebook.presto.hive.HiveSessionProperties.NESTED_COLUMNS_FILTER_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.hive.TestHiveIntegrationSmokeTest.assertRemoteExchangesCount;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveLogicalPlanner
        extends AbstractTestQueryFramework
{
    public TestHiveLogicalPlanner()
    {
        super(() -> createQueryRunner(ImmutableList.of(ORDERS, LINE_ITEM), ImmutableMap.of("experimental.pushdown-subfields-enabled", "true"), Optional.empty()));
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
                plan -> assertTableLayout(plan, "lineitem", withColumnDomains(ImmutableMap.of(new Subfield("partkey", ImmutableList.of()), Domain.singleValue(BIGINT, 10L))), TRUE_CONSTANT, ImmutableSet.of("partkey")));

        assertPlan(pushdownFilterEnabled, "SELECT partkey, linenumber FROM lineitem WHERE partkey = 10",
                output(exchange(
                        strictTableScan("lineitem", identityMap("partkey", "linenumber")))),
                plan -> assertTableLayout(plan, "lineitem", withColumnDomains(ImmutableMap.of(new Subfield("partkey", ImmutableList.of()), Domain.singleValue(BIGINT, 10L))), TRUE_CONSTANT, ImmutableSet.of("partkey")));

        // Only remaining predicate
        assertPlan("SELECT linenumber FROM lineitem WHERE mod(orderkey, 2) = 1",
                output(exchange(project(
                        filter("mod(orderkey, 2) = 1",
                                strictTableScan("lineitem", identityMap("linenumber", "orderkey")))))));

        FunctionManager functionManager = getQueryRunner().getMetadata().getFunctionManager();
        FunctionResolution functionResolution = new FunctionResolution(functionManager);
        RowExpression remainingPredicate = new CallExpression(EQUAL.name(),
                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                BOOLEAN,
                ImmutableList.of(
                        new CallExpression("mod",
                                functionManager.lookupFunction("mod", fromTypes(BIGINT, BIGINT)),
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
                plan -> assertTableLayout(plan, "lineitem", withColumnDomains(ImmutableMap.of(new Subfield("partkey", ImmutableList.of()), Domain.singleValue(BIGINT, 10L))), remainingPredicate, ImmutableSet.of("partkey", "orderkey")));

        assertPlan(pushdownFilterEnabled, "SELECT partkey, orderkey, linenumber FROM lineitem WHERE partkey = 10 AND mod(orderkey, 2) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("partkey", "orderkey", "linenumber")))),
                plan -> assertTableLayout(plan, "lineitem", withColumnDomains(ImmutableMap.of(new Subfield("partkey", ImmutableList.of()), Domain.singleValue(BIGINT, 10L))), remainingPredicate, ImmutableSet.of("partkey", "orderkey")));
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
                        ImmutableMap.of(new Subfield("a[1]"), Domain.singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields where a[1 + 1] = 1",
                        ImmutableMap.of(new Subfield("a[2]"), Domain.singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT *  FROM test_pushdown_filter_on_subfields WHERE b['foo'] = 1",
                        ImmutableMap.of(new Subfield("b[\"foo\"]"), Domain.singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE b[concat('f','o', 'o')] = 1",
                        ImmutableMap.of(new Subfield("b[\"foo\"]"), Domain.singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE c.a = 1",
                        ImmutableMap.of(new Subfield("c.a"), Domain.singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE c.b.x = 1",
                        ImmutableMap.of(new Subfield("c.b.x"), Domain.singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE c.c[5] = 1",
                        ImmutableMap.of(new Subfield("c.c[5]"), Domain.singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE c.d[5] = 1",
                        ImmutableMap.of(new Subfield("c.d[5]"), Domain.singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE c.e[concat('f', 'o', 'o')] = 1",
                        ImmutableMap.of(new Subfield("c.e[\"foo\"]"), Domain.singleValue(BIGINT, 1L)));

        assertPushdownFilterOnSubfields("SELECT * FROM test_pushdown_filter_on_subfields WHERE c.e['foo'] = 1",
                        ImmutableMap.of(new Subfield("c.e[\"foo\"]"), Domain.singleValue(BIGINT, 1L)));

        assertUpdate("DROP TABLE test_pushdown_filter_on_subfields");
    }

    @Test
    public void testPushdownArraySubscripts()
    {
        assertUpdate("CREATE TABLE test_pushdown_array_subscripts(id bigint, a array(bigint), b array(array(varchar)))");

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
        assertUpdate("CREATE TABLE test_pushdown_map_subscripts(id bigint, a map(bigint, bigint), b map(bigint, map(bigint, varchar)), c map(varchar, bigint))");

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

        // Unnest
        assertPushdownSubfields("SELECT t.a, t.d.d1, x.a FROM test_pushdown_struct_subfields CROSS JOIN UNNEST(y) as t(a, b, c, d)", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a"), "y", toSubfields("y[*].a", "y[*].d.d1")));

        assertPushdownSubfields("SELECT t.*, x.a FROM test_pushdown_struct_subfields CROSS JOIN UNNEST(y) as t(a, b, c, d)", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a"), "y", toSubfields("y[*].a", "y[*].b", "y[*].c", "y[*].d")));

        assertPushdownSubfields("SELECT id, x.a FROM test_pushdown_struct_subfields CROSS JOIN UNNEST(y) as t(a, b, c, d)", "test_pushdown_struct_subfields",
                ImmutableMap.of("x", toSubfields("x.a")));

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
                "d row(d1 bigint, d2 array(bigint), d3 map(bigint, bigint), d4 row(x double, y double)))");

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
        }
        finally {
            assertUpdate("DROP TABLE orders_ex");
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
                .setCatalogSessionProperty(HIVE_CATALOG, NESTED_COLUMNS_FILTER_ENABLED, "true")
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
}
