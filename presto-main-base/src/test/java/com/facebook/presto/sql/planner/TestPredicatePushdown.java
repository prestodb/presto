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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.sql.InMemoryExpressionOptimizerProvider;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PredicatePushDown;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.GENERATE_DOMAIN_FILTERS;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static java.util.Collections.emptyList;

public class TestPredicatePushdown
        extends BasePlanTest
{
    public TestPredicatePushdown()
    {
    }

    public TestPredicatePushdown(Map<String, String> sessionProperties)
    {
        super(sessionProperties);
    }

    @Test
    public void testNonStraddlingJoinExpression()
    {
        assertPlan("SELECT * FROM orders JOIN lineitem ON orders.orderkey = lineitem.orderkey AND cast(lineitem.linenumber AS varchar) = '2'",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("LINEITEM_OK", "ORDERS_OK")),
                                anyTree(
                                        filter("cast('2' as varchar) = cast(LINEITEM_LINENUMBER as varchar)",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINEITEM_OK", "orderkey",
                                                        "LINEITEM_LINENUMBER", "linenumber")))),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))));
    }

    @Test
    public void testPushDownToLhsOfSemiJoin()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders)) " +
                        "WHERE linenumber = 2",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                anyTree(
                                        filter("LINE_NUMBER = 2",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_NUMBER", "linenumber",
                                                        "LINE_QUANTITY", "quantity")))),
                                anyTree(tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey"))))));
    }

    @Test
    public void testNonDeterministicPredicatePropagatesOnlyToSourceSideOfSemiJoin()
    {
        assertPlan("SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders) AND orderkey = random(5)",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                anyTree(
                                        filter("LINE_ORDER_KEY = CAST(random(5) AS bigint)",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey")))),
                                node(ExchangeNode.class, // NO filter here
                                        project(
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));

        assertPlan("SELECT * FROM lineitem WHERE orderkey NOT IN (SELECT orderkey FROM orders) AND orderkey = random(5)",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                anyTree(
                                        filter("LINE_ORDER_KEY = CAST(random(5) AS bigint)",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey")))),
                                anyTree(
                                        project(// NO filter here
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testNonDeterministicPredicateDoesNotPropagateFromFilteringSideToSourceSideOfSemiJoin()
    {
        assertPlan("SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey = random(5))",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                // NO filter here
                                project(
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey"))),
                                node(ExchangeNode.class,
                                        project(
                                                filter("ORDERS_ORDER_KEY = CAST(random(5) AS bigint)",
                                                        tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey"))))))));
    }

    @Test
    public void testGreaterPredicateFromFilterSidePropagatesToSourceSideOfSemiJoin()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey > 2))",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                anyTree(
                                        filter("LINE_ORDER_KEY > BIGINT '2'",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_QUANTITY", "quantity")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY > BIGINT '2'",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testEqualsPredicateFromFilterSidePropagatesToSourceSideOfSemiJoin()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey = 2))",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "expr_6", "SEMI_JOIN_RESULT",
                                anyTree(
                                        filter("LINE_ORDER_KEY = BIGINT '2'",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_QUANTITY", "quantity")))),
                                anyTree(
                                        project(
                                                ImmutableMap.of("expr_6", expression("2")),
                                                filter("ORDERS_ORDER_KEY = BIGINT '2'",
                                                        tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey"))))))));
    }

    @Test
    public void testDomainPredicateFromFilterSidePropagatesToSourceSideOfSemiJoin()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE (orderkey = 2 and comment = 'abc') " +
                        "OR (orderkey = 3 and comment = 'def') OR (orderkey = 4 and comment = 'ghi')))",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                anyTree(
                                        filter("LINE_ORDER_KEY IN( BIGINT'2', BIGINT'3', BIGINT'4')",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_QUANTITY", "quantity")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY IN (BIGINT '2',BIGINT '3',BIGINT '4') AND ORDERS_COMMENT IN ('abc','def','ghi') " +
                                                        "AND (((((ORDERS_ORDER_KEY) = (BIGINT'2')) AND ((ORDERS_COMMENT) = (VARCHAR'abc'))) " +
                                                        "OR (((ORDERS_ORDER_KEY) = (BIGINT'3')) AND ((ORDERS_COMMENT) = (VARCHAR'def')))) " +
                                                        "OR (((ORDERS_ORDER_KEY) = (BIGINT'4')) AND ((ORDERS_COMMENT) = (VARCHAR'ghi'))))",
                                                tableScan("orders", ImmutableMap.of(
                                                        "ORDERS_ORDER_KEY", "orderkey",
                                                        "ORDERS_COMMENT", "comment")))))));
    }

    @Test
    public void testPredicateFromFilterSideNotPropagatesToSourceSideOfSemiJoinIfNotIn()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey NOT IN (SELECT orderkey FROM orders WHERE orderkey > 2))",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                // There should be no Filter above table scan, because we don't know whether SemiJoin's filtering source is empty.
                                // And filter would filter out NULLs from source side which is not what we need then.
                                project(
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey",
                                                "LINE_QUANTITY", "quantity"))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY > BIGINT '2'",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testGreaterPredicateFromSourceSidePropagatesToFilterSideOfSemiJoin()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders) AND orderkey > 2)",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                anyTree(
                                        filter("LINE_ORDER_KEY > BIGINT '2'",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_QUANTITY", "quantity")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY > BIGINT '2'",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testEqualPredicateFromSourceSidePropagatesToFilterSideOfSemiJoin()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders) AND orderkey = 2)",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                anyTree(
                                        filter("LINE_ORDER_KEY = BIGINT '2'",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_QUANTITY", "quantity")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY = BIGINT '2'",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testPredicateFromSourceSideNotPropagatesToFilterSideOfSemiJoinIfNotIn()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey NOT IN (SELECT orderkey FROM orders) AND orderkey > 2)",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                project(
                                        filter("LINE_ORDER_KEY > BIGINT '2'",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_QUANTITY", "quantity")))),
                                node(ExchangeNode.class, // NO filter here
                                        project(
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testPredicateFromFilterSideNotPropagatesToSourceSideOfSemiJoinUsedInProjection()
    {
        assertPlan("SELECT orderkey IN (SELECT orderkey FROM orders WHERE orderkey > 2) FROM lineitem",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                // NO filter here
                                project(
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey"))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY > BIGINT '2'",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testFilteredSelectFromPartitionedTable()
    {
        // use all optimizers, including AddExchanges
        List<PlanOptimizer> allOptimizers = getQueryRunner().getPlanOptimizers(false);

        assertPlan(
                "SELECT DISTINCT orderstatus FROM orders",
                // TODO this could be optimized to VALUES with values from partitions
                anyTree(
                        tableScan("orders")),
                allOptimizers);

        assertPlan(
                "SELECT orderstatus FROM orders WHERE orderstatus = 'O'",
                // predicate matches exactly single partition, no FilterNode needed
                output(
                        exchange(
                                project(
                                        ImmutableMap.of("expr_2", expression("'O'")),
                                        tableScan("orders")))),
                allOptimizers);

        assertPlan(
                "SELECT orderstatus FROM orders WHERE orderstatus = 'no_such_partition_value'",
                output(
                        values("orderstatus")),
                allOptimizers);
    }

    @Test
    public void testPredicatePushDownThroughMarkDistinct()
    {
        assertPlan(
                "SELECT (SELECT a FROM (VALUES 1, 2, 3) t(a) WHERE a = b) FROM (VALUES 0, 1) p(b) WHERE b = 1",
                // TODO this could be optimized to VALUES with values from partitions
                anyTree(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("A", "B")),
                                project(assignUniqueId("unique", project(ImmutableMap.of("A", expression("1")), filter("A = 1", values("A"))))),
                                project(project(ImmutableMap.of("B", expression("1")), filter("1 = B", values("B")))))));
    }

    @Test
    public void testPredicatePushDownOverProjection()
    {
        // Non-singletons should not be pushed down
        assertPlan(
                "WITH t AS (SELECT orderkey * 2 x FROM orders) " +
                        "SELECT * FROM t WHERE x + x > 1",
                anyTree(
                        filter("((expr + expr) > BIGINT '1')",
                                project(ImmutableMap.of("expr", expression("orderkey * BIGINT '2'")),
                                        tableScan("orders", ImmutableMap.of("ORDERKEY", "orderkey"))))));

        // constant non-singleton should be pushed down
        assertPlan(
                "with t AS (SELECT orderkey * 2 x, 1 y FROM orders) " +
                        "SELECT * FROM t WHERE x + y + y >1",
                anyTree(
                        project(
                                filter("(((orderkey * BIGINT '2') + BIGINT '1') + BIGINT '1') > BIGINT '1'",
                                        tableScan("orders", ImmutableMap.of(
                                                "orderkey", "orderkey"))))));

        // singletons should be pushed down
        assertPlan(
                "WITH t AS (SELECT orderkey * 2 x FROM orders) " +
                        "SELECT * FROM t WHERE x > 1",
                anyTree(
                        project(
                                filter("(orderkey * BIGINT '2') > BIGINT '1'",
                                        tableScan("orders", ImmutableMap.of(
                                                "orderkey", "orderkey"))))));

        // composite singletons should be pushed down
        assertPlan(
                "with t AS (SELECT orderkey * 2 x, orderkey y FROM orders) " +
                        "SELECT * FROM t WHERE x + y > 1",
                anyTree(
                        project(
                                filter("((orderkey * BIGINT '2') + orderkey) > BIGINT '1'",
                                        tableScan("orders", ImmutableMap.of(
                                                "orderkey", "orderkey"))))));

        // Identities should be pushed down
        assertPlan(
                "WITH t AS (SELECT orderkey x FROM orders) " +
                        "SELECT * FROM t WHERE x >1",
                anyTree(
                        filter("orderkey > BIGINT '1'",
                                tableScan("orders", ImmutableMap.of(
                                        "orderkey", "orderkey")))));

        // Non-deterministic predicate should not be pushed down
        assertPlan(
                "WITH t AS (SELECT rand() * orderkey x FROM orders) " +
                        "SELECT * FROM t WHERE x > 5000",
                anyTree(
                        filter("expr > 5E3",
                                project(ImmutableMap.of("expr", expression("rand() * CAST(orderkey AS double)")),
                                        tableScan("orders", ImmutableMap.of(
                                                "ORDERKEY", "orderkey"))))));
    }

    @Test
    public void testConjunctsOrder()
    {
        assertPlan(
                "select partkey " +
                        "from (" +
                        "  select" +
                        "    partkey," +
                        "    100/(size-1) x" +
                        "  from part" +
                        "  where size <> 1" +
                        ") " +
                        "where x = 2",
                anyTree(
                        // Order matters: size<>1 should be before 100/(size-1)=2.
                        // In this particular example, reversing the order leads to div-by-zero error.
                        filter("size <> 1 AND 100/(size - 1) = 2",
                                tableScan("part", ImmutableMap.of(
                                        "partkey", "partkey",
                                        "size", "size")))));
    }

    @Test
    public void testPredicateOnPartitionSymbolsPushedThroughWindow()
    {
        PlanMatchPattern tableScan = tableScan(
                "orders",
                ImmutableMap.of(
                        "CUST_KEY", "custkey",
                        "ORDER_KEY", "orderkey"));
        assertPlan(
                "SELECT * FROM (" +
                        "SELECT custkey, orderkey, rank() OVER (PARTITION BY custkey  ORDER BY orderdate ASC)" +
                        "FROM orders" +
                        ") WHERE custkey = 0 AND orderkey > 0",
                anyTree(
                        filter("ORDER_KEY > BIGINT '0'",
                                anyTree(
                                        node(WindowNode.class,
                                                anyTree(
                                                        filter("CUST_KEY = BIGINT '0'",
                                                                tableScan)))))));
    }

    @Test
    public void testPredicateOnNonDeterministicSymbolsPushedDown()
    {
        assertPlan(
                "SELECT * FROM (" +
                        "SELECT random_column, orderkey, rank() OVER (PARTITION BY random_column  ORDER BY orderdate ASC)" +
                        "FROM (select round(custkey*rand()) random_column, * from orders) " +
                        ") WHERE random_column > 100",
                anyTree(
                        node(WindowNode.class,
                                anyTree(
                                        filter("\"ROUND\" > 1E2",
                                                project(ImmutableMap.of("ROUND", expression("round(CAST(CUST_KEY AS double) * rand())")),
                                                        tableScan(
                                                                "orders",
                                                                ImmutableMap.of("CUST_KEY", "custkey"))))))));
    }

    @Test
    public void testNonDeterministicPredicateNotPushedDown()
    {
        assertPlan(
                "SELECT * FROM (" +
                        "SELECT custkey, orderkey, rank() OVER (PARTITION BY custkey  ORDER BY orderdate ASC)" +
                        "FROM orders" +
                        ") WHERE custkey > 100*rand()",
                anyTree(
                        filter("CAST(\"CUST_KEY\" AS double) > (1E2 * \"rand\"())",
                                anyTree(
                                        node(WindowNode.class,
                                                anyTree(
                                                        tableScan(
                                                                "orders",
                                                                ImmutableMap.of("CUST_KEY", "custkey"))))))));
    }

    @Test
    public void testNoPushdownWithTry()
    {
        assertPlan(
                "SELECT * FROM (" +
                        "SELECT custkey, orderkey, rank() OVER (PARTITION BY custkey  ORDER BY orderdate ASC)" +
                        "FROM orders" +
                        ") WHERE try(custkey) = 1",
                anyTree(node(FilterNode.class, anyTree(node(WindowNode.class, anyTree(tableScan("orders")))))));
    }

    @Test
    public void testPredicatePushDownCanReduceInnerToCrossJoin()
    {
        RuleTester tester = new RuleTester();
        tester.assertThat(new PredicatePushDown(tester.getMetadata(), tester.getSqlParser(), new InMemoryExpressionOptimizerProvider(tester.getMetadata()), false))
                .on(p ->
                        p.join(INNER,
                                p.filter(p.comparison(OperatorType.EQUAL, p.variable("a1"), constant(1L, INTEGER)),
                                        p.values(p.variable("a1"))),
                                p.values(p.variable("b1")),
                                ImmutableList.of(new EquiJoinClause(p.variable("a1"), p.variable("b1"))),
                                ImmutableList.of(p.variable("a1")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(PARTITIONED),
                                ImmutableMap.of()))
                .matches(
                        project(
                                ImmutableMap.of("a1", expression("a1")),
                                join(
                                        INNER,
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.of(REPLICATED),
                                        project(
                                                filter("a1=1",
                                                        values("a1"))),
                                        project(
                                                filter("1=b1",
                                                        values("b1"))))));
    }

    @Test
    public void testPredicatePushdownDoesNotAddProjectsBetweenJoinNodes()
    {
        RuleTester tester = new RuleTester();
        PredicatePushDown predicatePushDownOptimizer = new PredicatePushDown(tester.getMetadata(), tester.getSqlParser(), new InMemoryExpressionOptimizerProvider(tester.getMetadata()), false);

        tester.assertThat(predicatePushDownOptimizer)
                .on("SELECT 1 " +
                        "FROM supplier s " +
                        "    INNER JOIN lineitem l on s.suppkey = l.suppkey " +
                        "    INNER JOIN nation n on s.nationkey = n.nationkey " +
                        "WHERE s.phone = '424242' " +
                        "    AND n.name = 'mars' " +
                        "    AND l.comment = 'lorem ipsum' ")
                .matches(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("S_NATIONKEY", "N_NATIONKEY")),
                                        // No identity projection is added above this JoinNode since it's not needed
                                        // This JoinNode is therefore 'visible' for join-reordering
                                        join(INNER,
                                                ImmutableList.of(equiJoinClause("S_SUPPKEY", "L_SUPPKEY")),
                                                project(
                                                        filter("S_PHONE = '424242'",
                                                                tableScan("supplier",
                                                                        ImmutableMap.of(
                                                                                "S_SUPPKEY", "suppkey",
                                                                                "S_PHONE", "phone",
                                                                                "S_NATIONKEY", "nationkey")))),
                                                project(
                                                        filter("L_COMMENT = 'lorem ipsum'",
                                                                tableScan("lineitem",
                                                                        ImmutableMap.of(
                                                                                "L_SUPPKEY", "suppkey",
                                                                                "L_COMMENT", "comment"))))),
                                        project(
                                                filter("N_NAME = 'mars'",
                                                        tableScan("nation",
                                                                ImmutableMap.of(
                                                                        "N_NATIONKEY", "nationkey",
                                                                        "N_NAME", "name")))))));

        tester.assertThat(predicatePushDownOptimizer)
                .on("SELECT 1 " +
                        "FROM supplier s " +
                        "    INNER JOIN lineitem l on s.suppkey = l.suppkey " +
                        "    INNER JOIN nation n on s.nationkey + l.partkey = n.nationkey " +
                        "WHERE s.phone = '424242' " +
                        "    AND n.name = 'mars' " +
                        "    AND l.comment = 'lorem ipsum' ")
                .matches(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("expr", "N_NATIONKEY")),
                                        // We need this non-identity ProjectNode to build a new assignment for use in the JOIN with nation
                                        project(ImmutableMap.of("expr", expression("S_NATIONKEY + L_PARTKEY")),
                                                join(INNER,
                                                        ImmutableList.of(equiJoinClause("S_SUPPKEY", "L_SUPPKEY")),
                                                        project(
                                                                filter("S_PHONE = '424242'",
                                                                        tableScan("supplier",
                                                                                ImmutableMap.of(
                                                                                        "S_SUPPKEY", "suppkey",
                                                                                        "S_PHONE", "phone",
                                                                                        "S_NATIONKEY", "nationkey")))),
                                                        project(
                                                                filter("L_COMMENT = 'lorem ipsum'",
                                                                        tableScan("lineitem",
                                                                                ImmutableMap.of(
                                                                                        "L_SUPPKEY", "suppkey",
                                                                                        "L_PARTKEY", "partkey",
                                                                                        "L_COMMENT", "comment")))))),
                                        project(
                                                filter("N_NAME = 'mars'",
                                                        tableScan("nation",
                                                                ImmutableMap.of(
                                                                        "N_NATIONKEY", "nationkey",
                                                                        "N_NAME", "name")))))));
    }

    @Test
    public void testDomainFiltersCanBeInferredForLargeDisjunctiveFilters()
    {
        RuleTester tester = new RuleTester(emptyList(), ImmutableMap.of(GENERATE_DOMAIN_FILTERS, "true"));
        PredicatePushDown predicatePushDownOptimizer = new PredicatePushDown(tester.getMetadata(), tester.getSqlParser(), new InMemoryExpressionOptimizerProvider(tester.getMetadata()), false);

        // For Inner Join
        tester.assertThat(predicatePushDownOptimizer)
                // Query has more than 2 disjunctions in its predicate; SimplifyRowExpressions will not convert this into a CNF form
                // Because of this, we do not get predicates on 's.phone' and 'l.orderkey' pushed down
                // However when 'generate_domain_filters=true', these predicates are generated and pushed down
                .on("select 1 FROM supplier s INNER JOIN lineitem l on s.suppkey = l.suppkey " +
                        "WHERE (s.phone = '424242' AND l.orderkey = 5 ) " +
                        "OR (s.phone = '242424' AND l.orderkey = 10) " +
                        "OR (s.phone = '32424' AND l.orderkey = 150)")
                .matches(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("S_SUPPKEY", "L_SUPPKEY")),
                                        Optional.of("(S_PHONE = '424242' AND L_ORDERKEY = 5) OR (S_PHONE = '242424' AND L_ORDERKEY = 10) OR (S_PHONE = '32424' AND L_ORDERKEY = 150)"),
                                        project(
                                                filter("S_PHONE IN ('242424','32424','424242')",
                                                        tableScan("supplier",
                                                                ImmutableMap.of(
                                                                        "S_SUPPKEY", "suppkey",
                                                                        "S_PHONE", "phone")))),
                                        project(
                                                filter("L_ORDERKEY IN (5,10,150)",
                                                        tableScan("lineitem",
                                                                ImmutableMap.of(
                                                                        "L_SUPPKEY", "suppkey",
                                                                        "L_ORDERKEY", "orderkey")))))));

        // For an outer join, if an inner-side predicate is not pushing down an ISNULL; we can pushdown the full inner-side range predicate
        tester.assertThat(predicatePushDownOptimizer)
                .on("select 1 FROM supplier s LEFT JOIN lineitem l on s.suppkey = l.suppkey " +
                        "WHERE (s.phone = '424242' AND l.orderkey = 5 ) " +
                        "OR (s.phone = '242424' AND l.orderkey = 10) " +
                        "OR (s.phone = '32424' AND l.orderkey = 150)")
                .matches(
                        anyTree(
                                filter("(S_PHONE = '424242' AND L_ORDERKEY = 5) OR (S_PHONE = '242424' AND L_ORDERKEY = 10) OR (S_PHONE = '32424' AND L_ORDERKEY = 150)",
                                        join(LEFT,
                                                ImmutableList.of(equiJoinClause("S_SUPPKEY", "L_SUPPKEY")),
                                                project(
                                                        filter("S_PHONE IN ('242424','32424','424242')",
                                                                tableScan("supplier",
                                                                        ImmutableMap.of(
                                                                                "S_SUPPKEY", "suppkey",
                                                                                "S_PHONE", "phone")))),
                                                project(
                                                        filter("L_ORDERKEY IN (5,10,150)",
                                                                tableScan("lineitem",
                                                                        ImmutableMap.of(
                                                                                "L_SUPPKEY", "suppkey",
                                                                                "L_ORDERKEY", "orderkey"))))))));

        // For an outer join, if an inner-side predicate *is* pushing down an ISNULL; we cannot push any inner side predicates
        tester.assertThat(predicatePushDownOptimizer)
                .on("select 1 FROM supplier s LEFT JOIN lineitem l on s.suppkey = l.suppkey " +
                        "WHERE (s.phone = '424242' AND l.orderkey = 5 ) " +
                        "OR (s.phone = '242424' AND l.orderkey = 10) " +
                        "OR (s.phone = '32424' AND l.orderkey IS NULL)")
                .matches(
                        anyTree(
                                filter("(S_PHONE = '424242' AND L_ORDERKEY = 5) OR (S_PHONE = '242424' AND L_ORDERKEY = 10) OR (S_PHONE = '32424' AND L_ORDERKEY IS NULL)",
                                        join(LEFT,
                                                ImmutableList.of(equiJoinClause("S_SUPPKEY", "L_SUPPKEY")),
                                                project(
                                                        filter("S_PHONE IN ('242424','32424','424242')",
                                                                tableScan("supplier",
                                                                        ImmutableMap.of(
                                                                                "S_SUPPKEY", "suppkey",
                                                                                "S_PHONE", "phone")))),
                                                project(
                                                        tableScan("lineitem",
                                                                ImmutableMap.of(
                                                                        "L_SUPPKEY", "suppkey",
                                                                        "L_ORDERKEY", "orderkey")))))));
    }

    @Test
    public void testDomainFiltersAppliedOnSemiJoinOutputFilterHaveNoImpact()
    {
        // No impact on SemiJoin
        Session generateDomainFilterSession = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(GENERATE_DOMAIN_FILTERS, "true")
                .build();

        // Query is subquery of TPCH Q20
        assertPlan(" SELECT  " +
                        "      ps.suppkey  " +
                        "    FROM  " +
                        "      partsupp ps " +
                        "    WHERE  " +
                        "      ps.partkey IN ( " +
                        "        SELECT  " +
                        "          p.partkey  " +
                        "        FROM  " +
                        "          part p " +
                        "        WHERE  " +
                        "          p.name like 'forest%' " +
                        "      )  " +
                        "      AND ps.availqty > ( " +
                        "        SELECT  " +
                        "          0.5*sum(l.quantity)  " +
                        "        FROM  " +
                        "          lineitem l " +
                        "        WHERE  " +
                        "          l.partkey = ps.partkey  " +
                        "          AND l.suppkey = ps.suppkey  " +
                        "          AND l.shipdate >= date('1994-01-01') " +
                        "          AND l.shipdate < date('1994-01-01') + interval '1' YEAR " +
                        "      )",
                generateDomainFilterSession,
                output(
                        join(
                                anyTree(
                                        // During filter pushdown of the boolean predicate SEMI_JOIN_RESULT through the InnerJoin, we produce the
                                        // redundant domain filter (SEMI_JOIN_RESULT = BOOLEAN'true'). This however does not have any impact on how
                                        // this filter is pushed down through the SemiJoin
                                        filter("SEMI_JOIN_RESULT AND (SEMI_JOIN_RESULT = BOOLEAN'true')",
                                                anyTree(
                                                        semiJoin("PS_PARTKEY", "P_PART", "SEMI_JOIN_RESULT",
                                                                anyTree(
                                                                        tableScan("partsupp", ImmutableMap.of("PS_PARTKEY", "partkey"))),
                                                                anyTree(
                                                                        tableScan("part", ImmutableMap.of("P_PART", "partkey"))))))),
                                anyTree(tableScan("lineitem")))));
    }
}
