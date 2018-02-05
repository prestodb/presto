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

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestPredicatePushdown
        extends BasePlanTest
{
    @Test
    public void testNonStraddlingJoinExpression()
    {
        assertPlan("SELECT * FROM orders JOIN lineitem ON orders.orderkey = lineitem.orderkey AND cast(lineitem.linenumber AS varchar) = '2'",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                any(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                anyTree(
                                        filter("cast(LINEITEM_LINENUMBER as varchar) = cast('2' as varchar)",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINEITEM_OK", "orderkey",
                                                        "LINEITEM_LINENUMBER", "linenumber")))))));
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
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                anyTree(
                                        filter("LINE_ORDER_KEY = BIGINT '2' AND BIGINT '2' = LINE_ORDER_KEY", // TODO this should be simplified
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_QUANTITY", "quantity")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY = BIGINT '2' AND BIGINT '2' = ORDERS_ORDER_KEY", // TODO this should be simplified
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
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
                                        filter("LINE_ORDER_KEY = BIGINT '2' AND BIGINT '2' = LINE_ORDER_KEY", // TODO this should be simplified
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_QUANTITY", "quantity")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY = BIGINT '2' AND BIGINT '2' = ORDERS_ORDER_KEY", // TODO this should be simplified
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
                                tableScan("orders"))),
                allOptimizers);

        assertPlan(
                "SELECT orderstatus FROM orders WHERE orderstatus = 'no_such_partition_value'",
                output(
                        values("orderstatus")),
                allOptimizers);
    }
}
