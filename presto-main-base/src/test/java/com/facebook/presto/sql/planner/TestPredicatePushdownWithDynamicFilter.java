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
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static com.facebook.presto.SystemSessionProperties.GENERATE_DOMAIN_FILTERS;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestPredicatePushdownWithDynamicFilter
        extends TestPredicatePushdown
{
    TestPredicatePushdownWithDynamicFilter()
    {
        super(ImmutableMap.of(ENABLE_DYNAMIC_FILTERING, "true"));
    }

    @Test
    @Override
    public void testNonStraddlingJoinExpression()
    {
        assertPlan(
                "SELECT * FROM orders JOIN lineitem ON orders.orderkey = lineitem.orderkey AND cast(lineitem.linenumber AS varchar) = '2'",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("LINEITEM_OK", "ORDERS_OK")),
                                anyTree(
                                        node(
                                                FilterNode.class,
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINEITEM_OK", "orderkey",
                                                        "LINEITEM_LINENUMBER", "linenumber")))),
                                anyTree(tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))));
    }

    @Override
    @Test
    public void testEqualPredicateFromSourceSidePropagatesToFilterSideOfSemiJoin()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders) AND orderkey = 2)",
                anyTree(
                        project(
                                project(
                                        node(
                                                FilterNode.class,
                                                tableScan(
                                                        "lineitem",
                                                        ImmutableMap.of("LINE_ORDER_KEY", "orderkey", "LINE_QUANTITY", "quantity"))))),
                        node(
                                ExchangeNode.class,
                                project(
                                        project(
                                                node(
                                                        FilterNode.class,
                                                        tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey"))))))));
    }

    @Override
    @Test
    public void testEqualsPredicateFromFilterSidePropagatesToSourceSideOfSemiJoin()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey = 2))",
                anyTree(
                        semiJoin(
                                "LINE_ORDER_KEY",
                                "expr_6",
                                "SEMI_JOIN_RESULT",
                                project(
                                        project(
                                                node(
                                                        FilterNode.class,
                                                        tableScan(
                                                                "lineitem",
                                                                ImmutableMap.of("LINE_ORDER_KEY", "orderkey", "LINE_QUANTITY", "quantity"))))),
                                node(
                                        ExchangeNode.class,
                                        project(
                                                project(
                                                        ImmutableMap.of("expr_6", expression("2")),
                                                        node(
                                                                FilterNode.class,
                                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))))));
    }

    @Override
    @Test
    public void testNonDeterministicPredicateDoesNotPropagateFromFilteringSideToSourceSideOfSemiJoin()
    {
        assertPlan("SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey = random(5))",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                project(
                                        node(
                                                FilterNode.class,
                                                tableScan(
                                                        "lineitem",
                                                        ImmutableMap.of("LINE_ORDER_KEY", "orderkey")))),
                                node(
                                        ExchangeNode.class,
                                        project(filter(
                                                "ORDERS_ORDER_KEY = CAST(random(5) AS bigint)",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey"))))))));
    }

    @Override
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
                                // Join order with lineitem is flipped when dynamic filtering is applied as compared
                                // to super#testDomainFiltersAppliedOnSemiJoinOutputFilterHaveNoImpact
                                anyTree(tableScan("lineitem")),
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
                                                                        tableScan("part", ImmutableMap.of("P_PART", "partkey"))))))))));
    }
}
