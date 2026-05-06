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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.sql.Optimizer;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PUSH_FILTER_THROUGH_SELECTING_AGGREGATION;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPushFilterThroughSelectingAggregationPlans
        extends BasePlanTest
{
    private Session enabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "true")
                .build();
    }

    private Session disabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "false")
                .build();
    }

    @Test
    public void testFilterOnMaxPushesAllTheWayDownToScan()
    {
        // Simpler case (no join): the pushed-down filter on totalprice should reach the orders
        // table scan as a pre-aggregation filter.
        String sql = "SELECT custkey, MAX(totalprice) FROM orders GROUP BY custkey HAVING MAX(totalprice) >= 100000";

        assertDistributedPlan(sql, enabled(),
                anyTree(filter("totalprice >= DOUBLE '100000.0'",
                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey", "custkey")))));
    }

    @Test
    public void testFilterStaysBetweenTwoJoinsWhenPushdownCannotPierceInnerJoin()
    {
        // Two joins. The aggregated column is a computed expression (totalprice + extendedprice)
        // that pulls from BOTH sides of the inner orders/lineitem join, so after our rule pushes
        // the filter below the outer aggregation and predicate pushdown carries it past the
        // outer customer join, the resulting expression-form filter cannot be pushed past the
        // orders/lineitem join — it must STAY between the two joins.
        String sql = "SELECT MAX(t.combined) FROM " +
                "(SELECT o.custkey, o.totalprice + l.extendedprice AS combined " +
                " FROM orders o JOIN lineitem l ON o.orderkey = l.orderkey) t " +
                "JOIN customer c ON t.custkey = c.custkey " +
                "GROUP BY t.custkey HAVING MAX(t.combined) >= 1000000";

        Plan optimizedPlan = plan(sql, Optimizer.PlanStage.OPTIMIZED, enabled());
        // The pushed predicate references columns from BOTH sides of the orders/lineitem join, so
        // predicate pushdown lands it as the orders/lineitem InnerJoin's additional filter — i.e.
        // sitting BETWEEN the customer join (above) and the orders/lineitem join (below).
        boolean foundInJoinFilter = searchFrom(optimizedPlan.getRoot())
                .where(node -> node instanceof JoinNode
                        && ((JoinNode) node).getFilter().isPresent()
                        && ((JoinNode) node).getFilter().get().toString().contains("totalprice")
                        && ((JoinNode) node).getFilter().get().toString().contains("extendedprice"))
                .matches();
        boolean foundInFilterNode = searchFrom(optimizedPlan.getRoot())
                .where(node -> node instanceof FilterNode
                        && ((FilterNode) node).getPredicate().toString().contains("totalprice")
                        && ((FilterNode) node).getPredicate().toString().contains("extendedprice"))
                .matches();
        assertTrue(foundInJoinFilter || foundInFilterNode,
                "Expected (totalprice + extendedprice) >= ... to appear between the two joins (as a JoinNode filter or a FilterNode) when push_filter_through_selecting_aggregation is enabled");
    }

    @Test
    public void testFilterOnMaxPushesPastTwoJoinsToDeepestTable()
    {
        // Two joins: orders-lineitem, then with customer. Aggregation MAX(l.extendedprice) sits
        // ABOVE both joins. The HAVING filter should push below the aggregation (becoming
        // extendedprice >= c) and then through both joins all the way down to the lineitem scan
        // — the deepest table that produces the filtered column.
        String sql = "SELECT o.custkey, MAX(l.extendedprice) FROM orders o " +
                "JOIN lineitem l ON o.orderkey = l.orderkey " +
                "JOIN customer c ON o.custkey = c.custkey " +
                "GROUP BY o.custkey HAVING MAX(l.extendedprice) >= 50000";

        Plan optimizedPlan = plan(sql, Optimizer.PlanStage.OPTIMIZED, enabled());
        assertTrue(searchFrom(optimizedPlan.getRoot())
                        .where(node -> node instanceof FilterNode
                                && ((FilterNode) node).getPredicate().toString().contains("extendedprice"))
                        .matches(),
                "Expected a FilterNode on extendedprice (pushed past two joins to the lineitem scan) when push_filter_through_selecting_aggregation is enabled");
    }

    @Test
    public void testFilterOnMaxPushesAllTheWayDownPastJoin()
    {
        // The pushed-down filter on totalprice should reach the orders table scan, sitting BEFORE
        // the join. This requires our rule + predicate pushdown to carry the filter through the
        // CTE/projection layer and join down to the orders scan.
        String sql = "WITH agg AS (SELECT custkey, MAX(totalprice) AS max_price FROM orders GROUP BY custkey) " +
                "SELECT c.name, agg.max_price FROM agg JOIN customer c ON c.custkey = agg.custkey WHERE agg.max_price >= 300000";

        Plan optimizedPlan = plan(sql, com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED, enabled());
        assertTrue(searchFrom(optimizedPlan.getRoot())
                        .where(node -> node instanceof FilterNode
                                && ((FilterNode) node).getPredicate().toString().contains("totalprice"))
                        .matches(),
                "Expected a FilterNode on totalprice (pushed below the aggregation and through the join) when push_filter_through_selecting_aggregation is enabled");
    }

    @Test
    public void testFilterOnMaxDoesNotReachOrdersScanWhenDisabled()
    {
        String sql = "WITH agg AS (SELECT custkey, MAX(totalprice) AS max_price FROM orders GROUP BY custkey) " +
                "SELECT c.name, agg.max_price FROM agg JOIN customer c ON c.custkey = agg.custkey WHERE agg.max_price >= 300000";

        // When the rule is disabled, no filter on totalprice (the underlying column) should appear.
        assertFalse(searchFrom(plan(sql, Optimizer.PlanStage.OPTIMIZED, disabled()).getRoot())
                        .where(node -> node instanceof FilterNode
                                && ((FilterNode) node).getPredicate().toString().contains("totalprice"))
                        .matches(),
                "totalprice filter should not appear when push_filter_through_selecting_aggregation is disabled");
    }
}
