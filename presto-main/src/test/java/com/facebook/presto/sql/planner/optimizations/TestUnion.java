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

import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestUnion
        extends BasePlanTest
{
    public TestUnion()
    {
        super();
    }

    public TestUnion(ImmutableMap<String, String> sessionProperties)
    {
        super(sessionProperties);
    }

    @Test
    public void testPartialAggregationsWithUnion()
    {
        Plan plan = plan(
                "SELECT orderstatus, sum(orderkey) FROM (SELECT orderkey, orderstatus FROM orders UNION ALL SELECT orderkey, orderstatus FROM orders) x GROUP BY (orderstatus)",
                LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                false);
        assertAtMostOneAggregationBetweenRemoteExchanges(plan);
    }

    @Test
    public void testPartialRollupAggregationsWithUnion()
    {
        Plan plan = plan(
                "SELECT orderstatus, sum(orderkey) FROM (SELECT orderkey, orderstatus FROM orders UNION ALL SELECT orderkey, orderstatus FROM orders) x GROUP BY ROLLUP (orderstatus)",
                LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                false);
        assertAtMostOneAggregationBetweenRemoteExchanges(plan);
    }

    @Test
    public void testUnionOnProbeSide()
    {
        Plan plan = plan(
                "SELECT * FROM (SELECT * FROM nation UNION ALL SELECT * from nation) n, region r WHERE n.regionkey=r.regionkey",
                LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                false);

        assertPlanIsFullyDistributed(plan);
    }

    private void assertPlanIsFullyDistributed(Plan plan)
    {
        List<PlanNode> beforeGather = searchFrom(plan.getRoot())
                .skipOnlyWhen(TestUnion::isRemoteGatherExchange)
                .findAll()
                .stream()
                .collect(toList());

        boolean noAggregationsAndJoinsBeforeGather = searchFrom(plan.getRoot())
                .skipOnlyWhen(TestUnion::isRemoteGatherExchange)
                .findAll()
                .stream()
                .noneMatch(planNode -> planNode instanceof AggregationNode || planNode instanceof JoinNode);

        List<PlanNode> gathers = searchFrom(plan.getRoot())
                .where(TestUnion::isRemoteGatherExchange)
                .findAll()
                .stream()
                .collect(toList());

        assertEquals(gathers.size(), 1, "There are more then one REMOTE GATHER ExchangeNode");
        assertTrue(noAggregationsAndJoinsBeforeGather, "There is an Aggregation or Join between output and first REMOTE GATHER ExchangeNode");
    }

    private static void assertAtMostOneAggregationBetweenRemoteExchanges(Plan plan)
    {
        List<PlanNode> fragments = searchFrom(plan.getRoot())
                .where(TestUnion::isRemoteExchange)
                .findAll()
                .stream()
                .flatMap(exchangeNode -> exchangeNode.getSources().stream())
                .collect(toList());

        for (PlanNode fragment : fragments) {
            List<PlanNode> aggregations = searchFrom(fragment)
                    .where(AggregationNode.class::isInstance)
                    .skipOnlyWhen(TestUnion::isNotRemoteExchange)
                    .findAll();

            assertTrue(aggregations.size() <= 1, "Found two AggregationNodes before remote exchanges");
        }
    }

    private static boolean isRemoteGatherExchange(PlanNode planNode)
    {
        return isRemoteExchange(planNode) && ((ExchangeNode) planNode).getType().equals(GATHER);
    }

    private static boolean isNotRemoteExchange(PlanNode planNode)
    {
        return !isRemoteExchange(planNode);
    }

    private static boolean isRemoteExchange(PlanNode planNode)
    {
        return (planNode instanceof ExchangeNode) && ((ExchangeNode) planNode).getScope().equals(REMOTE);
    }
}
