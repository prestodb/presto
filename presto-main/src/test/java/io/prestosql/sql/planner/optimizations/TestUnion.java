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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.Iterables;
import io.prestosql.sql.planner.LogicalPlanner;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TopNNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestUnion
        extends BasePlanTest
{
    public TestUnion()
    {
        super();
    }

    public TestUnion(Map<String, String> sessionProperties)
    {
        super(sessionProperties);
    }

    @Test
    public void testSimpleUnion()
    {
        Plan plan = plan(
                "SELECT suppkey FROM supplier UNION ALL SELECT nationkey FROM nation",
                LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                false);

        List<PlanNode> remotes = searchFrom(plan.getRoot())
                .where(TestUnion::isRemoteExchange)
                .findAll();

        assertEquals(remotes.size(), 1, "There should be exactly one RemoteExchange");
        assertEquals(((ExchangeNode) Iterables.getOnlyElement(remotes)).getType(), GATHER);
        assertPlanIsFullyDistributed(plan);
    }

    @Test
    public void testUnionUnderTopN()
    {
        Plan plan = plan(
                "SELECT * FROM (" +
                        "   SELECT regionkey FROM nation " +
                        "   UNION ALL " +
                        "   SELECT nationkey FROM nation" +
                        ") t(a) " +
                        "ORDER BY a LIMIT 1",
                LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                false);

        List<PlanNode> remotes = searchFrom(plan.getRoot())
                .where(TestUnion::isRemoteExchange)
                .findAll();

        assertEquals(remotes.size(), 1, "There should be exactly one RemoteExchange");
        assertEquals(((ExchangeNode) Iterables.getOnlyElement(remotes)).getType(), GATHER);

        int numberOfpartialTopN = searchFrom(plan.getRoot())
                .where(planNode -> planNode instanceof TopNNode && ((TopNNode) planNode).getStep().equals(TopNNode.Step.PARTIAL))
                .count();
        assertEquals(numberOfpartialTopN, 2, "There should be exactly two partial TopN nodes");
        assertPlanIsFullyDistributed(plan);
    }

    @Test
    public void testUnionOverSingleNodeAggregationAndUnion()
    {
        Plan plan = plan(
                "SELECT count(*) FROM (" +
                        "SELECT 1 FROM nation GROUP BY regionkey " +
                        "UNION ALL (" +
                        "   SELECT 1 FROM nation " +
                        "   UNION ALL " +
                        "   SELECT 1 FROM nation))",
                LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                false);

        List<PlanNode> remotes = searchFrom(plan.getRoot())
                .where(TestUnion::isRemoteExchange)
                .findAll();

        assertEquals(remotes.size(), 2, "There should be exactly two RemoteExchanges");
        assertEquals(((ExchangeNode) remotes.get(0)).getType(), GATHER);
        assertEquals(((ExchangeNode) remotes.get(1)).getType(), REPARTITION);
    }

    @Test
    public void testPartialAggregationsWithUnion()
    {
        Plan plan = plan(
                "SELECT orderstatus, sum(orderkey) FROM (SELECT orderkey, orderstatus FROM orders UNION ALL SELECT orderkey, orderstatus FROM orders) x GROUP BY (orderstatus)",
                LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                false);
        assertAtMostOneAggregationBetweenRemoteExchanges(plan);
        assertPlanIsFullyDistributed(plan);
    }

    @Test
    public void testPartialRollupAggregationsWithUnion()
    {
        Plan plan = plan(
                "SELECT orderstatus, sum(orderkey) FROM (SELECT orderkey, orderstatus FROM orders UNION ALL SELECT orderkey, orderstatus FROM orders) x GROUP BY ROLLUP (orderstatus)",
                LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                false);
        assertAtMostOneAggregationBetweenRemoteExchanges(plan);
        assertPlanIsFullyDistributed(plan);
    }

    @Test
    public void testAggregationWithUnionAndValues()
    {
        Plan plan = plan(
                "SELECT regionkey, count(*) FROM (SELECT regionkey FROM nation UNION ALL SELECT * FROM (VALUES 2, 100) t(regionkey)) GROUP BY regionkey",
                LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                false);
        assertAtMostOneAggregationBetweenRemoteExchanges(plan);
        // TODO: Enable this check once distributed UNION can handle both partitioned and single node sources at the same time
        //assertPlanIsFullyDistributed(plan);
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
        int numberOfGathers = searchFrom(plan.getRoot())
                .where(TestUnion::isRemoteGatheringExchange)
                .findAll()
                .size();

        if (numberOfGathers == 0) {
            // there are no "gather" nodes, so the plan is expected to be fully distributed
            return;
        }

        assertTrue(
                searchFrom(plan.getRoot())
                        .recurseOnlyWhen(TestUnion::isNotRemoteGatheringExchange)
                        .findAll()
                        .stream()
                        .noneMatch(this::shouldBeDistributed),
                "There is a node that should be distributed between output and first REMOTE GATHER ExchangeNode");

        assertEquals(numberOfGathers, 1, "Only a single REMOTE GATHER was expected");
    }

    private boolean shouldBeDistributed(PlanNode planNode)
    {
        if (planNode instanceof JoinNode) {
            return true;
        }
        if (planNode instanceof AggregationNode) {
            // TODO: differentiate aggregation with empty grouping set
            return true;
        }
        if (planNode instanceof TopNNode) {
            return ((TopNNode) planNode).getStep() == TopNNode.Step.PARTIAL;
        }
        return false;
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
                    .recurseOnlyWhen(TestUnion::isNotRemoteExchange)
                    .findAll();

            assertFalse(aggregations.size() > 1, "More than a single AggregationNode between remote exchanges");
        }
    }

    private static boolean isNotRemoteGatheringExchange(PlanNode planNode)
    {
        return !isRemoteGatheringExchange(planNode);
    }

    private static boolean isRemoteGatheringExchange(PlanNode planNode)
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
