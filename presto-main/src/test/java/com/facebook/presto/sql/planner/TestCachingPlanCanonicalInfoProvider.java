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
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.cost.HistoryBasedPlanStatisticsCalculator;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.historyBasedPlanCanonicalizationStrategyList;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.graph.Traverser.forTree;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCachingPlanCanonicalInfoProvider
        extends BasePlanTest
{
    @Test
    public void testCache()
    {
        Session session = createSession();
        String sql = "SELECT O.totalprice, C.name FROM orders O, customer C WHERE C.custkey = O.custkey LIMIT 10";
        PlanNode root = plan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, session).getRoot();
        assertTrue(root.getStatsEquivalentPlanNode().isPresent());

        CachingPlanCanonicalInfoProvider planCanonicalInfoProvider = (CachingPlanCanonicalInfoProvider) ((HistoryBasedPlanStatisticsCalculator) getQueryRunner().getStatsCalculator()).getPlanCanonicalInfoProvider();
        assertEquals(planCanonicalInfoProvider.getCacheSize(), 5L * historyBasedPlanCanonicalizationStrategyList().size());
        forTree(PlanNode::getSources).depthFirstPreOrder(root).forEach(child -> {
            if (!child.getStatsEquivalentPlanNode().isPresent()) {
                return;
            }
            for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList()) {
                planCanonicalInfoProvider.hash(session, child.getStatsEquivalentPlanNode().get(), strategy).get();
            }
        });
        // Assert that size of cache remains same, meaning all needed hashes were already cached.
        assertEquals(planCanonicalInfoProvider.getCacheSize(), 5L * historyBasedPlanCanonicalizationStrategyList().size());
    }

    private Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty("task_concurrency", "1")
                .build();
    }
}
