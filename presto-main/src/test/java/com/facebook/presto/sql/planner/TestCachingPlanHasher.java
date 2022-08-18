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
import com.facebook.presto.cost.HistoryBasedPlanStatisticsCalculator;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCachingPlanHasher
        extends BasePlanTest
{
    @Test
    public void testCache()
    {
        Session session = createSession();
        String sql = "SELECT COUNT_IF(totalprice > 0) from orders WHERE custkey > 100 GROUP BY orderkey";
        PlanNode plan = plan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, session).getRoot();
        assertTrue(plan.getStatsEquivalentPlanNode().isPresent());
        PlanHasher planHasher = ((HistoryBasedPlanStatisticsCalculator) getQueryRunner().getStatsCalculator()).getPlanHasher();
        // We cache hashes for all intermediate plan nodes
        assertEquals(((CachingPlanHasher) planHasher).getCacheSize(), 12);
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
