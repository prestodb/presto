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
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.sql.Optimizer;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.testing.InMemoryHistoryBasedPlanStatisticsProvider;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY;
import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.historyBasedPlanCanonicalizationStrategyList;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.graph.Traverser.forTree;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCachingPlanCanonicalInfoProvider
        extends BasePlanTest
{
    public TestCachingPlanCanonicalInfoProvider()
    {
        super(() -> createTestQueryRunner());
    }

    private static LocalQueryRunner createTestQueryRunner()
    {
        LocalQueryRunner queryRunner = createQueryRunner(ImmutableMap.of());
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<HistoryBasedPlanStatisticsProvider> getHistoryBasedPlanStatisticsProviders()
            {
                return ImmutableList.of(new InMemoryHistoryBasedPlanStatisticsProvider());
            }
        });
        return queryRunner;
    }

    @Test
    public void testCache()
    {
        Session session = createSession();
        String sql = "SELECT O.totalprice, C.name FROM orders O, customer C WHERE C.custkey = O.custkey LIMIT 10";
        PlanNode root = plan(sql, Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED, session).getRoot();
        assertTrue(root.getStatsEquivalentPlanNode().isPresent());

        CachingPlanCanonicalInfoProvider planCanonicalInfoProvider = (CachingPlanCanonicalInfoProvider) ((HistoryBasedPlanStatisticsCalculator) getQueryRunner().getStatsCalculator()).getPlanCanonicalInfoProvider();
        assertEquals(planCanonicalInfoProvider.getCacheSize(), 5L * historyBasedPlanCanonicalizationStrategyList().size());
        forTree(PlanNode::getSources).depthFirstPreOrder(root).forEach(child -> {
            if (!child.getStatsEquivalentPlanNode().isPresent()) {
                return;
            }
            for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList()) {
                planCanonicalInfoProvider.hash(session, child.getStatsEquivalentPlanNode().get(), strategy, false).get();
            }
        });
        // Assert that size of cache remains same, meaning all needed hashes were already cached.
        assertEquals(planCanonicalInfoProvider.getCacheSize(), 5L * historyBasedPlanCanonicalizationStrategyList().size());
        planCanonicalInfoProvider.getHistoryBasedStatisticsCacheManager().invalidate(session.getQueryId());
        assertEquals(planCanonicalInfoProvider.getCacheSize(), 0);

        forTree(PlanNode::getSources).depthFirstPreOrder(root).forEach(child -> {
            if (!child.getStatsEquivalentPlanNode().isPresent()) {
                return;
            }
            for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList()) {
                // Only read from cache, hence will return Optional.empty() as the cache is already invalidated
                assertFalse(planCanonicalInfoProvider.hash(session, child.getStatsEquivalentPlanNode().get(), strategy, true).isPresent());
            }
        });
        // Assert that cache is not populated as we only read from cache without populating with cache miss
        assertEquals(planCanonicalInfoProvider.getCacheSize(), 0);
    }

    private Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty("task_concurrency", "1")
                .setSystemProperty(RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY, "false")
                .build();
    }
}
