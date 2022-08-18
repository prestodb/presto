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
package com.facebook.presto.cost;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class TestHistoryBasedStatsProvider
{
    private final LocalQueryRunner queryRunner;

    public TestHistoryBasedStatsProvider()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                .build());

        queryRunner.createCatalog(
                queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());

        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<HistoryBasedPlanStatisticsProvider> getHistoryBasedPlanStatisticsProviders()
            {
                return ImmutableList.of(new TestHistoryBasedPlanStatisticsProvider());
            }
        });
    }

    @Test
    public void testHistoryBasedStatsCalculator()
    {
        // Overridden stats
        assertPlan(
                "SELECT orderstatus FROM orders",
                anyTree(node(TableScanNode.class).withOutputRowCount(100)));

        // Original stats
        assertPlan(
                "SELECT * FROM nation",
                anyTree(node(TableScanNode.class).withOutputRowCount(25)));
    }

    private void assertPlan(String sql, PlanMatchPattern pattern)
    {
        assertPlan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, pattern);
    }

    private void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern)
    {
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, stage, WarningCollector.NOOP);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            return null;
        });
    }

    private static class TestHistoryBasedPlanStatisticsProvider
            implements HistoryBasedPlanStatisticsProvider
    {
        public TestHistoryBasedPlanStatisticsProvider()
        {
        }

        @Override
        public String getName()
        {
            return "test";
        }

        @Override
        public Map<PlanNodeWithHash, HistoricalPlanStatistics> getStats(List<PlanNodeWithHash> planNodeHashes)
        {
            return planNodeHashes.stream().collect(toImmutableMap(
                    PlanNodeWithHash -> PlanNodeWithHash,
                    PlanNodeWithHash -> {
                        if (PlanNodeWithHash.getPlanNode() instanceof TableScanNode) {
                            TableScanNode node = (TableScanNode) PlanNodeWithHash.getPlanNode();
                            if (node.getTable().toString().contains("orders")) {
                                return new HistoricalPlanStatistics(new PlanStatistics(Estimate.of(100), Estimate.of(100), 1.0));
                            }
                        }
                        return new HistoricalPlanStatistics(PlanStatistics.empty());
                    }));
        }

        @Override
        public void putStats(Map<PlanNodeWithHash, HistoricalPlanStatistics> hashesAndStatistics) {}
    }
}
