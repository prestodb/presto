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
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.TASK_CONCURRENCY;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;

public class TestSortWithinPartitionPlans
        extends BasePlanTest
{
    @Test
    public void testSortWithPartition()
    {
        LocalQueryRunner localQueryRunner = getQueryRunner();
        localQueryRunner.setAdditionalOptimizer(ImmutableList.of(new IterativeOptimizer(
                localQueryRunner.getMetadata(),
                new RuleStatsRecorder(),
                localQueryRunner.getStatsCalculator(),
                localQueryRunner.getEstimatedExchangesCostCalculator(),
                ImmutableSet.of(new TestAddPartitionToSortRule()))));
        Session session = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(TASK_CONCURRENCY, "2")
                .build();

        assertDistributedPlan("SELECT partkey, discount from lineitem order by discount",
                session,
                anyTree(
                        exchange(REMOTE_STREAMING, GATHER, ImmutableList.of(),
                                sort(
                                        anyTree(
                                                exchange(LOCAL, REPARTITION,
                                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                                anyTree(
                                                                        tableScan("lineitem")))))))));
    }
}
