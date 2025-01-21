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

import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.ADD_EXCHANGE_BELOW_PARTIAL_AGGREGATION_OVER_GROUP_ID;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestLogicalAddExchangesBelowPartialAggregationOverGroupIdRuleSet
        extends BasePlanTest
{
    public TestLogicalAddExchangesBelowPartialAggregationOverGroupIdRuleSet()
    {
        super(TestLogicalAddExchangesBelowPartialAggregationOverGroupIdRuleSet::setup);
    }

    private static LocalQueryRunner setup()
    {
        // We set available max-partial-aggregation-memory to a low value to allow the rule to trigger for the TPCH tiny scale factor
        TaskManagerConfig taskManagerConfig = new TaskManagerConfig().setMaxPartialAggregationMemoryUsage(DataSize.succinctDataSize(1, KILOBYTE));
        return createQueryRunner(ImmutableMap.of(ADD_EXCHANGE_BELOW_PARTIAL_AGGREGATION_OVER_GROUP_ID, "true"), taskManagerConfig);
    }

    @Test
    public void testRollup()
    {
        assertDistributedPlan("SELECT orderkey, suppkey, partkey, sum(quantity) from lineitem GROUP BY ROLLUP(orderkey, suppkey, partkey)",
                anyTree(node(GroupIdNode.class,
                        // Since 'orderkey' will be the variable with the highest frequency, we repartition on it
                        anyTree(exchange(LOCAL, REPARTITION, ImmutableList.of(), ImmutableSet.of("orderkey"),
                                exchange(REMOTE_STREAMING, REPARTITION, ImmutableList.of(), ImmutableSet.of("orderkey"),
                                        anyTree(tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey")))))))));
    }

    @Test
    public void testNegativeCases()
    {
        // Rule does not apply if grouping sets <=2
        assertDistributedPlan("SELECT orderkey, suppkey, sum(quantity) from lineitem GROUP BY GROUPING SETS (orderkey,(orderkey, suppkey))",
                anyTree(node(GroupIdNode.class,
                        tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey")))));

        //  Rule does not apply when aggregation will be effective due to a sufficiently high max-partial-aggregation-memory
        TaskManagerConfig taskManagerConfig = new TaskManagerConfig().setMaxPartialAggregationMemoryUsage(DataSize.succinctDataSize(1, MEGABYTE));
        try (LocalQueryRunner queryRunner = createQueryRunner(ImmutableMap.of(ADD_EXCHANGE_BELOW_PARTIAL_AGGREGATION_OVER_GROUP_ID, "true"), taskManagerConfig)) {
            queryRunner.inTransaction(queryRunner.getDefaultSession(), transactionSession -> {
                Plan plan = queryRunner.createPlan(transactionSession,
                        "SELECT orderkey, suppkey, partkey, sum(quantity) from lineitem GROUP BY ROLLUP(orderkey, suppkey, partkey)",
                        WarningCollector.NOOP);

                PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), plan,
                        anyTree(node(GroupIdNode.class,
                                tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey")))));
                return null;
            });
        }
    }
}
