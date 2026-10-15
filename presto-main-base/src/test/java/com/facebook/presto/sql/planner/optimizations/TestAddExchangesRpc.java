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
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RPCNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.RPC_FUNCTION_OPTIMIZER_ENABLED;
import static com.facebook.presto.SystemSessionProperties.RPC_FUNCTION_PARALLELISM;
import static com.facebook.presto.SystemSessionProperties.RPC_STREAMING_MODE;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.ROUND_ROBIN;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitioning.FIXED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.createSystemPartitioning;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.RPCNode.StreamingMode.BATCH;
import static com.facebook.presto.sql.planner.plan.RPCNode.StreamingMode.PER_ROW;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestAddExchangesRpc
        extends BasePlanTest
{
    @DataProvider
    public Object[][] streamingModes()
    {
        return new Object[][] {{BATCH}, {PER_ROW}};
    }

    @Test(dataProvider = "streamingModes")
    public void testRpcHasCountedRemoteInputExchange(RPCNode.StreamingMode streamingMode)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(RPC_FUNCTION_OPTIMIZER_ENABLED, "true")
                .setSystemProperty(RPC_FUNCTION_PARALLELISM, "2")
                .setSystemProperty(HASH_PARTITION_COUNT, "8")
                .setSystemProperty(RPC_STREAMING_MODE, streamingMode.name())
                .build();
        PlanOptimizer addExchanges = getQueryRunner().getPlanOptimizers(false).stream()
                .filter(StatsRecordingPlanOptimizer.class::isInstance)
                .map(StatsRecordingPlanOptimizer.class::cast)
                .map(StatsRecordingPlanOptimizer::getDelegate)
                .filter(AddExchanges.class::isInstance)
                .collect(onlyElement());
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new RpcFunctionOptimizer(() -> ImmutableSet.of("lower")),
                addExchanges);
        Plan plan = getQueryRunner().inTransaction(session, transactionSession -> getQueryRunner().createPlan(
                transactionSession,
                "SELECT lower(comment) FROM orders",
                optimizers,
                OPTIMIZED,
                WarningCollector.NOOP));

        RPCNode rpc = searchFrom(plan.getRoot()).where(RPCNode.class::isInstance).findOnlyElement();
        assertEquals(rpc.getStreamingMode(), streamingMode);
        assertTrue(rpc.getSource() instanceof ExchangeNode, "RPC input must cross a stage boundary");
        ExchangeNode exchange = (ExchangeNode) rpc.getSource();
        assertEquals(exchange.getScope(), REMOTE_STREAMING);
        assertEquals(exchange.getType(), REPARTITION);
        assertEquals(exchange.getPartitioningScheme().getPartitioning().getHandle(), createSystemPartitioning(FIXED, ROUND_ROBIN, 2));
    }
}
