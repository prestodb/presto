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
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RPCNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.RPC_FUNCTION_PARALLELISM;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.ROUND_ROBIN;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitioning.FIXED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.createSystemPartitioning;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.roundRobinExchange;
import static com.facebook.presto.sql.planner.plan.RPCNode.StreamingMode.BATCH;
import static com.facebook.presto.sql.planner.plan.RPCNode.StreamingMode.PER_ROW;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestAddExchangesRpcPlanning
        extends BasePlanTest
{
    @DataProvider
    public Object[][] rpcInputs()
    {
        List<Object[]> cases = new ArrayList<>();
        for (boolean[] environment : new boolean[][] {{false, false}, {true, false}, {true, true}}) {
            for (int parallelism : new int[] {0, 1, 8}) {
                for (RPCNode.StreamingMode mode : new RPCNode.StreamingMode[] {BATCH, PER_ROW}) {
                    for (boolean unionInput : new boolean[] {false, true}) {
                        cases.add(new Object[] {environment[0], environment[1], parallelism, mode, unionInput});
                    }
                }
            }
        }
        return cases.toArray(new Object[0][]);
    }

    @Test(dataProvider = "rpcInputs")
    public void testRpcInputPlanning(boolean nativeExecution, boolean sparkExecution, int parallelism, RPCNode.StreamingMode mode, boolean unionInput)
    {
        assertRpcInputPlanning(nativeExecution, sparkExecution, parallelism, mode, unionInput, 0, false, parallelism > 1 ? parallelism : 0);
    }

    @DataProvider
    public Object[][] recommendedRpcInputs()
    {
        return new Object[][] {
                {false, false, 0, BATCH, true, 8, true, 8},
                {true, false, 0, BATCH, true, 8, true, 8},
                {false, false, 0, BATCH, false, 8, false, 8},
                {true, false, 0, BATCH, false, 8, false, 8},
                {false, false, 0, PER_ROW, true, 8, true, 0},
                {true, false, 0, PER_ROW, true, 8, true, 0},
                {false, false, 1, BATCH, true, 8, true, 0},
                {true, false, 1, BATCH, true, 8, true, 0},
                {false, false, 2, BATCH, true, 8, true, 2},
                {true, false, 2, BATCH, true, 8, true, 2},
                {true, true, 0, BATCH, true, 8, true, 0},
                {true, false, 0, BATCH, true, 1, true, 0},
                {true, false, 0, BATCH, true, 0, true, 0},
                {true, false, 0, BATCH, true, 8, false, 0},
        };
    }

    @Test(dataProvider = "recommendedRpcInputs")
    public void testRecommendedRpcTaskCount(
            boolean nativeExecution,
            boolean sparkExecution,
            int parallelism,
            RPCNode.StreamingMode mode,
            boolean unionInput,
            int recommendedTaskCount,
            boolean singleNodeUnion,
            int expectedPartitionCount)
    {
        assertRpcInputPlanning(nativeExecution, sparkExecution, parallelism, mode, unionInput, recommendedTaskCount, singleNodeUnion, expectedPartitionCount);
    }

    private void assertRpcInputPlanning(
            boolean nativeExecution,
            boolean sparkExecution,
            int parallelism,
            RPCNode.StreamingMode mode,
            boolean unionInput,
            int recommendedTaskCount,
            boolean singleNodeUnion,
            int expectedPartitionCount)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(HASH_PARTITION_COUNT, "128")
                .setSystemProperty(RPC_FUNCTION_PARALLELISM, Integer.toString(parallelism))
                .build();
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getMetadata());
        VariableReferenceExpression input = builder.variable("input", BIGINT);
        VariableReferenceExpression result = builder.variable("result", BIGINT);
        PlanNode inputNode = builder.values(input);
        if (unionInput) {
            VariableReferenceExpression left = builder.variable("left", BIGINT);
            VariableReferenceExpression right = builder.variable("right", BIGINT);
            PlanNode leftSource = builder.values(left);
            PlanNode rightSource = builder.values(right);
            if (!singleNodeUnion) {
                leftSource = roundRobinExchange(idAllocator.getNextId(), REMOTE_STREAMING, leftSource, 8);
                rightSource = partitionedExchange(idAllocator.getNextId(), REMOTE_STREAMING, rightSource,
                        Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of(right)), Optional.empty());
            }
            inputNode = builder.union(ImmutableListMultimap.of(input, left, input, right), ImmutableList.of(leftSource, rightSource));
        }
        RPCNode rpc = new RPCNode(Optional.empty(), idAllocator.getNextId(), inputNode, "test_rpc",
                ImmutableList.of(input), ImmutableList.of("input"), result, mode, 0, recommendedTaskCount);
        PlanOptimizer addExchanges = new AddExchanges(getMetadata(), new PartitioningProviderManager(), nativeExecution, sparkExecution);
        PlanNode optimized = addExchanges.optimize(rpc, session, builder.getTypes(),
                new VariableAllocator(builder.getTypes().allVariables()), idAllocator, WarningCollector.NOOP, false)
                .getPlanNode();

        assertTrue(optimized instanceof RPCNode);
        RPCNode optimizedRpc = (RPCNode) optimized;
        assertEquals(optimizedRpc.getStreamingMode(), mode);
        assertEquals(optimizedRpc.getRecommendedTaskCount(), recommendedTaskCount);
        assertEquals(optimizedRpc.getOutputVariables(), rpc.getOutputVariables());
        PlanNode source = optimizedRpc.getSource();
        if (expectedPartitionCount > 1) {
            assertTrue(source instanceof ExchangeNode);
            ExchangeNode exchange = (ExchangeNode) source;
            assertEquals(exchange.getScope(), REMOTE_STREAMING);
            assertEquals(exchange.getType(), REPARTITION);
            assertEquals(exchange.getPartitioningScheme().getPartitioning().getHandle(), createSystemPartitioning(FIXED, ROUND_ROBIN, expectedPartitionCount));
            source = exchange.getSources().get(0);
        }
        assertEquals(source.getId(), inputNode.getId());
        assertEquals(source.getOutputVariables(), inputNode.getOutputVariables());
    }
}
