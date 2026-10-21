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
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.UnionNode;
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

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.RPC_FUNCTION_PARALLELISM;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.HASH;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.ROUND_ROBIN;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitioning.FIXED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.createSystemPartitioning;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.roundRobinExchange;
import static com.facebook.presto.sql.planner.plan.RPCNode.StreamingMode.PER_ROW;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestSparkUnionPartitionCountReconciliation
        extends BasePlanTest
{
    @DataProvider
    public Object[][] unionInputs()
    {
        return new Object[][] {
                {false, true, 8, FIXED_HASH_DISTRIBUTION, false, false},
                {true, true, 8, FIXED_HASH_DISTRIBUTION, true, false},
                {true, false, 8, FIXED_HASH_DISTRIBUTION, true, false},
                {true, true, 8, createSystemPartitioning(FIXED, HASH, 8), false, false},
                {true, false, 8, createSystemPartitioning(FIXED, HASH, 8), false, false},
                {true, true, 8, createSystemPartitioning(FIXED, HASH, 16), true, true},
                {true, true, 128, FIXED_HASH_DISTRIBUTION, false, false},
        };
    }

    @Test(dataProvider = "unionInputs")
    public void testUnionInputs(
            boolean sparkExecution,
            boolean rpcFirst,
            int rpcPartitionCount,
            PartitioningHandle hashPartitioning,
            boolean rpcInputReshuffled,
            boolean hashInputReshuffled)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(HASH_PARTITION_COUNT, "128")
                .setSystemProperty(RPC_FUNCTION_PARALLELISM, Integer.toString(rpcPartitionCount))
                .build();
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getMetadata());
        VariableReferenceExpression input = builder.variable("input", BIGINT);
        VariableReferenceExpression result = builder.variable("result", BIGINT);
        VariableReferenceExpression other = builder.variable("other", BIGINT);
        VariableReferenceExpression output = builder.variable("output", BIGINT);
        RPCNode rpc = new RPCNode(Optional.empty(), idAllocator.getNextId(), builder.values(input), "test_rpc",
                ImmutableList.of(input), ImmutableList.of("input"), result, PER_ROW, 0);
        ExchangeNode hash = partitionedExchange(idAllocator.getNextId(), REMOTE_STREAMING, builder.values(other),
                Partitioning.create(hashPartitioning, ImmutableList.of(other)), Optional.empty());
        UnionNode union = builder.union(
                ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                        .putAll(output, rpcFirst ? ImmutableList.of(result, other) : ImmutableList.of(other, result))
                        .build(),
                rpcFirst ? ImmutableList.of(rpc, hash) : ImmutableList.of(hash, rpc));

        PlanNode optimized = new AddExchanges(getMetadata(), new PartitioningProviderManager(), false, sparkExecution)
                .optimize(union, session, builder.getTypes(), new VariableAllocator(builder.getTypes().allVariables()), idAllocator, WarningCollector.NOOP, false)
                .getPlanNode();

        assertTrue(optimized instanceof UnionNode);
        assertEquals(optimized.getOutputVariables(), union.getOutputVariables());
        int rpcIndex = rpcFirst ? 0 : 1;
        int hashIndex = rpcFirst ? 1 : 0;
        PlanNode optimizedRpcInput = unwrap(optimized.getSources().get(rpcIndex), union.getSources().get(rpcIndex), rpcInputReshuffled);
        PlanNode optimizedHashInput = unwrap(optimized.getSources().get(hashIndex), union.getSources().get(hashIndex), hashInputReshuffled);

        // Reconciliation preserves the RPC's requested input parallelism and the other branch's hash exchange.
        RPCNode optimizedRpc = (RPCNode) optimizedRpcInput;
        assertTrue(optimizedRpc.getSource() instanceof ExchangeNode);
        assertEquals(((ExchangeNode) optimizedRpc.getSource()).getPartitioningScheme().getPartitioning().getHandle(),
                createSystemPartitioning(FIXED, ROUND_ROBIN, rpcPartitionCount));
        assertEquals(((ExchangeNode) optimizedHashInput).getPartitioningScheme().getPartitioning().getHandle(), hashPartitioning);
    }

    @Test
    public void testMatchingRpcInputsAreNotReshuffled()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(HASH_PARTITION_COUNT, "128")
                .setSystemProperty(RPC_FUNCTION_PARALLELISM, "8")
                .build();
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getMetadata());
        VariableReferenceExpression left = builder.variable("left", BIGINT);
        VariableReferenceExpression leftResult = builder.variable("left_result", BIGINT);
        VariableReferenceExpression right = builder.variable("right", BIGINT);
        VariableReferenceExpression rightResult = builder.variable("right_result", BIGINT);
        VariableReferenceExpression output = builder.variable("output", BIGINT);
        RPCNode leftRpc = new RPCNode(Optional.empty(), idAllocator.getNextId(), builder.values(left), "test_rpc",
                ImmutableList.of(left), ImmutableList.of("left"), leftResult, PER_ROW, 0);
        RPCNode rightRpc = new RPCNode(Optional.empty(), idAllocator.getNextId(), builder.values(right), "test_rpc",
                ImmutableList.of(right), ImmutableList.of("right"), rightResult, PER_ROW, 0);
        UnionNode union = builder.union(ImmutableListMultimap.of(output, leftResult, output, rightResult), ImmutableList.of(leftRpc, rightRpc));

        PlanNode optimized = new AddExchanges(getMetadata(), new PartitioningProviderManager(), false, true)
                .optimize(union, session, builder.getTypes(), new VariableAllocator(builder.getTypes().allVariables()), idAllocator, WarningCollector.NOOP, false)
                .getPlanNode();

        assertTrue(optimized instanceof UnionNode);
        assertEquals(optimized.getSources().get(0).getId(), leftRpc.getId());
        assertEquals(optimized.getSources().get(1).getId(), rightRpc.getId());
    }

    private static PlanNode unwrap(PlanNode optimizedInput, PlanNode originalInput, boolean reshuffled)
    {
        PlanNode input = optimizedInput;
        if (reshuffled) {
            assertTrue(input instanceof ExchangeNode);
            ExchangeNode exchange = (ExchangeNode) input;
            assertEquals(exchange.getScope(), REMOTE_STREAMING);
            assertEquals(exchange.getPartitioningScheme().getPartitioning().getHandle(), FIXED_ARBITRARY_DISTRIBUTION);
            input = exchange.getSources().get(0);
        }
        assertEquals(input.getId(), originalInput.getId());
        return input;
    }

    @Test
    public void testReconciliationDisabledWithZeroRpcParallelism()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(HASH_PARTITION_COUNT, "128")
                .setSystemProperty(RPC_FUNCTION_PARALLELISM, "0")
                .build();
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getMetadata());
        VariableReferenceExpression left = builder.variable("left", BIGINT);
        VariableReferenceExpression right = builder.variable("right", BIGINT);
        VariableReferenceExpression output = builder.variable("output", BIGINT);
        // Keep mismatched distributed inputs so this exercises the UNION local-merge path even with RPC parallelism disabled.
        ExchangeNode roundRobin = roundRobinExchange(idAllocator.getNextId(), REMOTE_STREAMING, builder.values(left), 8);
        ExchangeNode hash = partitionedExchange(idAllocator.getNextId(), REMOTE_STREAMING, builder.values(right),
                Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of(right)), Optional.empty());
        UnionNode union = builder.union(ImmutableListMultimap.of(output, left, output, right), ImmutableList.of(roundRobin, hash));

        PlanNode optimized = new AddExchanges(getMetadata(), new PartitioningProviderManager(), false, true)
                .optimize(union, session, builder.getTypes(), new VariableAllocator(builder.getTypes().allVariables()), idAllocator, WarningCollector.NOOP, false)
                .getPlanNode();

        assertTrue(optimized instanceof UnionNode);
        assertEquals(optimized.getSources().get(0).getId(), roundRobin.getId());
        assertEquals(optimized.getSources().get(1).getId(), hash.getId());
        assertEquals(((ExchangeNode) optimized.getSources().get(0)).getPartitioningScheme().getPartitioning().getHandle(),
                createSystemPartitioning(FIXED, ROUND_ROBIN, 8));
        assertEquals(((ExchangeNode) optimized.getSources().get(1)).getPartitioningScheme().getPartitioning().getHandle(), FIXED_HASH_DISTRIBUTION);
    }

    @Test
    public void testRpcOutsideUnion()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(RPC_FUNCTION_PARALLELISM, "8")
                .build();
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getMetadata());
        VariableReferenceExpression input = builder.variable("input", BIGINT);
        VariableReferenceExpression result = builder.variable("result", BIGINT);
        RPCNode rpc = new RPCNode(Optional.empty(), idAllocator.getNextId(), builder.values(input), "test_rpc",
                ImmutableList.of(input), ImmutableList.of("input"), result, PER_ROW, 0);

        PlanNode optimized = new AddExchanges(getMetadata(), new PartitioningProviderManager(), false, true)
                .optimize(rpc, session, builder.getTypes(), new VariableAllocator(builder.getTypes().allVariables()), idAllocator, WarningCollector.NOOP, false)
                .getPlanNode();

        assertTrue(optimized instanceof RPCNode);
        assertTrue(((RPCNode) optimized).getSource() instanceof ExchangeNode);
        assertEquals(((ExchangeNode) ((RPCNode) optimized).getSource()).getPartitioningScheme().getPartitioning().getHandle(),
                createSystemPartitioning(FIXED, ROUND_ROBIN, 8));
    }
}
