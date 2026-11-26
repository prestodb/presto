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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.SystemPartitioningHandle;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RPCNode;
import com.facebook.presto.sql.planner.sanity.ValidateDependenciesChecker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.RPC_DEFERRED_COLLECT_TASK_COUNT;
import static com.facebook.presto.SystemSessionProperties.RPC_DEFERRED_POLL_TASK_COUNT;
import static com.facebook.presto.SystemSessionProperties.RPC_FUNCTION_PARALLELISM;
import static com.facebook.presto.SystemSessionProperties.getRpcDeferredCollectTaskCount;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.HASH;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.ROUND_ROBIN;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitioning.FIXED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.createSystemPartitioning;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.roundRobinExchange;
import static com.facebook.presto.sql.planner.plan.RPCNode.ExecutionPhase.COLLECT;
import static com.facebook.presto.sql.planner.plan.RPCNode.ExecutionPhase.POLL;
import static com.facebook.presto.sql.planner.plan.RPCNode.ExecutionPhase.SUBMIT;
import static com.facebook.presto.sql.planner.plan.RPCNode.StreamingMode.BATCH;
import static com.facebook.presto.sql.planner.plan.RPCNode.StreamingMode.PER_ROW;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

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
    @DataProvider
    public Object[][] deferredTaskCounts()
    {
        return new Object[][] {{1, 1}, {1, 2}, {2, 3}};
    }

    @Test(dataProvider = "deferredTaskCounts")
    public void testDeferredStagesKeepBothShuffleBoundaries(int pollTasks, int collectTasks)
    {
        assertDeferredParentPlanning(pollTasks, collectTasks, DeferredParent.NONE);
    }

    @Test(dataProvider = "deferredTaskCounts")
    public void testDeferredStagesPreserveGlobalAggregationRequirements(int pollTasks, int collectTasks)
    {
        assertDeferredParentPlanning(pollTasks, collectTasks, DeferredParent.GLOBAL_AGGREGATION);
    }

    @Test(dataProvider = "deferredTaskCounts")
    public void testDeferredStagesPreserveGroupingOnCollectedResult(int pollTasks, int collectTasks)
    {
        assertDeferredParentPlanning(pollTasks, collectTasks, DeferredParent.GROUPED_AGGREGATION);
    }

    @DataProvider
    public Object[][] deferredCollectTaskCounts()
    {
        return new Object[][] {
                {17, null, 0, 17},
                {146, null, 0, 146},
                {17, 0, 0, 17},
                {146, 0, 0, 146},
                {17, 1, 1, 1},
                {146, 1, 1, 1},
                {17, 8, 8, 8},
                {146, 8, 8, 8},
                {512, 512, 512, 512},
        };
    }

    @Test(dataProvider = "deferredCollectTaskCounts")
    public void testDeferredCollectTaskCountPlanning(int queryTasks, Integer collectTasks, int expectedCollectTasks, int expectedInputTasks)
    {
        Session session = deferredCollectSession(queryTasks, collectTasks);
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, ids, getMetadata());
        VariableReferenceExpression input = builder.variable("input", VARCHAR);
        VariableReferenceExpression result = builder.variable("result", VARCHAR);
        VariableAllocator variables = new VariableAllocator(builder.getTypes().allVariables());
        PlanNode deferred = buildDeferredRpc(session, builder.values(input), input, result, variables, ids);

        PlanNode optimized = new AddExchanges(getMetadata(), new PartitioningProviderManager(), true, true)
                .optimize(deferred, session, builder.getTypes(), variables, ids, WarningCollector.NOOP, false).getPlanNode();

        assertEquals(optimized.getId(), deferred.getId());
        assertDeferredPhaseCounts(optimized, expectedCollectTasks);
        assertEquals(inputPartitionCount(optimized, queryTasks), expectedInputTasks);
        new ValidateDependenciesChecker().validate(optimized);
    }

    @DataProvider
    public Object[][] inheritedDeferredCollectTaskCounts()
    {
        return new Object[][] {{null}, {0}};
    }

    @Test(dataProvider = "inheritedDeferredCollectTaskCounts")
    public void testInheritedCollectPartitionCountRemainsUnspecified(Integer collectTasks)
    {
        Session session = deferredCollectSession(512, collectTasks);
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, ids, getMetadata());
        VariableReferenceExpression input = builder.variable("input", VARCHAR);
        VariableReferenceExpression result = builder.variable("result", VARCHAR);
        VariableAllocator variables = new VariableAllocator(builder.getTypes().allVariables());
        PlanNode deferred = buildDeferredRpc(session, builder.values(input), input, result, variables, ids);

        PlanNode optimized = new AddExchanges(getMetadata(), new PartitioningProviderManager(), true, true)
                .optimize(deferred, session, builder.getTypes(), variables, ids, WarningCollector.NOOP, false).getPlanNode();

        RPCNode collect = findPhase(optimized, COLLECT);
        assertTrue(collect.getSource() instanceof ExchangeNode);
        ExchangeNode collectExchange = (ExchangeNode) collect.getSource();
        assertEquals(collectExchange.getScope(), REMOTE_STREAMING);
        assertEquals(collectExchange.getType(), REPARTITION);
        assertEquals(((SystemPartitioningHandle) collectExchange.getPartitioningScheme().getPartitioning().getHandle().getConnectorHandle()).getPartitionCount(), Optional.empty());
        assertEquals(collectExchange.getPartitioningScheme().getPartitioning().getHandle(), FIXED_HASH_DISTRIBUTION);
        assertEquals(collect.getRecommendedTaskCount(), 0);
        assertDeferredExchange(findPhase(collect.getSource(), POLL), 1);
        new ValidateDependenciesChecker().validate(optimized);
    }

    @Test
    public void testDeferredTaskCountSessionDefaults()
    {
        Session session = deferredCollectSession(17, null);
        assertEquals(session.getSystemProperty(RPC_DEFERRED_COLLECT_TASK_COUNT, Integer.class).intValue(), 0);
        assertEquals(session.getSystemProperty(RPC_DEFERRED_POLL_TASK_COUNT, Integer.class).intValue(), 1);
    }

    @Test
    public void testDeferredCollectTaskCountRejectsNegative()
    {
        PrestoException failure = expectThrows(PrestoException.class,
                () -> getRpcDeferredCollectTaskCount(deferredCollectSession(17, -1)));
        assertEquals(failure.getErrorCode(), INVALID_SESSION_PROPERTY.toErrorCode());
    }

    @DataProvider
    public Object[][] deferredUnionInputs()
    {
        List<Object[]> cases = new ArrayList<>();
        for (Object[] taskCounts : deferredCollectTaskCounts()) {
            for (boolean twoRpcBranches : new boolean[] {false, true}) {
                for (boolean rpcFirst : new boolean[] {false, true}) {
                    cases.add(new Object[] {taskCounts[0], taskCounts[1], taskCounts[2], taskCounts[3], twoRpcBranches, rpcFirst});
                }
            }
        }
        return cases.toArray(new Object[0][]);
    }

    @Test(dataProvider = "deferredUnionInputs")
    public void testDeferredCollectUnionInputPartitionCounts(
            int queryTasks,
            Integer collectTasks,
            int expectedCollectTasks,
            int expectedInputTasks,
            boolean twoRpcBranches,
            boolean rpcFirst)
    {
        Session session = deferredCollectSession(queryTasks, collectTasks);
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, ids, getMetadata());
        VariableReferenceExpression input = builder.variable("input", VARCHAR);
        VariableReferenceExpression result = builder.variable("result", VARCHAR);
        VariableReferenceExpression other = builder.variable("other", VARCHAR);
        VariableReferenceExpression otherResult = builder.variable("other_result", VARCHAR);
        VariableReferenceExpression output = builder.variable("output", VARCHAR);
        VariableAllocator variables = new VariableAllocator(builder.getTypes().allVariables());
        PlanNode deferred = buildDeferredRpc(session, builder.values(input), input, result, variables, ids);
        PlanNode otherBranch = twoRpcBranches
                ? buildDeferredRpc(session, builder.values(other), other, otherResult, variables, ids)
                : partitionedExchange(ids.getNextId(), REMOTE_STREAMING, builder.values(other),
                        Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of(other)), Optional.empty());
        VariableReferenceExpression otherOutput = twoRpcBranches ? otherResult : other;
        UnionNode union = builder.union(
                ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                        .putAll(output, rpcFirst ? ImmutableList.of(result, otherOutput) : ImmutableList.of(otherOutput, result))
                        .build(),
                rpcFirst ? ImmutableList.of(deferred, otherBranch) : ImmutableList.of(otherBranch, deferred));

        PlanNode optimized = new AddExchanges(getMetadata(), new PartitioningProviderManager(), true, true)
                .optimize(union, session, builder.getTypes(), variables, ids, WarningCollector.NOOP, false).getPlanNode();

        assertTrue(optimized instanceof UnionNode);
        assertEquals(optimized.getOutputVariables(), union.getOutputVariables());
        int expectedUnionTasks = twoRpcBranches ? expectedInputTasks : queryTasks;
        for (PlanNode source : optimized.getSources()) {
            assertEquals(inputPartitionCount(source, queryTasks), expectedUnionTasks);
        }
        PlanNode plannedDeferred = optimized.getSources().get(rpcFirst ? 0 : 1);
        if (!twoRpcBranches && expectedCollectTasks > 0) {
            assertTrue(plannedDeferred instanceof ExchangeNode);
            assertEquals(((ExchangeNode) plannedDeferred).getPartitioningScheme().getPartitioning().getHandle(), FIXED_ARBITRARY_DISTRIBUTION);
            plannedDeferred = plannedDeferred.getSources().get(0);
        }
        assertEquals(plannedDeferred.getId(), deferred.getId());
        assertDeferredPhaseCounts(plannedDeferred, expectedCollectTasks);
        PlanNode plannedOther = optimized.getSources().get(rpcFirst ? 1 : 0);
        assertEquals(plannedOther.getId(), otherBranch.getId());
        if (twoRpcBranches) {
            assertDeferredPhaseCounts(plannedOther, expectedCollectTasks);
        }
        new ValidateDependenciesChecker().validate(optimized);
    }

    @DataProvider
    public Object[][] deferredJoinInputs()
    {
        return new Object[][] {{17, true}, {146, false}};
    }

    @Test(dataProvider = "deferredJoinInputs")
    public void testDeferredCollectJoinInputPartitionCounts(int queryTasks, boolean rpcFirst)
    {
        Session session = deferredCollectSession(queryTasks, 8);
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, ids, getMetadata());
        VariableReferenceExpression input = builder.variable("input", VARCHAR);
        VariableReferenceExpression result = builder.variable("result", VARCHAR);
        VariableReferenceExpression other = builder.variable("other", VARCHAR);
        VariableAllocator variables = new VariableAllocator(builder.getTypes().allVariables());
        PlanNode deferred = buildDeferredRpc(session, builder.values(input), input, result, variables, ids);
        PlanNode ordinary = partitionedExchange(ids.getNextId(), REMOTE_STREAMING, builder.values(other),
                Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of(other)), Optional.empty());
        JoinNode join = builder.join(INNER, rpcFirst ? deferred : ordinary, rpcFirst ? ordinary : deferred,
                new EquiJoinClause(rpcFirst ? result : other, rpcFirst ? other : result))
                .withDistributionType(PARTITIONED);

        PlanNode optimized = new AddExchanges(getMetadata(), new PartitioningProviderManager(), true, true)
                .optimize(join, session, builder.getTypes(), variables, ids, WarningCollector.NOOP, false).getPlanNode();

        assertTrue(optimized instanceof JoinNode);
        assertEquals(inputPartitionCount(optimized.getSources().get(0), queryTasks), queryTasks);
        assertEquals(inputPartitionCount(optimized.getSources().get(1), queryTasks), queryTasks);
        assertEquals(nearestRemoteExchange(optimized.getSources().get(0)).getPartitioningScheme().getPartitioning().getHandle(),
                nearestRemoteExchange(optimized.getSources().get(1)).getPartitioningScheme().getPartitioning().getHandle());
        assertDeferredPhaseCounts(optimized.getSources().get(rpcFirst ? 0 : 1), 8);
        new ValidateDependenciesChecker().validate(optimized);
    }

    private Session deferredCollectSession(int queryTasks, Integer collectTasks)
    {
        Session.SessionBuilder builder = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(HASH_PARTITION_COUNT, Integer.toString(queryTasks))
                .setSystemProperty(RPC_FUNCTION_PARALLELISM, "0");
        if (collectTasks != null) {
            builder.setSystemProperty(RPC_DEFERRED_COLLECT_TASK_COUNT, Integer.toString(collectTasks));
        }
        return builder.build();
    }

    private PlanNode buildDeferredRpc(Session session, PlanNode source, VariableReferenceExpression input,
            VariableReferenceExpression result, VariableAllocator variables, PlanNodeIdAllocator ids)
    {
        DeferredRpcExecution execution = new DeferredRpcExecution()
        {
            @Override
            public DeferredRpcCall submit()
            {
                return DeferredRpcCall.builder("fake_submit", TestRpcFunctionOptimizer.fakeReceiptType())
                        .setArguments(ImmutableList.of(input)).build();
            }

            @Override
            public DeferredRpcCall poll(VariableReferenceExpression receipt)
            {
                return DeferredRpcCall.builder("fake_poll", TestRpcFunctionOptimizer.fakeReceiptType())
                        .setArguments(ImmutableList.of(receipt))
                        .setInputGroupingExpressions(ImmutableList.of(TestRpcFunctionOptimizer.fakeGrouping(receipt))).build();
            }

            @Override
            public DeferredRpcCall collect(VariableReferenceExpression ready)
            {
                return DeferredRpcCall.builder("fake_collect", VARCHAR)
                        .setArguments(ImmutableList.of(ready))
                        .setInputGroupingExpressions(ImmutableList.of(TestRpcFunctionOptimizer.fakeGrouping(ready))).build();
            }
        };
        return DeferredRpcPlanBuilder.build(Optional.empty(), source, result, ImmutableSet.of(), execution, session,
                variables, ids, getMetadata().getFunctionAndTypeManager());
    }

    private static void assertDeferredPhaseCounts(PlanNode plan, int collectTasks)
    {
        RPCNode collect = findPhase(plan, COLLECT);
        assertTrue(collect != null);
        assertEquals(collect.getRecommendedTaskCount(), collectTasks);
        assertDeferredExchange(collect, collectTasks);
        RPCNode poll = findPhase(collect.getSource(), POLL);
        assertTrue(poll != null);
        assertEquals(poll.getRecommendedTaskCount(), 1);
        assertDeferredExchange(poll, 1);
        assertTrue(findPhase(poll.getSource(), SUBMIT) != null);
    }

    private static int inputPartitionCount(PlanNode source, int queryTasks)
    {
        ExchangeNode exchange = nearestRemoteExchange(source);
        assertEquals(exchange.getType(), REPARTITION);
        assertTrue(exchange.getPartitioningScheme().getPartitioning().getHandle().getConnectorHandle() instanceof SystemPartitioningHandle);
        SystemPartitioningHandle handle = (SystemPartitioningHandle) exchange.getPartitioningScheme().getPartitioning().getHandle().getConnectorHandle();
        assertEquals(handle.getPartitioning(), FIXED);
        return handle.getPartitionCount().orElse(queryTasks);
    }

    private static ExchangeNode nearestRemoteExchange(PlanNode node)
    {
        if (node instanceof ExchangeNode && ((ExchangeNode) node).getScope().isRemote()) {
            return (ExchangeNode) node;
        }
        assertEquals(node.getSources().size(), 1);
        return nearestRemoteExchange(node.getSources().get(0));
    }

    private void assertDeferredParentPlanning(int pollTasks, int collectTasks, DeferredParent parent)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(RPC_FUNCTION_PARALLELISM, "0")
                .build();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, ids, getMetadata());
        VariableReferenceExpression input = builder.variable("input", VARCHAR);
        VariableReferenceExpression receipt = builder.variable("receipt", TestRpcFunctionOptimizer.fakeReceiptType());
        VariableReferenceExpression ready = builder.variable("ready", TestRpcFunctionOptimizer.fakeReceiptType());
        VariableReferenceExpression result = builder.variable("result", VARCHAR);
        ConstantExpression options = new ConstantExpression(utf8Slice("{}"), VARCHAR);
        RPCNode submit = new RPCNode(Optional.empty(), ids.getNextId(), Optional.empty(), builder.values(input),
                "fake_submit", ImmutableList.of(input), ImmutableList.of(input.getName()),
                receipt, BATCH, 128, 0, SUBMIT);
        RPCNode poll = new RPCNode(Optional.empty(), ids.getNextId(), Optional.empty(), submit,
                "fake_poll", ImmutableList.of(receipt, options), ImmutableList.of(receipt.getName(), "constant_options"),
                ready, BATCH, 128, pollTasks, POLL, ImmutableList.of(TestRpcFunctionOptimizer.fakeGrouping(receipt)));
        RPCNode collect = new RPCNode(Optional.empty(), ids.getNextId(), Optional.empty(), poll,
                "fake_collect", ImmutableList.of(ready, options), ImmutableList.of(ready.getName(), "constant_options"),
                result, BATCH, 128, collectTasks, COLLECT, ImmutableList.of(TestRpcFunctionOptimizer.fakeGrouping(ready)));
        PlanNode root = collect;
        if (parent != DeferredParent.NONE) {
            VariableReferenceExpression count = builder.variable("count", BIGINT);
            CallExpression countCall = new CallExpression("count", getMetadata().getFunctionAndTypeManager().lookupFunction("count", fromTypes(VARCHAR)),
                    BIGINT, ImmutableList.of(result));
            root = builder.aggregation(aggregation -> {
                aggregation.source(collect).addAggregation(count, countCall);
                if (parent == DeferredParent.GLOBAL_AGGREGATION) {
                    aggregation.globalGrouping();
                }
                else {
                    aggregation.singleGroupingSet(result);
                }
            });
        }
        PlanNode optimized = new AddExchanges(getMetadata(), new PartitioningProviderManager(), true, true)
                .optimize(root, session, builder.getTypes(), new VariableAllocator(builder.getTypes().allVariables()),
                        ids, WarningCollector.NOOP, false).getPlanNode();
        RPCNode plannedCollect = findPhase(optimized, COLLECT);
        assertEquals(plannedCollect.getExecutionPhase(), COLLECT);
        assertDeferredExchange(plannedCollect, collectTasks);
        RPCNode plannedPoll = findPhase(plannedCollect.getSource(), POLL);
        assertDeferredExchange(plannedPoll, pollTasks);
        assertTrue(findPhase(plannedPoll.getSource(), SUBMIT) != null);
        List<ExchangeNode> parentExchanges = exchangesAboveCollect(optimized);
        if (parent == DeferredParent.GLOBAL_AGGREGATION) {
            assertTrue(parentExchanges.stream().anyMatch(exchange -> exchange.getScope() == REMOTE_STREAMING && exchange.getType() == GATHER));
        }
        else if (parent == DeferredParent.GROUPED_AGGREGATION) {
            assertTrue(parentExchanges.stream().anyMatch(exchange -> exchange.getScope() == REMOTE_STREAMING && exchange.getType() == REPARTITION &&
                    exchange.getPartitioningScheme().getPartitioning().getArguments().contains(result)));
        }
        new ValidateDependenciesChecker().validate(optimized);
    }

    private enum DeferredParent
    {
        NONE,
        GLOBAL_AGGREGATION,
        GROUPED_AGGREGATION
    }

    private static List<ExchangeNode> exchangesAboveCollect(PlanNode node)
    {
        if (node instanceof RPCNode && ((RPCNode) node).getExecutionPhase() == COLLECT) {
            return ImmutableList.of();
        }
        List<ExchangeNode> exchanges = new ArrayList<>();
        if (node instanceof ExchangeNode) {
            exchanges.add((ExchangeNode) node);
        }
        node.getSources().forEach(source -> exchanges.addAll(exchangesAboveCollect(source)));
        return exchanges;
    }

    private static void assertDeferredExchange(RPCNode node, int taskCount)
    {
        assertTrue(node.getSource() instanceof ExchangeNode);
        ExchangeNode exchange = (ExchangeNode) node.getSource();
        assertEquals(exchange.getScope(), REMOTE_STREAMING);
        assertEquals(exchange.getType(), REPARTITION);
        assertEquals(exchange.getPartitioningScheme().getPartitioning().getHandle(),
                taskCount == 0 ? FIXED_HASH_DISTRIBUTION : createSystemPartitioning(FIXED, HASH, taskCount));
        assertEquals(exchange.getPartitioningScheme().getPartitioning().getArguments().get(0).getType(), VARCHAR);
    }

    private static RPCNode findPhase(PlanNode plan, RPCNode.ExecutionPhase phase)
    {
        if (plan instanceof RPCNode && ((RPCNode) plan).getExecutionPhase() == phase) {
            return (RPCNode) plan;
        }
        for (PlanNode child : plan.getSources()) {
            RPCNode found = findPhase(child, phase);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
}
