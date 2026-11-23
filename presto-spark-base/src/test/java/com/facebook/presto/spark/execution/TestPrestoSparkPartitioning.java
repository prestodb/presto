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
package com.facebook.presto.spark.execution;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.AddExchanges;
import com.facebook.presto.sql.planner.optimizations.DeferredRpcCall;
import com.facebook.presto.sql.planner.optimizations.DeferredRpcExecution;
import com.facebook.presto.sql.planner.optimizations.DeferredRpcPlanBuilder;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RPCNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.Partitioner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.RPC_DEFERRED_COLLECT_TASK_COUNT;
import static com.facebook.presto.SystemSessionProperties.RPC_FUNCTION_PARALLELISM;
import static com.facebook.presto.SystemSessionProperties.getRpcDeferredCollectTaskCount;
import static com.facebook.presto.SystemSessionProperties.getRpcDeferredPollTaskCount;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spark.execution.AbstractPrestoSparkQueryExecution.createPartitioner;
import static com.facebook.presto.spark.execution.AbstractPrestoSparkQueryExecution.getSystemBucketToPartition;
import static com.facebook.presto.spark.util.PrestoSparkUtils.getPartitionCountFromBucketToPartition;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.HASH;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.ROUND_ROBIN;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitioning.FIXED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.createSystemPartitioning;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.facebook.presto.sql.planner.plan.RPCNode.ExecutionPhase.COLLECT;
import static com.facebook.presto.sql.planner.plan.RPCNode.ExecutionPhase.POLL;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestPrestoSparkPartitioning
{
    private static final int HASH_PARTITION_COUNT = 4;
    private static final int PHYSICAL_PARTITION_COUNT = 146;
    private static final PartitioningHandle COUNTED_ROUND_ROBIN = createSystemPartitioning(FIXED, ROUND_ROBIN, 8);
    private static final MetadataManager METADATA = createTestMetadataManager();

    @DataProvider
    public Object[][] countedHashPartitions()
    {
        return new Object[][] {{1}, {2}, {8}, {64}};
    }

    @Test(dataProvider = "countedHashPartitions")
    public void testCountedHashPartitioningHonorsExplicitCount(int partitionCount)
    {
        PartitioningHandle handle = createSystemPartitioning(FIXED, HASH, partitionCount);
        int[] mapping = getSystemBucketToPartition(handle, HASH_PARTITION_COUNT).orElseThrow(AssertionError::new);
        assertEquals(mapping, IntStream.range(0, partitionCount).toArray());
        assertEquals(createPartitioner(partitioningScheme(handle, HASH_PARTITION_COUNT)).numPartitions(), partitionCount);
        assertEquals(getPartitionCountFromBucketToPartition(mapping), partitionCount);
    }

    @Test
    public void testDeferredDefaultCountsUsePhasePartitioning()
    {
        Session session = testSessionBuilder().build();
        assertEquals(getRpcDeferredPollTaskCount(session), 1);
        assertEquals(getRpcDeferredCollectTaskCount(session), 0);
        PlanNode planned = planDeferredRpc(null);
        ExchangeNode pollExchange = deferredExchange(planned, POLL);
        assertEquals(pollExchange.getPartitioningScheme().getPartitioning().getHandle(), createSystemPartitioning(FIXED, HASH, 1));
        assertPlannedPhasePartitionCount(pollExchange, 1);
        ExchangeNode collectExchange = deferredExchange(planned, COLLECT);
        assertEquals(collectExchange.getPartitioningScheme().getPartitioning().getHandle(), FIXED_HASH_DISTRIBUTION);
        assertPlannedPhasePartitionCount(collectExchange, PHYSICAL_PARTITION_COUNT);
    }

    @DataProvider
    public Object[][] inheritedDeferredCollectTaskCounts()
    {
        return new Object[][] {{null}, {0}};
    }

    @Test(dataProvider = "inheritedDeferredCollectTaskCounts")
    public void testInheritedCollectUsesPhysicalPartitionAllocation(Integer collectTasks)
    {
        PlanNode planned = planDeferredRpc(collectTasks);
        assertPlannedPhasePartitionCount(deferredExchange(planned, POLL), 1);
        ExchangeNode collectExchange = deferredExchange(planned, COLLECT);
        int[] collectMapping = assertPlannedPhasePartitionCount(collectExchange, PHYSICAL_PARTITION_COUNT);
        int[] ordinaryMapping = getSystemBucketToPartition(FIXED_HASH_DISTRIBUTION, PHYSICAL_PARTITION_COUNT).orElseThrow(AssertionError::new);
        assertEquals(collectMapping, ordinaryMapping);
    }

    @Test
    public void testExplicitCollectCountSurvivesPhysicalPartitionAllocation()
    {
        PlanNode planned = planDeferredRpc(8);
        assertPlannedPhasePartitionCount(deferredExchange(planned, POLL), 1);
        assertPlannedPhasePartitionCount(deferredExchange(planned, COLLECT), 8);
    }

    @DataProvider
    public Object[][] deferredUnionBranchOrders()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "deferredUnionBranchOrders")
    public void testMixedDeferredUnionReconcilesEqualInitialCountsAfterPhysicalAllocation(boolean deferredFirst)
    {
        UnionNode union = planDeferredUnion(deferredFirst, false);
        for (PlanNode source : union.getSources()) {
            assertPlannedPhasePartitionCount(nearestRemoteExchange(source), PHYSICAL_PARTITION_COUNT);
        }
        PlanNode deferred = union.getSources().get(deferredFirst ? 0 : 1);
        assertTrue(deferred instanceof ExchangeNode);
        assertEquals(((ExchangeNode) deferred).getPartitioningScheme().getPartitioning().getHandle(), FIXED_ARBITRARY_DISTRIBUTION);
        assertPlannedPhasePartitionCount(deferredExchange(deferred, COLLECT), 512);
        assertPlannedPhasePartitionCount(deferredExchange(deferred, POLL), 1);
    }

    @Test(dataProvider = "deferredUnionBranchOrders")
    public void testEqualExplicitDeferredUnionCountsSurvivePhysicalAllocation(boolean deferredFirst)
    {
        UnionNode union = planDeferredUnion(deferredFirst, true);
        for (PlanNode source : union.getSources()) {
            assertTrue(source instanceof RPCNode);
            assertPlannedPhasePartitionCount(deferredExchange(source, COLLECT), 512);
            assertPlannedPhasePartitionCount(deferredExchange(source, POLL), 1);
            assertPlannedPhasePartitionCount(nearestRemoteExchange(source), 512);
        }
    }

    @Test
    public void testBucketToPartitionHonorsExplicitCount()
    {
        assertEquals(getSystemBucketToPartition(COUNTED_ROUND_ROBIN, HASH_PARTITION_COUNT).orElse(null), IntStream.range(0, 8).toArray());
    }

    @Test
    public void testBucketToPartitionIsNotClampedToHashPartitionCount()
    {
        assertEquals(getSystemBucketToPartition(createSystemPartitioning(FIXED, ROUND_ROBIN, 64), HASH_PARTITION_COUNT).orElse(null), IntStream.range(0, 64).toArray());
    }

    @Test
    public void testBucketToPartitionUnchangedForCountLessHandles()
    {
        assertEquals(getSystemBucketToPartition(FIXED_ARBITRARY_DISTRIBUTION, HASH_PARTITION_COUNT).orElse(null), IntStream.range(0, HASH_PARTITION_COUNT).toArray());
        assertEquals(getSystemBucketToPartition(FIXED_HASH_DISTRIBUTION, HASH_PARTITION_COUNT).orElse(null), IntStream.range(0, HASH_PARTITION_COUNT).toArray());
        assertFalse(getSystemBucketToPartition(FIXED_BROADCAST_DISTRIBUTION, HASH_PARTITION_COUNT).isPresent());
    }

    @Test
    public void testCreatePartitionerForCountedHandle()
    {
        assertEquals(createPartitioner(partitioningScheme(COUNTED_ROUND_ROBIN, HASH_PARTITION_COUNT)).numPartitions(), 8);
    }

    @Test
    public void testCreatePartitionerUnchangedForCountLessHandles()
    {
        assertEquals(createPartitioner(partitioningScheme(FIXED_ARBITRARY_DISTRIBUTION, HASH_PARTITION_COUNT)).numPartitions(), HASH_PARTITION_COUNT);
        assertEquals(createPartitioner(partitioningScheme(FIXED_HASH_DISTRIBUTION, HASH_PARTITION_COUNT)).numPartitions(), HASH_PARTITION_COUNT);

        Partitioner single = createPartitioner(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, Collections.emptyList()), Collections.emptyList()));
        assertEquals(single.numPartitions(), 1);
    }

    @Test
    public void testCreatePartitionerStillRejectsUnsupportedPartitioning()
    {
        IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> createPartitioner(new PartitioningScheme(Partitioning.create(FIXED_BROADCAST_DISTRIBUTION, Collections.emptyList()), Collections.emptyList())));
        assertTrue(exception.getMessage().startsWith("Unexpected partitioning"), exception.getMessage());
    }

    @Test
    public void testWriteSideAndPartitionerAgreeOnPartitionCount()
    {
        for (int requestedCount : new int[] {1, 2, 8, 64}) {
            PartitioningHandle handle = createSystemPartitioning(FIXED, ROUND_ROBIN, requestedCount);
            int[] bucketToPartition = getSystemBucketToPartition(handle, HASH_PARTITION_COUNT).orElseThrow(AssertionError::new);
            int writeSideCount = getPartitionCountFromBucketToPartition(bucketToPartition);
            assertEquals(writeSideCount, createPartitioner(partitioningScheme(handle, HASH_PARTITION_COUNT)).numPartitions());
            assertEquals(writeSideCount, requestedCount);
        }
    }

    @Test
    public void testPartitionCountIsMaxPlusOneNotArrayLength()
    {
        assertEquals(getPartitionCountFromBucketToPartition(new int[] {0, 1, 2, 3}), 4);
        assertEquals(getPartitionCountFromBucketToPartition(new int[] {0, 1, 0, 1, 0, 1}), 2);
        assertEquals(getPartitionCountFromBucketToPartition(new int[] {0}), 1);
    }

    @Test
    public void testPartitionCountRejectsEmptyMapping()
    {
        expectThrows(IllegalArgumentException.class, () -> getPartitionCountFromBucketToPartition(new int[] {}));
    }

    private static PartitioningScheme partitioningScheme(PartitioningHandle handle, int hashPartitionCount)
    {
        return new PartitioningScheme(Partitioning.create(handle, Collections.emptyList()), Collections.emptyList())
                .withBucketToPartition(getSystemBucketToPartition(handle, hashPartitionCount));
    }

    private static PlanNode planDeferredRpc(Integer collectTasks)
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setSystemProperty(SystemSessionProperties.HASH_PARTITION_COUNT, "512")
                .setSystemProperty(RPC_FUNCTION_PARALLELISM, "0");
        if (collectTasks != null) {
            sessionBuilder.setSystemProperty(RPC_DEFERRED_COLLECT_TASK_COUNT, Integer.toString(collectTasks));
        }
        Session session = sessionBuilder.build();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        VariableAllocator variables = new VariableAllocator();
        VariableReferenceExpression input = variables.newVariable("input", VARCHAR);
        VariableReferenceExpression result = variables.newVariable("result", VARCHAR);
        PlanNode deferred = buildDeferredRpc(session, ids, variables, input, result);
        return new AddExchanges(METADATA, new PartitioningProviderManager(), true, true)
                .optimize(deferred, session, TypeProvider.viewOf(variables.getVariables()), variables, ids, WarningCollector.NOOP, false).getPlanNode();
    }

    private static UnionNode planDeferredUnion(boolean deferredFirst, boolean bothDeferred)
    {
        Session session = testSessionBuilder()
                .setSystemProperty(SystemSessionProperties.HASH_PARTITION_COUNT, "512")
                .setSystemProperty(RPC_DEFERRED_COLLECT_TASK_COUNT, "512")
                .setSystemProperty(RPC_FUNCTION_PARALLELISM, "0")
                .build();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        VariableAllocator variables = new VariableAllocator();
        VariableReferenceExpression input = variables.newVariable("input", VARCHAR);
        VariableReferenceExpression result = variables.newVariable("result", VARCHAR);
        VariableReferenceExpression other = variables.newVariable("other", VARCHAR);
        VariableReferenceExpression otherResult = variables.newVariable("other_result", VARCHAR);
        VariableReferenceExpression output = variables.newVariable("output", VARCHAR);
        PlanNode deferred = buildDeferredRpc(session, ids, variables, input, result);
        PlanNode otherBranch = bothDeferred
                ? buildDeferredRpc(session, ids, variables, other, otherResult)
                : partitionedExchange(ids.getNextId(), REMOTE_STREAMING,
                        new ValuesNode(Optional.empty(), ids.getNextId(), ImmutableList.of(other), ImmutableList.of(), Optional.empty()),
                        Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of(other)), Optional.empty());
        VariableReferenceExpression otherOutput = bothDeferred ? otherResult : other;
        UnionNode union = new UnionNode(Optional.empty(), ids.getNextId(),
                deferredFirst ? ImmutableList.of(deferred, otherBranch) : ImmutableList.of(otherBranch, deferred),
                ImmutableList.of(output),
                ImmutableMap.of(output, deferredFirst ? ImmutableList.of(result, otherOutput) : ImmutableList.of(otherOutput, result)));
        PlanNode optimized = new AddExchanges(METADATA, new PartitioningProviderManager(), true, true)
                .optimize(union, session, TypeProvider.viewOf(variables.getVariables()), variables, ids, WarningCollector.NOOP, false).getPlanNode();
        assertTrue(optimized instanceof UnionNode);
        assertEquals(optimized.getOutputVariables(), union.getOutputVariables());
        return (UnionNode) optimized;
    }

    private static PlanNode buildDeferredRpc(Session session, PlanNodeIdAllocator ids, VariableAllocator variables,
            VariableReferenceExpression input, VariableReferenceExpression result)
    {
        ValuesNode source = new ValuesNode(Optional.empty(), ids.getNextId(), ImmutableList.of(input), ImmutableList.of(), Optional.empty());
        DeferredRpcExecution execution = new DeferredRpcExecution()
        {
            @Override
            public DeferredRpcCall submit()
            {
                return DeferredRpcCall.builder("fake_submit", VARCHAR).setArguments(ImmutableList.of(input)).build();
            }

            @Override
            public DeferredRpcCall poll(VariableReferenceExpression receipt)
            {
                return DeferredRpcCall.builder("fake_poll", VARCHAR).setArguments(ImmutableList.of(receipt))
                        .setInputGroupingExpressions(ImmutableList.of(receipt)).build();
            }

            @Override
            public DeferredRpcCall collect(VariableReferenceExpression ready)
            {
                return DeferredRpcCall.builder("fake_collect", VARCHAR).setArguments(ImmutableList.of(ready))
                        .setInputGroupingExpressions(ImmutableList.of(ready)).build();
            }
        };
        return DeferredRpcPlanBuilder.build(Optional.empty(), source, result, ImmutableSet.of(), execution, session,
                variables, ids, METADATA.getFunctionAndTypeManager());
    }

    private static ExchangeNode nearestRemoteExchange(PlanNode plan)
    {
        if (plan instanceof ExchangeNode && ((ExchangeNode) plan).getScope().isRemote()) {
            return (ExchangeNode) plan;
        }
        assertEquals(plan.getSources().size(), 1);
        return nearestRemoteExchange(plan.getSources().get(0));
    }

    private static ExchangeNode deferredExchange(PlanNode plan, RPCNode.ExecutionPhase phase)
    {
        RPCNode node = PlanNodeSearcher.searchFrom(plan)
                .where(candidate -> candidate instanceof RPCNode && ((RPCNode) candidate).getExecutionPhase() == phase)
                .findOnlyElement();
        assertTrue(node.getSource() instanceof ExchangeNode);
        return (ExchangeNode) node.getSource();
    }

    private static int[] assertPlannedPhasePartitionCount(ExchangeNode exchange, int expectedCount)
    {
        PartitioningScheme scheme = exchange.getPartitioningScheme();
        int[] mapping = getSystemBucketToPartition(scheme.getPartitioning().getHandle(), PHYSICAL_PARTITION_COUNT).orElseThrow(AssertionError::new);
        assertEquals(getPartitionCountFromBucketToPartition(mapping), expectedCount);
        assertEquals(createPartitioner(scheme.withBucketToPartition(Optional.of(mapping))).numPartitions(), expectedCount);
        return mapping;
    }
}
