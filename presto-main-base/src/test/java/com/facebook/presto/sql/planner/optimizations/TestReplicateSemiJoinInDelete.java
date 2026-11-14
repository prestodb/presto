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
import com.facebook.presto.spi.plan.DeleteNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.UpdateNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.BROADCAST_SEMI_JOIN_FOR_DELETE;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.metadata.AbstractMockMetadata.dummyMetadata;
import static com.facebook.presto.spi.plan.SemiJoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.SemiJoinNode.DistributionType.REPLICATED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestReplicateSemiJoinInDelete
{
    private static final Session DELETE_BROADCAST_DISABLED = Session.builder(TEST_SESSION)
            .setSystemProperty(BROADCAST_SEMI_JOIN_FOR_DELETE, "false")
            .build();

    @Test
    public void testUpdateReplicatesPartitionedSemiJoin()
    {
        assertUpdateReplicatesPartitionedSemiJoin(TEST_SESSION);
    }

    // broadcast_semi_join_for_delete only controls DELETE; an UPDATE fails to apply its
    // write over a partitioned semi-join, so it is replicated regardless.
    @Test
    public void testUpdateReplicatesPartitionedSemiJoinWhenDeleteBroadcastDisabled()
    {
        assertUpdateReplicatesPartitionedSemiJoin(DELETE_BROADCAST_DISABLED);
    }

    @Test
    public void testDeleteReplicatesPartitionedSemiJoin()
    {
        PlanOptimizerResult result = optimizeDelete(TEST_SESSION);

        assertTrue(result.isOptimizerTriggered());
        DeleteNode rewritten = (DeleteNode) result.getPlanNode();
        assertEquals(((SemiJoinNode) rewritten.getSource()).getDistributionType(), Optional.of(REPLICATED));
    }

    @Test
    public void testDeleteKeepsPartitionedSemiJoinWhenDisabled()
    {
        PlanOptimizerResult result = optimizeDelete(DELETE_BROADCAST_DISABLED);

        assertFalse(result.isOptimizerTriggered());
        DeleteNode rewritten = (DeleteNode) result.getPlanNode();
        assertEquals(((SemiJoinNode) rewritten.getSource()).getDistributionType(), Optional.of(PARTITIONED));
    }

    private static PlanOptimizerResult optimizeDelete(Session session)
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = new PlanBuilder(session, idAllocator, dummyMetadata());
        VariableReferenceExpression rowId = planBuilder.variable("row_id");
        VariableReferenceExpression filterKey = planBuilder.variable("filter_key");
        DeleteNode delete = new DeleteNode(
                Optional.empty(),
                idAllocator.getNextId(),
                partitionedSemiJoin(planBuilder, rowId, filterKey),
                Optional.of(rowId),
                ImmutableList.of(planBuilder.variable("rows")),
                Optional.empty());

        return new ReplicateSemiJoinInDelete().optimize(
                delete,
                session,
                TypeProvider.empty(),
                new VariableAllocator(),
                new PlanNodeIdAllocator(),
                WarningCollector.NOOP,
                false);
    }

    private static SemiJoinNode partitionedSemiJoin(PlanBuilder planBuilder, VariableReferenceExpression sourceKey, VariableReferenceExpression filterKey)
    {
        return planBuilder.semiJoin(
                planBuilder.values(sourceKey),
                planBuilder.values(filterKey),
                sourceKey,
                filterKey,
                planBuilder.variable("semi_join_output", BOOLEAN),
                Optional.empty(),
                Optional.empty(),
                Optional.of(PARTITIONED));
    }

    private static void assertUpdateReplicatesPartitionedSemiJoin(Session session)
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = new PlanBuilder(session, idAllocator, dummyMetadata());
        VariableReferenceExpression sourceKey = planBuilder.variable("source_key");
        VariableReferenceExpression filterKey = planBuilder.variable("filter_key");
        VariableReferenceExpression rowId = planBuilder.variable("row_id");
        SemiJoinNode semiJoin = partitionedSemiJoin(planBuilder, sourceKey, filterKey);
        UpdateNode update = new UpdateNode(
                Optional.empty(),
                idAllocator.getNextId(),
                semiJoin,
                Optional.of(rowId),
                ImmutableList.of(planBuilder.variable("updated_value"), rowId),
                ImmutableList.of(planBuilder.variable("rows")));

        PlanOptimizerResult result = new ReplicateSemiJoinInDelete().optimize(
                update,
                session,
                TypeProvider.empty(),
                new VariableAllocator(),
                new PlanNodeIdAllocator(),
                WarningCollector.NOOP,
                false);

        assertTrue(result.isOptimizerTriggered());
        UpdateNode rewritten = (UpdateNode) result.getPlanNode();
        assertEquals(rewritten.getId(), update.getId());
        assertEquals(rewritten.getRowId(), update.getRowId());
        assertEquals(rewritten.getColumnValueAndRowIdSymbols(), update.getColumnValueAndRowIdSymbols());
        assertEquals(rewritten.getOutputVariables(), update.getOutputVariables());
        assertEquals(((SemiJoinNode) rewritten.getSource()).getDistributionType(), Optional.of(REPLICATED));
    }
}
