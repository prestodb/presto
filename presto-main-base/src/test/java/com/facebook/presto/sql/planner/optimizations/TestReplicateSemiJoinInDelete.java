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

import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
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
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.metadata.AbstractMockMetadata.dummyMetadata;
import static com.facebook.presto.spi.plan.SemiJoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.SemiJoinNode.DistributionType.REPLICATED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestReplicateSemiJoinInDelete
{
    @Test
    public void testUpdateReplicatesPartitionedSemiJoin()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, idAllocator, dummyMetadata());
        VariableReferenceExpression sourceKey = planBuilder.variable("source_key");
        VariableReferenceExpression filterKey = planBuilder.variable("filter_key");
        VariableReferenceExpression rowId = planBuilder.variable("row_id");
        SemiJoinNode semiJoin = planBuilder.semiJoin(
                planBuilder.values(sourceKey),
                planBuilder.values(filterKey),
                sourceKey,
                filterKey,
                planBuilder.variable("semi_join_output", BOOLEAN),
                Optional.empty(),
                Optional.empty(),
                Optional.of(PARTITIONED));
        UpdateNode update = new UpdateNode(
                Optional.empty(),
                idAllocator.getNextId(),
                semiJoin,
                Optional.of(rowId),
                ImmutableList.of(planBuilder.variable("updated_value"), rowId),
                ImmutableList.of(planBuilder.variable("rows")));

        PlanOptimizerResult result = new ReplicateSemiJoinInDelete().optimize(
                update,
                TEST_SESSION,
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
