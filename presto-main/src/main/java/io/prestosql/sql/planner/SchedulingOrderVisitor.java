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

package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.planner.plan.IndexJoinNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.function.Consumer;

public class SchedulingOrderVisitor
{
    public static List<PlanNodeId> scheduleOrder(PlanNode root)
    {
        ImmutableList.Builder<PlanNodeId> schedulingOrder = ImmutableList.builder();
        root.accept(new Visitor(), schedulingOrder::add);
        return schedulingOrder.build();
    }

    private SchedulingOrderVisitor() {}

    private static class Visitor
            extends PlanVisitor<Void, Consumer<PlanNodeId>>
    {
        @Override
        protected Void visitPlan(PlanNode node, Consumer<PlanNodeId> schedulingOrder)
        {
            for (PlanNode source : node.getSources()) {
                source.accept(this, schedulingOrder);
            }
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Consumer<PlanNodeId> schedulingOrder)
        {
            node.getRight().accept(this, schedulingOrder);
            node.getLeft().accept(this, schedulingOrder);
            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Consumer<PlanNodeId> schedulingOrder)
        {
            node.getFilteringSource().accept(this, schedulingOrder);
            node.getSource().accept(this, schedulingOrder);
            return null;
        }

        @Override
        public Void visitSpatialJoin(SpatialJoinNode node, Consumer<PlanNodeId> schedulingOrder)
        {
            node.getRight().accept(this, schedulingOrder);
            node.getLeft().accept(this, schedulingOrder);
            return null;
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Consumer<PlanNodeId> schedulingOrder)
        {
            node.getIndexSource().accept(this, schedulingOrder);
            node.getProbeSource().accept(this, schedulingOrder);
            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Consumer<PlanNodeId> schedulingOrder)
        {
            schedulingOrder.accept(node.getId());
            return null;
        }
    }
}
