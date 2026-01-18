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

import com.facebook.presto.spi.plan.IndexJoinNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.TableFunctionProcessorNode;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.NoSuchElementException;
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
            extends InternalPlanVisitor<Void, Consumer<PlanNodeId>>
    {
        @Override
        public Void visitPlan(PlanNode node, Consumer<PlanNodeId> schedulingOrder)
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

        @Override
        public Void visitTableFunctionProcessor(TableFunctionProcessorNode node, Consumer<PlanNodeId> schedulingOrder)
        {
            if (!node.getSource().isPresent()) {
                schedulingOrder.accept(node.getId());
            }
            else {
                node.getSource().orElseThrow(NoSuchElementException::new).accept(this, schedulingOrder);
            }
            return null;
        }
    }
}
