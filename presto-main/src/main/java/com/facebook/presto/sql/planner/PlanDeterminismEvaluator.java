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

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;

public class PlanDeterminismEvaluator
{

    public static boolean isDeterministic(PlanNode planNode, RowExpressionDeterminismEvaluator rowExpressionDeterminismEvaluator)
    {
        return planNode.accept(new Visitor(rowExpressionDeterminismEvaluator), null);
    }

    public static class Visitor
            extends InternalPlanVisitor<Boolean, Void>
    {
        private final RowExpressionDeterminismEvaluator rowExpressionDeterminismEvaluator;

        public Visitor(RowExpressionDeterminismEvaluator rowExpressionDeterminismEvaluator)
        {
            this.rowExpressionDeterminismEvaluator = rowExpressionDeterminismEvaluator;
        }

        @Override
        public Boolean visitPlan(PlanNode node, Void context)
        {
            for (PlanNode source : node.getSources()) {
                if (!source.accept(this, context)) {
                    return false;
                }
            }
            //ToDo: Add checks for more PlanNodes
            return true;
        }

        @Override
        public Boolean visitFilter(FilterNode node, Void context)
        {
            boolean isDeterministic = rowExpressionDeterminismEvaluator.isDeterministic(node.getPredicate());
            return isDeterministic && node.getSource().accept(this, context);
        }

        @Override
        public Boolean visitProject(ProjectNode node, Void context)
        {
            boolean allAssignmentsDeterministic = node.getAssignments().getExpressions().stream()
                    .allMatch(rowExpressionDeterminismEvaluator::isDeterministic);
            return allAssignmentsDeterministic && node.getSource().accept(this, context);
        }

        @Override
        public Boolean visitJoin(JoinNode node, Void context)
        {
            boolean allCriteriaDeterministic = node.getCriteria().stream()
                    .allMatch(criterion -> rowExpressionDeterminismEvaluator.isDeterministic(criterion.getLeft())
                            && rowExpressionDeterminismEvaluator.isDeterministic(criterion.getRight()));
            boolean filterDeterministic = node.getFilter().map(rowExpressionDeterminismEvaluator::isDeterministic).orElse(true);
            return allCriteriaDeterministic && filterDeterministic
                    && node.getLeft().accept(this, context) && node.getRight().accept(this, context);
        }

        @Override
        public Boolean visitAggregation(AggregationNode node, Void context)
        {
            boolean allAggregationsDeterministic = node.getAggregations().values().stream()
                    .allMatch(aggregation -> rowExpressionDeterminismEvaluator.isDeterministic(aggregation.getCall()));
            return allAggregationsDeterministic && node.getSource().accept(this, context);
        }

        @Override
        public Boolean visitWindow(WindowNode node, Void context)
        {
            boolean allWindowFunctionsDeterministic = node.getWindowFunctions().values().stream()
                    .allMatch(windowFunction -> rowExpressionDeterminismEvaluator.isDeterministic(windowFunction.getFunctionCall()));
            return allWindowFunctionsDeterministic && node.getSource().accept(this, context);
        }

        @Override
        public Boolean visitLateralJoin(LateralJoinNode node, Void context)
        {
            return node.getInput().accept(this, context) && node.getSubquery().accept(this, context);
        }

        @Override
        public Boolean visitApply(ApplyNode node, Void context)
        {
            return node.getInput().accept(this, context) && node.getSubquery().accept(this, context);
        }
    }
}