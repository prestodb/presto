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
import com.facebook.presto.cost.PredicateCPUCostEstimator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import java.util.List;

import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.SWITCH;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ReorderPredicates
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private static boolean canSwap(RowExpression pred)
        {
            if (pred instanceof SpecialFormExpression) {
                // Special case: when filtering on a subquery output
                // like 'WHERE a = (SELECT b from c)'
                // we must place the check for the subquery size first
                // meaning we cannot swap.
                return ((SpecialFormExpression) pred).getForm() != SWITCH;
            }
            return true;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> v)
        {
            return new FilterNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getSource(),
                    reorderPredicates(node, node.getPredicate()));
        }

        private RowExpression reorderPredicates(FilterNode planNode, RowExpression expression)
        {
            if (expression instanceof SpecialFormExpression) {
                SpecialFormExpression specialFormExpr = (SpecialFormExpression) expression;
                SpecialFormExpression.Form op = specialFormExpr.getForm();
                if (!(op == SpecialFormExpression.Form.AND || op == SpecialFormExpression.Form.OR)) {
                    return expression;
                }

                List<RowExpression> binaryOpArgs = specialFormExpr.getArguments();
                checkState(binaryOpArgs.size() == 2);
                RowExpression leftPred = binaryOpArgs.get(0);
                RowExpression rightPred = binaryOpArgs.get(1);

                int leftCost = PredicateCPUCostEstimator.estimateCost(leftPred);
                int rightCost = PredicateCPUCostEstimator.estimateCost(rightPred);

                RowExpression newLeftSide = reorderPredicates(planNode, leftPred);
                RowExpression newRightSide = reorderPredicates(planNode, rightPred);

                if (rightCost < leftCost && canSwap(leftPred) && canSwap(rightPred)) {
                    RowExpression swapTemp = newLeftSide;
                    newLeftSide = newRightSide;
                    newRightSide = swapTemp;
                }

                return new SpecialFormExpression(op, specialFormExpr.getType(), newLeftSide, newRightSide);
            }
            return expression;
        }
    }
}
