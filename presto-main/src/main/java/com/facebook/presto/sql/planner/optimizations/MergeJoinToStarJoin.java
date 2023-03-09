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
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.StarJoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;
import java.util.logging.Logger;

import static com.facebook.presto.SystemSessionProperties.isStarJoinEnabled;
import static com.facebook.presto.SystemSessionProperties.isStarJoinLogEnabled;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.jsonLogicalPlan;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class MergeJoinToStarJoin
        implements PlanOptimizer
{
    private static final Logger log = Logger.getLogger(MergeJoinToStarJoin.class.getName());
    private final FunctionAndTypeManager functionAndTypeManager;

    public MergeJoinToStarJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = functionAndTypeManager;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isStarJoinEnabled(session)) {
            if (isStarJoinLogEnabled(session)) {
                log.info(jsonLogicalPlan(plan, types, functionAndTypeManager, StatsAndCosts.empty(), session));
            }
            return SimplePlanRewriter.rewriteWith(new Rewriter(), plan);
        }
        return plan;
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private static boolean eligibleJoinNode(JoinNode joinNode)
        {
            return joinNode.getCriteria().size() == 1 && joinNode.getType() == LEFT;
        }

        @Override
        public PlanNode visitJoin(JoinNode joinNode, RewriteContext<Void> context)
        {
            if (!eligibleJoinNode(joinNode)) {
                return context.defaultRewrite(joinNode, context.get());
            }
            VariableReferenceExpression leftJoinKey = joinNode.getCriteria().get(0).getLeft();
            Optional<JoinNode.DistributionType> distributionType = joinNode.getDistributionType();
            ImmutableList.Builder<PlanNode> rightChildNodes = ImmutableList.builder();
            rightChildNodes.add(joinNode.getRight());
            ImmutableList.Builder<JoinNode.EquiJoinClause> equiJoinClauseList = ImmutableList.builder();
            equiJoinClauseList.add(joinNode.getCriteria().get(0));
            ImmutableList.Builder<Optional<RowExpression>> filterExpressionList = ImmutableList.builder();
            filterExpressionList.add(joinNode.getFilter());
            ImmutableList.Builder<Optional<VariableReferenceExpression>> rightHashVariableList = ImmutableList.builder();
            rightHashVariableList.add(joinNode.getRightHashVariable());
            // Currently only deal with multiple build with one probe
            PlanNode child = joinNode.getLeft();
            while (child instanceof JoinNode && ((JoinNode) child).getCriteria().size() == 1 && ((JoinNode) child).getCriteria().get(0).getLeft().equals(leftJoinKey)
                    && ((JoinNode) child).getType().equals(LEFT) && ((JoinNode) child).getDistributionType().equals(distributionType)) {
                JoinNode childJoin = (JoinNode) child;
                rightChildNodes.add(childJoin.getRight());
                equiJoinClauseList.add(childJoin.getCriteria().get(0));
                filterExpressionList.add(childJoin.getFilter());
                rightHashVariableList.add(childJoin.getRightHashVariable());
                checkState(childJoin.getDynamicFilters().isEmpty());
                child = childJoin.getLeft();
            }
            if (rightChildNodes.build().size() <= 1) {
                return context.defaultRewrite(joinNode, context.get());
            }
            PlanNode leftChild = context.rewrite(child, context.get());
            ImmutableList<PlanNode> rightChildren = rightChildNodes.build().stream().map(x -> context.rewrite(x, context.get())).collect(toImmutableList());

            return new StarJoinNode(
                    joinNode.getSourceLocation(),
                    joinNode.getId(),
                    LEFT,
                    leftChild,
                    rightChildren,
                    equiJoinClauseList.build(),
                    joinNode.getOutputVariables(),
                    filterExpressionList.build(),
                    joinNode.getLeftHashVariable(),
                    rightHashVariableList.build(),
                    joinNode.getDistributionType(),
                    ImmutableMap.of());
        }
    }
}
