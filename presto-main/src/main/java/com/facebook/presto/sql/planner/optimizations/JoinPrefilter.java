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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isJoinPrefilterEnabled;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.sql.planner.PlannerUtils.clonePlanNode;
import static com.facebook.presto.sql.planner.PlannerUtils.isScanFilterProject;
import static com.facebook.presto.sql.planner.PlannerUtils.projectExpressions;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static java.util.Objects.requireNonNull;

public class JoinPrefilter
        implements PlanOptimizer
{
    private final Metadata metadata;
    private boolean isEnabledForTesting;

    public JoinPrefilter(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnabledForTesting || isJoinPrefilterEnabled(session);
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isEnabled(session)) {
            Rewriter rewriter = new Rewriter(session, metadata, idAllocator, variableAllocator);
            PlanNode rewritten = SimplePlanRewriter.rewriteWith(rewriter, plan, null);
            return PlanOptimizerResult.optimizerResult(rewritten, rewriter.isPlanChanged());
        }

        return PlanOptimizerResult.optimizerResult(plan, false);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;
        private final Metadata metadata;
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;
        private boolean planChanged;

        private Rewriter(Session session, Metadata metadata, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "functionAndTypeManager is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.variableAllocator = requireNonNull(variableAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode left = node.getLeft();
            PlanNode right = node.getRight();

            PlanNode rewrittenLeft = rewriteWith(this, left);
            PlanNode rewrittenRight = rewriteWith(this, right);
            List<EquiJoinClause> equiJoinClause = node.getCriteria();

            // We apply this for only left and inner join and the left side of the join is a simple scan and the join is on one key
            if (equiJoinClause.size() == 1 &&
                    (node.getType() == LEFT || node.getType() == INNER) &&
                    isScanFilterProject(rewrittenLeft)) {
                VariableReferenceExpression leftKey = equiJoinClause.stream().map(x -> x.getLeft()).findFirst().get();
                VariableReferenceExpression rightKey = equiJoinClause.stream().map(x -> x.getRight()).findFirst().get();

                // First create a SELECT DISTINCT leftKey FROM left
                Map<VariableReferenceExpression, VariableReferenceExpression> leftVarMap = new HashMap();
                PlanNode leftKeys = clonePlanNode(rewrittenLeft, session, metadata, idAllocator, ImmutableList.of(leftKey), leftVarMap);
                PlanNode projectNode = projectExpressions(leftKeys, idAllocator, variableAllocator, ImmutableList.of(leftVarMap.get(leftKey)), ImmutableList.of());

                // DISTINCT on the leftkey
                PlanNode filteringSource = new AggregationNode(
                        leftKey.getSourceLocation(),
                        idAllocator.getNextId(),
                        projectNode,
                        ImmutableMap.of(),
                        singleGroupingSet(projectNode.getOutputVariables()),
                        projectNode.getOutputVariables(),
                        AggregationNode.Step.SINGLE,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty());

                // There should be only one output variable. Project that
                filteringSource = projectExpressions(filteringSource, idAllocator, variableAllocator, ImmutableList.of(filteringSource.getOutputVariables().get(0)), ImmutableList.of());

                // Now we add a semijoin as the right side
                VariableReferenceExpression semiJoinOutput = variableAllocator.newVariable("semiJoinOutput", BOOLEAN);
                SemiJoinNode semiJoinNode = new SemiJoinNode(
                        rightKey.getSourceLocation(),
                        idAllocator.getNextId(),
                        node.getStatsEquivalentPlanNode(),
                        rewrittenRight,
                        filteringSource,
                        rightKey,
                        filteringSource.getOutputVariables().get(0),
                        semiJoinOutput,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of());

                rewrittenRight = new FilterNode(semiJoinNode.getSourceLocation(), idAllocator.getNextId(), semiJoinNode, semiJoinOutput);
            }

            if (rewrittenLeft != node.getLeft() || rewrittenRight != node.getRight()) {
                planChanged = true;
                return replaceChildren(node, ImmutableList.of(rewrittenLeft, rewrittenRight));
            }

            return node;
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }
    }
}
