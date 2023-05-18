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
import com.facebook.presto.spi.eventlistener.PlanOptimizerInformation;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OffsetNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.isSimplifyPlanWithEmptyInputEnabled;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Simplify plans with empty values node as input.
 * The node which needs special processing here is join, aggregation, union.
 * For inner join: replace with empty values node whenever probe or build is empty.
 * For example:
 * <pre>
 *     - Inner Join
 *          - Probe
 *              Empty Values
 *          - Build
 * </pre>
 * into
 * <pre>
 *     - Empty Values
 * </pre>
 *
 * For outer join: replace with empty values if outer side is empty and project node if inner side is empty
 * For example:
 * <pre>
 *     - Left Join
 *          - Probe
 *              - Scan
 *          - Build
 *              - Empty Values
 * </pre>
 * into
 * <pre>
 *     - Project
 *          assignments := NULL if output not in Scan, otherwise identity projection
 *          - Scan
 * </pre>
 *
 * For aggregation: if it has default output for empty input, stop and do not simplify, otherwise convert to empty values node
 * <pre>
 *     - Aggregation
 *          count() without group by
 *          - Empty Values
 * </pre>
 * No change for this query plan
 *
 * For Union node: if it has only one non-empty input, convert to a project node. If all inputs are empty, convert to empty values node
 */

public class SimplifyPlanWithEmptyInput
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isSimplifyPlanWithEmptyInputEnabled(session)) {
            Rewriter rewriter = new Rewriter(idAllocator);
            PlanNode rewrittenNode = SimplePlanRewriter.rewriteWith(rewriter, plan);
            if (rewriter.isPlanChanged()) {
                session.getOptimizerInformationCollector().addInformation(new PlanOptimizerInformation(SimplifyPlanWithEmptyInput.class.getSimpleName(), true, Optional.empty()));
            }
            return rewrittenNode;
        }
        return plan;
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private boolean planChanged;

        public Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.planChanged = false;
        }

        private static boolean isEmptyNode(PlanNode planNode)
        {
            return planNode instanceof ValuesNode && ((ValuesNode) planNode).getRows().size() == 0;
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenLeft = context.rewrite(node.getLeft());
            PlanNode rewrittenRight = context.rewrite(node.getRight());

            switch (node.getType()) {
                case INNER:
                    if (isEmptyNode(rewrittenLeft) || isEmptyNode(rewrittenRight)) {
                        return convertToEmptyValuesNode(node);
                    }
                    break;
                case LEFT:
                    if (isEmptyNode(rewrittenLeft)) {
                        return convertToEmptyValuesNode(node);
                    }
                    else if (isEmptyNode(rewrittenRight)) {
                        return convertJoinToProject(node, rewrittenLeft, rewrittenRight.getOutputVariables());
                    }
                    break;
                case RIGHT:
                    if (isEmptyNode(rewrittenRight)) {
                        return convertToEmptyValuesNode(node);
                    }
                    else if (isEmptyNode(rewrittenLeft)) {
                        return convertJoinToProject(node, rewrittenRight, rewrittenLeft.getOutputVariables());
                    }
                    break;
                case FULL:
                    if (isEmptyNode(rewrittenLeft) && isEmptyNode(rewrittenRight)) {
                        return convertToEmptyValuesNode(node);
                    }
                    else if (isEmptyNode(rewrittenLeft)) {
                        return convertJoinToProject(node, rewrittenRight, rewrittenLeft.getOutputVariables());
                    }
                    else if (isEmptyNode(rewrittenRight)) {
                        return convertJoinToProject(node, rewrittenLeft, rewrittenRight.getOutputVariables());
                    }
                    break;
                default:
                    break;
            }
            return node.replaceChildren(ImmutableList.of(rewrittenLeft, rewrittenRight));
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource());
            if (isEmptyNode(rewrittenSource) && !node.hasDefaultOutput()) {
                return convertToEmptyValuesNode(node);
            }
            return node.replaceChildren(ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Void> context)
        {
            List<PlanNode> rewrittenChildren = node.getSources().stream().map(x -> context.rewrite(x)).collect(toImmutableList());
            List<Integer> nonEmptyChildIndex = IntStream.range(0, rewrittenChildren.size()).filter(idx -> !isEmptyNode(rewrittenChildren.get(idx))).boxed().collect(toImmutableList());
            if (nonEmptyChildIndex.isEmpty()) {
                return convertToEmptyValuesNode(node);
            }
            else if (nonEmptyChildIndex.size() == 1) {
                this.planChanged = true;
                int index = nonEmptyChildIndex.get(0);
                Assignments.Builder builder = Assignments.builder();
                builder.putAll(node.getVariableMapping().entrySet().stream().collect(toImmutableMap(entry -> entry.getKey(), entry -> entry.getValue().get(index))));
                return new ProjectNode(node.getSourceLocation(), idAllocator.getNextId(), rewrittenChildren.get(index), builder.build(), LOCAL);
            }
            return node.replaceChildren(rewrittenChildren);
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource());
            PlanNode rewrittenFilterSource = context.rewrite(node.getFilteringSource());
            if (isEmptyNode(rewrittenSource)) {
                return convertToEmptyValuesNode(node);
            }
            else if (isEmptyNode(rewrittenFilterSource)) {
                this.planChanged = true;
                Assignments.Builder builder = Assignments.builder();
                builder.putAll(node.getOutputVariables().stream().collect(toImmutableMap(identity(), x -> x.equals(node.getSemiJoinOutput()) ? constant(false, BOOLEAN) : x)));
                return new ProjectNode(node.getSourceLocation(), idAllocator.getNextId(), rewrittenSource, builder.build(), LOCAL);
            }
            return node.replaceChildren(ImmutableList.of(rewrittenSource, rewrittenFilterSource));
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Void> context)
        {
            return convertToEmptyNodeIfInputEmpty(node, context);
        }

        @Override
        public PlanNode visitSample(SampleNode node, RewriteContext<Void> context)
        {
            return convertToEmptyNodeIfInputEmpty(node, context);
        }

        @Override
        public PlanNode visitOffset(OffsetNode node, RewriteContext<Void> context)
        {
            return convertToEmptyNodeIfInputEmpty(node, context);
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Void> context)
        {
            return convertToEmptyNodeIfInputEmpty(node, context);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            return convertToEmptyNodeIfInputEmpty(node, context);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            return convertToEmptyNodeIfInputEmpty(node, context);
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Void> context)
        {
            if (node.getCount() == 0) {
                return convertToEmptyValuesNode(node);
            }
            return convertToEmptyNodeIfInputEmpty(node, context);
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Void> context)
        {
            return convertToEmptyNodeIfInputEmpty(node, context);
        }

        @Override
        public PlanNode visitDistinctLimit(DistinctLimitNode node, RewriteContext<Void> context)
        {
            return convertToEmptyNodeIfInputEmpty(node, context);
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Void> context)
        {
            return convertToEmptyNodeIfInputEmpty(node, context);
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Void> context)
        {
            return convertToEmptyNodeIfInputEmpty(node, context);
        }

        @Override
        public PlanNode visitGroupId(GroupIdNode node, RewriteContext<Void> context)
        {
            return convertToEmptyNodeIfInputEmpty(node, context);
        }

        private PlanNode convertToEmptyValuesNode(PlanNode node)
        {
            this.planChanged = true;
            return new ValuesNode(node.getSourceLocation(), idAllocator.getNextId(), node.getOutputVariables(), ImmutableList.of(), Optional.empty());
        }

        private ProjectNode convertJoinToProject(JoinNode joinNode, PlanNode nonEmptySource, List<VariableReferenceExpression> nullVariables)
        {
            this.planChanged = true;
            Assignments.Builder builder = Assignments.builder();
            builder.putAll(joinNode.getOutputVariables().stream().collect(toImmutableMap(x -> x, x -> nullVariables.contains(x) ? constantNull(x.getSourceLocation(), x.getType()) : x)));
            return new ProjectNode(joinNode.getSourceLocation(), idAllocator.getNextId(), nonEmptySource, builder.build(), LOCAL);
        }

        private PlanNode convertToEmptyNodeIfInputEmpty(PlanNode node, RewriteContext<Void> context)
        {
            List<PlanNode> rewrittenChildren = node.getSources().stream().map(x -> context.rewrite(x)).collect(toImmutableList());
            if (rewrittenChildren.stream().allMatch(x -> isEmptyNode(x))) {
                return convertToEmptyValuesNode(node);
            }
            return node.replaceChildren(rewrittenChildren);
        }
    }
}
