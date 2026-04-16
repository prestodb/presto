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
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.isJoinPrefilterComplexBuildSideEnabled;
import static com.facebook.presto.SystemSessionProperties.isJoinPrefilterEnabled;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.sql.planner.PlannerUtils.addProjections;
import static com.facebook.presto.sql.planner.PlannerUtils.clonePlanNode;
import static com.facebook.presto.sql.planner.PlannerUtils.getVariableHash;
import static com.facebook.presto.sql.planner.PlannerUtils.isDeterministicScanFilterProject;
import static com.facebook.presto.sql.planner.PlannerUtils.isScanFilterProject;
import static com.facebook.presto.sql.planner.PlannerUtils.projectExpressions;
import static com.facebook.presto.sql.planner.PlannerUtils.restrictOutput;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
            Rewriter rewriter = new Rewriter(session, metadata, idAllocator, variableAllocator, metadata.getFunctionAndTypeManager());
            PlanNode rewritten = SimplePlanRewriter.rewriteWith(rewriter, plan, null);
            return PlanOptimizerResult.optimizerResult(rewritten, rewriter.isPlanChanged());
        }

        return PlanOptimizerResult.optimizerResult(plan, false);
    }

    /**
     * Finds a cloneable sub-plan from the left side of a join for prefiltering.
     * Returns the sub-plan that can be cloned, or empty if not applicable.
     *
     * Supports:
     * - scan/filter/project chains (including UNION ALL of such chains)
     * - Cross join where one child is scan/filter/project (complex mode)
     * - UnnestNode where source is scan/filter/project (complex mode)
     * - AggregationNode where source is any of the above (complex mode)
     */
    private static Optional<PlanNode> findCloneableSource(
            PlanNode node,
            Set<VariableReferenceExpression> joinKeyVars,
            FunctionAndTypeManager functionAndTypeManager,
            boolean complexEnabled)
    {
        // Peel through Filter/Project
        PlanNode peeled = node;
        while (peeled instanceof FilterNode || peeled instanceof ProjectNode) {
            if (peeled instanceof FilterNode) {
                peeled = ((FilterNode) peeled).getSource();
            }
            else {
                peeled = ((ProjectNode) peeled).getSource();
            }
        }

        // Base case: scan/filter/project (including UNION ALL when complex mode enabled)
        if (isScanFilterProject(node) && isDeterministicScanFilterProject(node, functionAndTypeManager)) {
            // UnionNode requires complex mode
            if (peeled instanceof UnionNode && !complexEnabled) {
                return Optional.empty();
            }
            return Optional.of(node);
        }

        if (!complexEnabled) {
            return Optional.empty();
        }

        // Cross join: clone the child that contains the join keys
        if (peeled instanceof JoinNode && ((JoinNode) peeled).isCrossJoin()) {
            JoinNode crossJoin = (JoinNode) peeled;
            Set<VariableReferenceExpression> leftOutputs = ImmutableSet.copyOf(crossJoin.getLeft().getOutputVariables());
            Set<VariableReferenceExpression> rightOutputs = ImmutableSet.copyOf(crossJoin.getRight().getOutputVariables());

            // Trace join keys through any Filter/Project above the cross join
            Optional<Set<VariableReferenceExpression>> resolvedKeys = resolveVariablesThroughProjectFilter(node, peeled, joinKeyVars);
            if (!resolvedKeys.isPresent()) {
                return Optional.empty();
            }

            if (leftOutputs.containsAll(resolvedKeys.get())
                    && isScanFilterProject(crossJoin.getLeft())
                    && isDeterministicScanFilterProject(crossJoin.getLeft(), functionAndTypeManager)) {
                return Optional.of(crossJoin.getLeft());
            }
            if (rightOutputs.containsAll(resolvedKeys.get())
                    && isScanFilterProject(crossJoin.getRight())
                    && isDeterministicScanFilterProject(crossJoin.getRight(), functionAndTypeManager)) {
                return Optional.of(crossJoin.getRight());
            }
        }

        // UnnestNode: clone the source if join keys come from replicate variables
        if (peeled instanceof UnnestNode) {
            UnnestNode unnest = (UnnestNode) peeled;
            Optional<Set<VariableReferenceExpression>> resolvedKeys = resolveVariablesThroughProjectFilter(node, peeled, joinKeyVars);
            if (!resolvedKeys.isPresent()) {
                return Optional.empty();
            }
            Set<VariableReferenceExpression> replicateVars = ImmutableSet.copyOf(unnest.getReplicateVariables());

            if (replicateVars.containsAll(resolvedKeys.get())
                    && isScanFilterProject(unnest.getSource())
                    && isDeterministicScanFilterProject(unnest.getSource(), functionAndTypeManager)) {
                return Optional.of(unnest.getSource());
            }
        }

        // AggregationNode: clone the source if join keys are grouping keys
        if (peeled instanceof AggregationNode) {
            AggregationNode agg = (AggregationNode) peeled;
            Optional<Set<VariableReferenceExpression>> resolvedKeys = resolveVariablesThroughProjectFilter(node, peeled, joinKeyVars);
            if (!resolvedKeys.isPresent()) {
                return Optional.empty();
            }
            Set<VariableReferenceExpression> groupingKeys = ImmutableSet.copyOf(agg.getGroupingKeys());

            if (agg.getStep() == AggregationNode.Step.SINGLE
                    && agg.getGroupingSetCount() == 1
                    && !agg.hasEmptyGroupingSet()
                    && groupingKeys.containsAll(resolvedKeys.get())
                    && isScanFilterProject(agg.getSource())
                    && isDeterministicScanFilterProject(agg.getSource(), functionAndTypeManager)) {
                return Optional.of(agg.getSource());
            }
        }

        return Optional.empty();
    }

    /**
     * Traces join key variables backward through Filter/Project nodes
     * between the top node and the target node to find the source variables
     * they depend on. Returns empty if a projected expression is not a simple
     * variable reference (computed join keys cannot be safely traced).
     */
    private static Optional<Set<VariableReferenceExpression>> resolveVariablesThroughProjectFilter(
            PlanNode top,
            PlanNode target,
            Set<VariableReferenceExpression> vars)
    {
        if (top == target) {
            return Optional.of(vars);
        }

        // Build a chain of nodes from top to target
        PlanNode current = top;
        Set<VariableReferenceExpression> resolved = vars;

        while (current != target) {
            if (current instanceof ProjectNode) {
                ProjectNode project = (ProjectNode) current;
                ImmutableSet.Builder<VariableReferenceExpression> newResolved = ImmutableSet.builder();
                for (VariableReferenceExpression var : resolved) {
                    RowExpression expr = project.getAssignments().getMap().get(var);
                    if (expr instanceof VariableReferenceExpression) {
                        newResolved.add((VariableReferenceExpression) expr);
                    }
                    else if (expr != null) {
                        // Expression is not a simple variable reference;
                        // computed join keys cannot be safely traced
                        return Optional.empty();
                    }
                    else {
                        newResolved.add(var);
                    }
                }
                resolved = newResolved.build();
                current = project.getSource();
            }
            else if (current instanceof FilterNode) {
                current = ((FilterNode) current).getSource();
            }
            else {
                return Optional.empty();
            }
        }
        return Optional.of(resolved);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;
        private final Metadata metadata;
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;
        private final FunctionAndTypeManager functionAndTypeManager;
        private final boolean complexEnabled;
        private boolean planChanged;

        private Rewriter(Session session, Metadata metadata, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, FunctionAndTypeManager functionAndTypeManager)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "functionAndTypeManager is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.variableAllocator = requireNonNull(variableAllocator, "idAllocator is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.complexEnabled = isJoinPrefilterComplexBuildSideEnabled(session);
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode left = node.getLeft();
            PlanNode right = node.getRight();

            PlanNode rewrittenLeft = rewriteWith(this, left);
            PlanNode rewrittenRight = rewriteWith(this, right);
            List<EquiJoinClause> equiJoinClause = node.getCriteria();

            if ((node.getType() == LEFT || node.getType() == INNER)
                    && !node.getCriteria().isEmpty()) {
                List<VariableReferenceExpression> leftKeyList = equiJoinClause.stream().map(EquiJoinClause::getLeft).collect(toImmutableList());
                List<VariableReferenceExpression> rightKeyList = equiJoinClause.stream().map(EquiJoinClause::getRight).collect(toImmutableList());

                Set<VariableReferenceExpression> leftKeySet = ImmutableSet.copyOf(leftKeyList);
                Optional<PlanNode> cloneableSource = findCloneableSource(rewrittenLeft, leftKeySet, functionAndTypeManager, complexEnabled);

                if (cloneableSource.isPresent()) {
                    checkState(IntStream.range(0, leftKeyList.size()).boxed().allMatch(i -> leftKeyList.get(i).getType().equals(rightKeyList.get(i).getType())));

                    boolean hashJoinKey = leftKeyList.size() > 1 || (leftKeyList.get(0).getType().equals(VARCHAR) || leftKeyList.get(0).getType() instanceof VarcharType);

                    // First create a SELECT DISTINCT leftKey FROM left
                    Map<VariableReferenceExpression, VariableReferenceExpression> leftVarMap = new HashMap();
                    PlanNode leftKeys = clonePlanNode(cloneableSource.get(), session, metadata, idAllocator, leftKeyList, leftVarMap);
                    ImmutableList.Builder<RowExpression> expressionsToProject = ImmutableList.builder();
                    if (hashJoinKey) {
                        RowExpression hashExpression = getVariableHash(leftKeyList, functionAndTypeManager);
                        expressionsToProject.add(hashExpression);
                    }
                    else {
                        expressionsToProject.add(leftVarMap.get(leftKeyList.get(0)));
                    }
                    PlanNode projectNode = projectExpressions(leftKeys, idAllocator, variableAllocator, expressionsToProject.build(), ImmutableList.of());

                    VariableReferenceExpression rightKeyToFilter = rightKeyList.get(0);
                    RowExpression rightHashExpression = null;
                    if (hashJoinKey) {
                        rightHashExpression = getVariableHash(rightKeyList, functionAndTypeManager);
                        rightKeyToFilter = variableAllocator.newVariable(rightHashExpression);
                    }

                    // DISTINCT on the leftkey or hash if wide column
                    PlanNode filteringSource = new AggregationNode(
                            node.getLeft().getSourceLocation(),
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

                    // Apply prefilter to right side, optionally pushing below aggregation
                    rewrittenRight = applyPrefilterToRight(
                            rewrittenRight,
                            filteringSource,
                            rightKeyToFilter,
                            rightKeyList,
                            rightHashExpression,
                            hashJoinKey,
                            node);
                }
            }

            if (rewrittenLeft != node.getLeft() || rewrittenRight != node.getRight()) {
                planChanged = true;
                return replaceChildren(node, ImmutableList.of(rewrittenLeft, rewrittenRight));
            }

            return node;
        }

        private PlanNode applyPrefilterToRight(
                PlanNode rewrittenRight,
                PlanNode filteringSource,
                VariableReferenceExpression rightKeyToFilter,
                List<VariableReferenceExpression> rightKeyList,
                RowExpression rightHashExpression,
                boolean hashJoinKey,
                JoinNode originalJoin)
        {
            // Try to push the prefilter below a right-side aggregation
            if (complexEnabled) {
                Optional<PlanNode> pushed = tryPushPrefilterBelowAggregation(
                        rewrittenRight, filteringSource, rightKeyToFilter,
                        rightKeyList, rightHashExpression, hashJoinKey, originalJoin);
                if (pushed.isPresent()) {
                    return pushed.get();
                }
            }

            // Default: add prefilter on top of right side
            if (hashJoinKey) {
                rewrittenRight = addProjections(rewrittenRight, idAllocator, ImmutableMap.of(rightKeyToFilter, rightHashExpression));
            }

            VariableReferenceExpression semiJoinOutput = variableAllocator.newVariable("semiJoinOutput", BOOLEAN);
            SemiJoinNode semiJoinNode = new SemiJoinNode(
                    originalJoin.getRight().getSourceLocation(),
                    idAllocator.getNextId(),
                    originalJoin.getStatsEquivalentPlanNode(),
                    rewrittenRight,
                    filteringSource,
                    rightKeyToFilter,
                    filteringSource.getOutputVariables().get(0),
                    semiJoinOutput,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of());

            PlanNode result = new FilterNode(semiJoinNode.getSourceLocation(), idAllocator.getNextId(), semiJoinNode, semiJoinOutput);
            if (result.getOutputVariables().size() > originalJoin.getRight().getOutputVariables().size()) {
                result = restrictOutput(result, idAllocator, originalJoin.getRight().getOutputVariables());
            }
            return result;
        }

        private Optional<PlanNode> tryPushPrefilterBelowAggregation(
                PlanNode rightSide,
                PlanNode filteringSource,
                VariableReferenceExpression rightKeyToFilter,
                List<VariableReferenceExpression> rightKeyList,
                RowExpression rightHashExpression,
                boolean hashJoinKey,
                JoinNode originalJoin)
        {
            // Peel through Project nodes
            PlanNode peeled = rightSide;
            ImmutableList.Builder<ProjectNode> projectStack = ImmutableList.builder();
            while (peeled instanceof ProjectNode) {
                projectStack.add((ProjectNode) peeled);
                peeled = ((ProjectNode) peeled).getSource();
            }

            if (!(peeled instanceof AggregationNode)) {
                return Optional.empty();
            }

            AggregationNode aggNode = (AggregationNode) peeled;
            Set<VariableReferenceExpression> groupingKeys = ImmutableSet.copyOf(aggNode.getGroupingKeys());

            if (aggNode.getStep() != AggregationNode.Step.SINGLE
                    || aggNode.getGroupingSetCount() != 1
                    || aggNode.hasEmptyGroupingSet()
                    || !groupingKeys.containsAll(rightKeyList)) {
                return Optional.empty();
            }

            // Build prefilter on the aggregation's source
            PlanNode aggSource = aggNode.getSource();
            if (hashJoinKey) {
                aggSource = addProjections(aggSource, idAllocator, ImmutableMap.of(rightKeyToFilter, rightHashExpression));
            }

            VariableReferenceExpression semiJoinOutput = variableAllocator.newVariable("semiJoinOutput", BOOLEAN);
            SemiJoinNode semiJoinNode = new SemiJoinNode(
                    originalJoin.getRight().getSourceLocation(),
                    idAllocator.getNextId(),
                    originalJoin.getStatsEquivalentPlanNode(),
                    aggSource,
                    filteringSource,
                    rightKeyToFilter,
                    filteringSource.getOutputVariables().get(0),
                    semiJoinOutput,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of());

            PlanNode filtered = new FilterNode(semiJoinNode.getSourceLocation(), idAllocator.getNextId(), semiJoinNode, semiJoinOutput);

            // Restrict output to remove hash/semiJoin variables, keeping only the original agg source outputs
            filtered = restrictOutput(filtered, idAllocator, aggNode.getSource().getOutputVariables());

            // Rebuild the aggregation on top of the filtered source
            PlanNode result = new AggregationNode(
                    aggNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    filtered,
                    aggNode.getAggregations(),
                    aggNode.getGroupingSets(),
                    aggNode.getPreGroupedVariables(),
                    aggNode.getStep(),
                    aggNode.getHashVariable(),
                    aggNode.getGroupIdVariable(),
                    aggNode.getAggregationId());

            // Rebuild any peeled Project nodes on top
            List<ProjectNode> projects = projectStack.build();
            for (int i = projects.size() - 1; i >= 0; i--) {
                ProjectNode proj = projects.get(i);
                result = new ProjectNode(
                        proj.getSourceLocation(),
                        idAllocator.getNextId(),
                        result,
                        proj.getAssignments(),
                        proj.getLocality());
            }

            return Optional.of(result);
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }
    }
}
