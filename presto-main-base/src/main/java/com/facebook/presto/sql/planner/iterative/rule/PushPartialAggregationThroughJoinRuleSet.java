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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isPushAggregationThroughJoin;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictOutputs;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;

/**
 * Transforms:
 * <pre>
 *   Aggregation (PARTIAL)
 *       Join (INNER)
 * </pre>
 * or
 * <pre>
 *   Aggregation (PARTIAL)
 *       Project
 *           Join (INNER)
 * </pre>
 * into a plan where the partial aggregation (and the projection, if present) are pushed
 * below the join to whichever side all the aggregation inputs come from.
 * <p>
 * For the second form, the projection is split: each assignment is pushed to whichever join child
 * all of its referenced variables come from (constants are pushed to the left child). An assignment
 * that spans both sides of the join prevents the rule from firing. This split allows the partial
 * aggregation to then be pushed below the join to whichever side all aggregation inputs come from.
 */
public class PushPartialAggregationThroughJoinRuleSet
{
    private static final Capture<AggregationNode> AGGREGATION_NODE = newCapture();
    private static final Capture<JoinNode> JOIN_NODE = newCapture();
    private static final Capture<ProjectNode> PROJECT_NODE = newCapture();

    // Pattern: Agg -> Join
    private static final Pattern<AggregationNode> WITHOUT_PROJECTION =
            aggregation()
                    .matching(PushPartialAggregationThroughJoinRuleSet::isSupportedAggregationNode)
                    .capturedAs(AGGREGATION_NODE)
                    .with(source().matching(join().capturedAs(JOIN_NODE)));

    // Pattern: Agg -> Project -> Join
    private static final Pattern<AggregationNode> WITH_PROJECTION =
            aggregation()
                    .matching(PushPartialAggregationThroughJoinRuleSet::isSupportedAggregationNode)
                    .capturedAs(AGGREGATION_NODE)
                    .with(source().matching(
                            project().capturedAs(PROJECT_NODE)
                                    .with(source().matching(join().capturedAs(JOIN_NODE)))));

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                withoutProjectionRule(),
                withProjectionRule());
    }

    @VisibleForTesting
    PushPartialAggregationThroughJoin withoutProjectionRule()
    {
        return new PushPartialAggregationThroughJoin();
    }

    @VisibleForTesting
    PushPartialAggregationWithProjectThroughJoin withProjectionRule()
    {
        return new PushPartialAggregationWithProjectThroughJoin();
    }

    private static boolean isSupportedAggregationNode(AggregationNode aggregationNode)
    {
        // Don't split streaming aggregations or segmented aggregations
        if (aggregationNode.isStreamable() || aggregationNode.isSegmentedAggregationEligible()) {
            return false;
        }

        if (aggregationNode.getHashVariable().isPresent()) {
            // TODO: add support for hash symbol in aggregation node
            return false;
        }
        return aggregationNode.getStep() == PARTIAL && aggregationNode.getGroupingSetCount() == 1;
    }

    /**
     * Handles the base case: Aggregation (PARTIAL) directly above a JoinNode.
     */
    @VisibleForTesting
    static class PushPartialAggregationThroughJoin
            extends BaseRule
    {
        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return WITHOUT_PROJECTION;
        }

        @Override
        public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
        {
            JoinNode joinNode = captures.get(JOIN_NODE);
            return applyPushdown(aggregationNode, joinNode, context);
        }
    }

    /**
     * Handles the case where a ProjectNode sits between the Aggregation and the JoinNode:
     *
     * <pre>
     *   Aggregation (PARTIAL)
     *       Project
     *           Join (INNER)
     * </pre>
     *
     * The projection is split into two parts: assignments whose referenced variables come
     * entirely from the left join child are pushed to the left, and those from the right child
     * are pushed to the right. Constants (no variable references) are pushed to the left.
     * Any single assignment that references variables from both sides prevents the rule from
     * firing. After the split, the partial aggregation is pushed to whichever side all of its
     * inputs now reside on.
     */
    @VisibleForTesting
    static class PushPartialAggregationWithProjectThroughJoin
            extends BaseRule
    {
        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return WITH_PROJECTION;
        }

        @Override
        public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
        {
            ProjectNode projectNode = captures.get(PROJECT_NODE);
            JoinNode joinNode = captures.get(JOIN_NODE);

            if (joinNode.getType() != JoinType.INNER) {
                return Result.empty();
            }

            Set<VariableReferenceExpression> leftVariables = ImmutableSet.copyOf(joinNode.getLeft().getOutputVariables());
            Set<VariableReferenceExpression> rightVariables = ImmutableSet.copyOf(joinNode.getRight().getOutputVariables());

            // Split assignments: left-only (including constants with no variable refs) vs right-only.
            // Any single assignment that spans both sides is rejected outright.
            ImmutableMap.Builder<VariableReferenceExpression, RowExpression> leftAssignmentsBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<VariableReferenceExpression, RowExpression> rightAssignmentsBuilder = ImmutableMap.builder();

            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : projectNode.getAssignments().entrySet()) {
                Set<VariableReferenceExpression> referencedVars = VariablesExtractor.extractUnique(entry.getValue());
                boolean usesLeft = !intersection(referencedVars, leftVariables).isEmpty();
                boolean usesRight = !intersection(referencedVars, rightVariables).isEmpty();

                if (usesLeft && usesRight) {
                    // This expression spans both sides – cannot push
                    return Result.empty();
                }
                if (usesRight) {
                    rightAssignmentsBuilder.put(entry.getKey(), entry.getValue());
                }
                else {
                    // Belongs to left side: uses only left variables, or is a constant (no variable refs).
                    leftAssignmentsBuilder.put(entry.getKey(), entry.getValue());
                }
            }

            Map<VariableReferenceExpression, RowExpression> leftAssignments = leftAssignmentsBuilder.build();
            Map<VariableReferenceExpression, RowExpression> rightAssignments = rightAssignmentsBuilder.build();

            // Only wrap a join child in a ProjectNode when there are assignments to push to that side.
            PlanNode newLeft = leftAssignments.isEmpty()
                    ? joinNode.getLeft()
                    : buildPushedProjection(projectNode, leftAssignments, joinNode.getLeft(), context);
            PlanNode newRight = rightAssignments.isEmpty()
                    ? joinNode.getRight()
                    : buildPushedProjection(projectNode, rightAssignments, joinNode.getRight(), context);

            JoinNode newJoinNode = rebuildJoin(joinNode, newLeft, newRight);

            // Now apply the aggregation push-down logic on the rewritten Agg -> Join tree.
            return applyPushdown(aggregationNode, newJoinNode, context);
        }

        /**
         * Builds a ProjectNode over {@code joinChild} that carries the specified
         * {@code assignmentsToPush} plus identity pass-throughs for every output variable of
         * {@code joinChild}, so the join still has all the variables it needs.
         */
        private PlanNode buildPushedProjection(
                ProjectNode originalProject,
                Map<VariableReferenceExpression, RowExpression> assignmentsToPush,
                PlanNode joinChild,
                Context context)
        {
            com.facebook.presto.spi.plan.Assignments.Builder assignments =
                    com.facebook.presto.spi.plan.Assignments.builder();

            // Start with identity pass-throughs for all variables produced by the join child.
            for (VariableReferenceExpression var : joinChild.getOutputVariables()) {
                assignments.put(var, var);
            }

            // Layer on the caller-supplied assignments (already filtered to this side).
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : assignmentsToPush.entrySet()) {
                assignments.put(entry.getKey(), entry.getValue());
            }

            return new ProjectNode(
                    originalProject.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    joinChild,
                    assignments.build(),
                    LOCAL);
        }

        private JoinNode rebuildJoin(JoinNode original, PlanNode newLeft, PlanNode newRight)
        {
            return new JoinNode(
                    original.getSourceLocation(),
                    original.getId(),
                    original.getType(),
                    newLeft,
                    newRight,
                    original.getCriteria(),
                    ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(newLeft.getOutputVariables())
                            .addAll(newRight.getOutputVariables())
                            .build(),
                    original.getFilter(),
                    original.getLeftHashVariable(),
                    original.getRightHashVariable(),
                    original.getDistributionType(),
                    original.getDynamicFilters());
        }
    }

    private abstract static class BaseRule
            implements Rule<AggregationNode>
    {
        @Override
        public boolean isEnabled(Session session)
        {
            return isPushAggregationThroughJoin(session);
        }

        protected Result applyPushdown(AggregationNode aggregationNode, JoinNode joinNode, Context context)
        {
            if (joinNode.getType() != JoinType.INNER) {
                return Result.empty();
            }

            // TODO: leave partial aggregation above Join?
            if (allAggregationsOn(aggregationNode.getAggregations(), joinNode.getLeft().getOutputVariables())) {
                return Result.ofPlanNode(pushPartialToLeftChild(aggregationNode, joinNode, context));
            }
            else if (allAggregationsOn(aggregationNode.getAggregations(), joinNode.getRight().getOutputVariables())) {
                return Result.ofPlanNode(pushPartialToRightChild(aggregationNode, joinNode, context));
            }

            return Result.empty();
        }

        private boolean allAggregationsOn(Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations, List<VariableReferenceExpression> variables)
        {
            Set<VariableReferenceExpression> variableSet = ImmutableSet.copyOf(variables);
            Set<VariableReferenceExpression> inputs = aggregations.values()
                    .stream()
                    .map(AggregationNodeUtils::extractAggregationUniqueVariables)
                    .flatMap(Collection::stream)
                    .collect(toImmutableSet());
            return variableSet.containsAll(inputs);
        }

        private PlanNode pushPartialToLeftChild(AggregationNode node, JoinNode child, Context context)
        {
            Set<VariableReferenceExpression> joinLeftChildVariables = ImmutableSet.copyOf(child.getLeft().getOutputVariables());
            List<VariableReferenceExpression> groupingSet = getPushedDownGroupingSet(node, joinLeftChildVariables, intersection(getJoinRequiredVariables(child), joinLeftChildVariables));
            AggregationNode pushedAggregation = replaceAggregationSource(node, child.getLeft(), groupingSet);
            return pushPartialToJoin(node, child, pushedAggregation, child.getRight(), context);
        }

        private PlanNode pushPartialToRightChild(AggregationNode node, JoinNode child, Context context)
        {
            Set<VariableReferenceExpression> joinRightChildVariables = ImmutableSet.copyOf(child.getRight().getOutputVariables());
            List<VariableReferenceExpression> groupingSet = getPushedDownGroupingSet(node, joinRightChildVariables, intersection(getJoinRequiredVariables(child), joinRightChildVariables));
            AggregationNode pushedAggregation = replaceAggregationSource(node, child.getRight(), groupingSet);
            return pushPartialToJoin(node, child, child.getLeft(), pushedAggregation, context);
        }

        private Set<VariableReferenceExpression> getJoinRequiredVariables(JoinNode node)
        {
            return Streams.concat(
                            node.getCriteria().stream().map(EquiJoinClause::getLeft),
                            node.getCriteria().stream().map(EquiJoinClause::getRight),
                            node.getFilter().map(VariablesExtractor::extractUnique).orElse(ImmutableSet.of()).stream(),
                            node.getLeftHashVariable().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream(),
                            node.getRightHashVariable().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream())
                    .collect(toImmutableSet());
        }

        private List<VariableReferenceExpression> getPushedDownGroupingSet(AggregationNode aggregation, Set<VariableReferenceExpression> availableVariables, Set<VariableReferenceExpression> requiredJoinVariables)
        {
            List<VariableReferenceExpression> groupingSet = aggregation.getGroupingKeys();

            // keep variables that are directly from the join's child (availableVariables)
            List<VariableReferenceExpression> pushedDownGroupingSet = groupingSet.stream()
                    .filter(availableVariables::contains)
                    .collect(Collectors.toList());

            // add missing required join variables to grouping set
            Set<VariableReferenceExpression> existingVariables = new HashSet<>(pushedDownGroupingSet);
            requiredJoinVariables.stream()
                    .filter(existingVariables::add)
                    .forEach(pushedDownGroupingSet::add);

            return pushedDownGroupingSet;
        }

        private AggregationNode replaceAggregationSource(
                AggregationNode aggregation,
                PlanNode source,
                List<VariableReferenceExpression> groupingKeys)
        {
            return new AggregationNode(
                    aggregation.getSourceLocation(),
                    aggregation.getId(),
                    source,
                    aggregation.getAggregations(),
                    singleGroupingSet(groupingKeys),
                    ImmutableList.of(),
                    aggregation.getStep(),
                    aggregation.getHashVariable(),
                    aggregation.getGroupIdVariable(),
                    aggregation.getAggregationId());
        }

        private PlanNode pushPartialToJoin(
                AggregationNode aggregation,
                JoinNode child,
                PlanNode leftChild,
                PlanNode rightChild,
                Context context)
        {
            JoinNode joinNode = new JoinNode(
                    child.getSourceLocation(),
                    child.getId(),
                    child.getType(),
                    leftChild,
                    rightChild,
                    child.getCriteria(),
                    ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(leftChild.getOutputVariables())
                            .addAll(rightChild.getOutputVariables())
                            .build(),
                    child.getFilter(),
                    child.getLeftHashVariable(),
                    child.getRightHashVariable(),
                    child.getDistributionType(),
                    child.getDynamicFilters());
            return restrictOutputs(context.getIdAllocator(), joinNode, ImmutableSet.copyOf(aggregation.getOutputVariables())).orElse(joinNode);
        }
    }
}
