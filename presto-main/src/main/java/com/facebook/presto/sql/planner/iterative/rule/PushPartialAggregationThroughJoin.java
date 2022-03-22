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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isPushAggregationThroughJoin;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictOutputs;
import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.extractAggregationUniqueVariables;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;

public class PushPartialAggregationThroughJoin
        implements Rule<AggregationNode>
{
    private static final Capture<JoinNode> JOIN_NODE = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(PushPartialAggregationThroughJoin::isSupportedAggregationNode)
            .with(source().matching(join().capturedAs(JOIN_NODE)));

    private static boolean isSupportedAggregationNode(AggregationNode aggregationNode)
    {
        // Don't split streaming aggregations
        if (aggregationNode.isStreamable()) {
            return false;
        }

        if (aggregationNode.getHashVariable().isPresent()) {
            // TODO: add support for hash symbol in aggregation node
            return false;
        }
        return aggregationNode.getStep() == PARTIAL && aggregationNode.getGroupingSetCount() == 1;
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushAggregationThroughJoin(session);
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(JOIN_NODE);

        if (joinNode.getType() != JoinNode.Type.INNER) {
            return Result.empty();
        }

        // TODO: leave partial aggregation above Join?
        if (allAggregationsOn(aggregationNode.getAggregations(), joinNode.getLeft().getOutputVariables(), context.getVariableAllocator().getTypes())) {
            return Result.ofPlanNode(pushPartialToLeftChild(aggregationNode, joinNode, context));
        }
        else if (allAggregationsOn(aggregationNode.getAggregations(), joinNode.getRight().getOutputVariables(), context.getVariableAllocator().getTypes())) {
            return Result.ofPlanNode(pushPartialToRightChild(aggregationNode, joinNode, context));
        }

        return Result.empty();
    }

    private boolean allAggregationsOn(Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations, List<VariableReferenceExpression> variables, TypeProvider types)
    {
        Set<VariableReferenceExpression> inputs = aggregations.values()
                .stream()
                .map(aggregation -> extractAggregationUniqueVariables(aggregation, types))
                .flatMap(Set::stream)
                .collect(toImmutableSet());
        return variables.containsAll(inputs);
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
                node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft),
                node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight),
                node.getFilter().map(expression -> VariablesExtractor.extractUnique(expression)).orElse(ImmutableSet.of()).stream(),
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
                aggregation.getId(),
                source,
                aggregation.getAggregations(),
                singleGroupingSet(groupingKeys),
                ImmutableList.of(),
                aggregation.getStep(),
                aggregation.getHashVariable(),
                aggregation.getGroupIdVariable());
    }

    private PlanNode pushPartialToJoin(
            AggregationNode aggregation,
            JoinNode child,
            PlanNode leftChild,
            PlanNode rightChild,
            Context context)
    {
        JoinNode joinNode = new JoinNode(
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
        return restrictOutputs(context.getIdAllocator(), joinNode, ImmutableSet.copyOf(aggregation.getOutputVariables()), false).orElse(joinNode);
    }
}
