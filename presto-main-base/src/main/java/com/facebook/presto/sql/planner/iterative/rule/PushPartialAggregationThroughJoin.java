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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.aggregation.BuiltInAggregationFunctionImplementation;
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isPushAggregationThroughJoin;
import static com.facebook.presto.SystemSessionProperties.isPushPartialAggregationThroughOuterJoin;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictOutputs;
import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.extractAggregationUniqueVariables;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static java.util.Objects.requireNonNull;

public class PushPartialAggregationThroughJoin
        implements Rule<AggregationNode>
{
    private static final Capture<JoinNode> JOIN_NODE = Capture.newCapture();

    private final FunctionAndTypeManager functionAndTypeManager;

    public PushPartialAggregationThroughJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(PushPartialAggregationThroughJoin::isSupportedAggregationNode)
            .with(source().matching(join().capturedAs(JOIN_NODE)));

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

        JoinType joinType = joinNode.getType();
        if (joinType != JoinType.INNER && joinType != JoinType.LEFT && joinType != JoinType.RIGHT && joinType != JoinType.FULL) {
            return Result.empty();
        }
        if (joinType != JoinType.INNER && !isPushPartialAggregationThroughOuterJoin(context.getSession())) {
            return Result.empty();
        }

        TypeProvider types = TypeProvider.viewOf(context.getVariableAllocator().getVariables());

        // A join input whose rows the join preserves exactly (both inputs of an INNER join, the
        // left input of a LEFT join, the right input of a RIGHT join) can accept any partial
        // aggregation. A null-extended input (the right input of a LEFT join, the left input of a
        // RIGHT join, both inputs of a FULL join) can only accept aggregations that ignore rows
        // whose inputs are null: for those, the null intermediate state carried by a null-extended
        // row contributes exactly what the original null arguments would have — nothing. Anything
        // else (e.g. count(*), which counts null-extended rows) would produce wrong results.
        boolean leftIsNullExtended = joinType == JoinType.RIGHT || joinType == JoinType.FULL;
        boolean rightIsNullExtended = joinType == JoinType.LEFT || joinType == JoinType.FULL;

        if (allAggregationsOn(aggregationNode.getAggregations(), joinNode.getLeft().getOutputVariables(), types)
                && (!leftIsNullExtended || allAggregationsIgnoreNullInputs(aggregationNode.getAggregations().values()))) {
            return Result.ofPlanNode(pushPartialToLeftChild(aggregationNode, joinNode, context));
        }
        if (allAggregationsOn(aggregationNode.getAggregations(), joinNode.getRight().getOutputVariables(), types)
                && (!rightIsNullExtended || allAggregationsIgnoreNullInputs(aggregationNode.getAggregations().values()))) {
            return Result.ofPlanNode(pushPartialToRightChild(aggregationNode, joinNode, context));
        }

        return Result.empty();
    }

    /**
     * Determines whether every aggregation contributes nothing for a null-extended join row.
     * That requires the function to skip rows with null inputs (e.g. min, max, sum, count(col),
     * avg) and every argument to be a column reference, so that a null-extended row implies null
     * arguments. count(*) fails the argument-count check, count(1) and sum(col + 1) fail the
     * column-reference check, and functions like array_agg (which aggregates nulls) or checksum
     * (which folds nulls into the hash) fail the null-input check.
     */
    private boolean allAggregationsIgnoreNullInputs(Collection<AggregationNode.Aggregation> aggregations)
    {
        return aggregations.stream().allMatch(aggregation ->
                !aggregation.getArguments().isEmpty()
                        && aggregation.getArguments().stream().allMatch(VariableReferenceExpression.class::isInstance)
                        && ignoresNullInputs(aggregation.getFunctionHandle()));
    }

    /**
     * FunctionMetadata#isCalledOnNullInput cannot be used here: for many built-in aggregations,
     * SqlAggregationFunction inherits BuiltInFunction#isCalledOnNullInput (always false), regardless
     * of the accumulator's actual null handling. The accumulator's parameter metadata is the
     * authoritative source: input functions only observe null rows through a
     * NULLABLE_BLOCK_INPUT_CHANNEL parameter. Implementations we cannot introspect are
     * conservatively assumed to process nulls.
     */
    private boolean ignoresNullInputs(FunctionHandle functionHandle)
    {
        AggregationFunctionImplementation implementation = functionAndTypeManager.getAggregateFunctionImplementation(functionHandle);
        if (!(implementation instanceof BuiltInAggregationFunctionImplementation)) {
            return false;
        }
        return ((BuiltInAggregationFunctionImplementation) implementation).getAggregationMetadata()
                .getValueInputMetadata().stream()
                .noneMatch(parameterMetadata -> parameterMetadata.getParameterType() == NULLABLE_BLOCK_INPUT_CHANNEL);
    }

    private boolean allAggregationsOn(Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations, List<VariableReferenceExpression> variables, TypeProvider types)
    {
        Set<VariableReferenceExpression> inputs = aggregations.values()
                .stream()
                .map(aggregation -> extractAggregationUniqueVariables(aggregation))
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
                        node.getCriteria().stream().map(EquiJoinClause::getLeft),
                        node.getCriteria().stream().map(EquiJoinClause::getRight),
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
