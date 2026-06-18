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
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isPushFilterThroughSelectingAggregation;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Pushes HAVING-style filters on the output of single-value-selecting aggregates
 * (MAX, MIN) below the aggregation when the predicate direction matches the
 * aggregate's selection semantics.
 *
 * <p>Examples:
 * <pre>
 *   SELECT g, max(x) FROM t GROUP BY g HAVING max(x) >= 1.0
 *   →
 *   SELECT g, max(x) FROM t WHERE x >= 1.0 GROUP BY g
 * </pre>
 *
 * <pre>
 *   SELECT g, min(x) FROM t GROUP BY g HAVING min(x) < 5
 *   →
 *   SELECT g, min(x) FROM t WHERE x < 5 GROUP BY g
 * </pre>
 *
 * <p>Safety conditions:
 * <ul>
 *   <li>The aggregation must have exactly one aggregate (otherwise pushing the filter
 *       below changes the row set seen by other aggregates).
 *   <li>The aggregate must be {@code MAX} or {@code MIN} of a direct column reference
 *       (no expressions, DISTINCT, FILTER, MASK, or ORDER BY).
 *   <li>The predicate direction must match the aggregate's selection direction:
 *       {@code MAX} accepts {@code >} / {@code >=}; {@code MIN} accepts {@code <} /
 *       {@code <=}.
 *   <li>The non-aggregate side of the comparison must be evaluable below the
 *       aggregation (literals, grouping keys, raw source columns).
 * </ul>
 *
 * <p>Equality on MAX/MIN gets a special "ADD pre-filter, KEEP HAVING" shape:
 * <ul>
 *   <li>{@code HAVING max(x) = c} → add {@code WHERE x >= c} below, keep the HAVING above.
 *   <li>{@code HAVING min(x) = c} → add {@code WHERE x <= c} below, keep the HAVING above.
 * </ul>
 * Pushing {@code WHERE x = c} directly would accept spurious groups (e.g. {@code {0.5, 1.0, 2.0}}
 * with {@code c = 1.0}: original max=2.0 fails the HAVING; a {@code x = c} rewrite would
 * pass it). The relaxed pre-filter ({@code x >= c} for MAX, {@code x <= c} for MIN) is
 * implied by the original predicate, so it's safe — and the HAVING above still rejects
 * groups that would have failed it originally.
 *
 * <p>For global aggregations (no GROUP BY), the HAVING clause is always kept to preserve
 * semantics. Example: {@code SELECT MAX(x) FROM t HAVING MAX(x) >= 100} returns empty when
 * no rows match, but {@code SELECT MAX(x) FROM t WHERE x >= 100} returns one row with NULL.
 * By keeping the HAVING, we get the benefit of early filtering while maintaining correctness.
 */
public class PushFilterThroughSelectingAggregation
        implements Rule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final Metadata metadata;
    private final FunctionResolution functionResolution;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;

    public PushFilterThroughSelectingAggregation(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager());
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushFilterThroughSelectingAggregation(session);
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        // Walk down through any chain of ProjectNodes (resolving GroupReferences via Lookup) until
        // we hit an AggregationNode. The planner often inserts a Project (e.g. for hash columns)
        // between the FilterNode for HAVING and the AggregationNode.
        List<ProjectNode> projectChain = new ArrayList<>();
        PlanNode current = context.getLookup().resolve(filterNode.getSource());
        while (current instanceof ProjectNode) {
            projectChain.add((ProjectNode) current);
            current = context.getLookup().resolve(((ProjectNode) current).getSource());
        }
        if (!(current instanceof AggregationNode)) {
            return Result.empty();
        }
        AggregationNode aggregationNode = (AggregationNode) current;

        Optional<EligibleAggregate> maybeEligibleAggregate = extractEligibleAggregate(aggregationNode);
        if (!maybeEligibleAggregate.isPresent()) {
            return Result.empty();
        }
        EligibleAggregate eligibleAggregate = maybeEligibleAggregate.get();

        // Verify each Project in between passes the aggregate output through as identity — otherwise
        // the FilterNode's reference doesn't actually identify the aggregate's value.
        for (ProjectNode project : projectChain) {
            RowExpression assignment = project.getAssignments().get(eligibleAggregate.aggregationOutput);
            if (!eligibleAggregate.aggregationOutput.equals(assignment)) {
                return Result.empty();
            }
        }

        // The aggregation's source — possibly a FilterNode we've already pushed below before.
        PlanNode aggregationSourceReference = aggregationNode.getSource();
        PlanNode resolvedAggregationSource = context.getLookup().resolve(aggregationSourceReference);
        Set<VariableReferenceExpression> sourceOutputs = ImmutableSet.copyOf(resolvedAggregationSource.getOutputVariables());

        List<RowExpression> originalConjuncts = extractConjuncts(filterNode.getPredicate());
        List<RowExpression> pushedDown = new ArrayList<>();
        List<RowExpression> remaining = new ArrayList<>();

        // For global aggregations (no GROUP BY), we must keep the HAVING clause to preserve
        // semantics. Example: SELECT MAX(x) FROM t HAVING MAX(x) >= 100 returns empty when
        // no rows match, but SELECT MAX(x) FROM t WHERE x >= 100 returns one row with NULL.
        boolean isGlobalAggregation = aggregationNode.getGroupingKeys().isEmpty();

        for (RowExpression conjunct : originalConjuncts) {
            Optional<PushdownResult> result = tryPushdown(conjunct, eligibleAggregate, sourceOutputs, isGlobalAggregation);
            if (result.isPresent()) {
                pushedDown.add(result.get().pushed);
                if (result.get().keepOriginal) {
                    remaining.add(conjunct);
                }
            }
            else {
                remaining.add(conjunct);
            }
        }

        if (pushedDown.isEmpty()) {
            return Result.empty();
        }

        // Dedupe against existing source filter to avoid re-pushing the same predicate on subsequent
        // iterations (relevant for the keep-HAVING shape where the original conjunct stays above).
        List<RowExpression> existingSourceConjuncts;
        PlanNode underlyingSource;
        if (resolvedAggregationSource instanceof FilterNode) {
            existingSourceConjuncts = extractConjuncts(((FilterNode) resolvedAggregationSource).getPredicate());
            underlyingSource = ((FilterNode) resolvedAggregationSource).getSource();
        }
        else {
            existingSourceConjuncts = ImmutableList.of();
            underlyingSource = aggregationSourceReference;
        }

        List<RowExpression> newPushed = pushedDown.stream()
                .filter(p -> !existingSourceConjuncts.contains(p))
                .collect(toImmutableList());
        if (newPushed.isEmpty()) {
            return Result.empty();
        }

        List<RowExpression> mergedSourceConjuncts = ImmutableList.<RowExpression>builder()
                .addAll(existingSourceConjuncts)
                .addAll(newPushed)
                .build();

        FilterNode pushedFilter = new FilterNode(
                filterNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                underlyingSource,
                and(mergedSourceConjuncts));

        AggregationNode newAggregation = (AggregationNode) aggregationNode.replaceChildren(ImmutableList.of(pushedFilter));

        // Walk back up through the project chain, top-of-chain at projectChain.get(0).
        PlanNode wrapped = newAggregation;
        for (int i = projectChain.size() - 1; i >= 0; i--) {
            wrapped = projectChain.get(i).replaceChildren(ImmutableList.of(wrapped));
        }

        if (remaining.isEmpty()) {
            return Result.ofPlanNode(wrapped);
        }
        return Result.ofPlanNode(new FilterNode(
                filterNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                wrapped,
                and(remaining)));
    }

    private Optional<EligibleAggregate> extractEligibleAggregate(AggregationNode aggregationNode)
    {
        // Single aggregate only — pushing the filter below would change the row set seen by other aggregates.
        Map<VariableReferenceExpression, Aggregation> aggregations = aggregationNode.getAggregations();
        if (aggregations.size() != 1) {
            return Optional.empty();
        }
        Map.Entry<VariableReferenceExpression, Aggregation> entry = aggregations.entrySet().iterator().next();
        VariableReferenceExpression aggregationOutput = entry.getKey();
        Aggregation aggregation = entry.getValue();

        if (aggregation.isDistinct() || aggregation.getFilter().isPresent() || aggregation.getMask().isPresent() || aggregation.getOrderBy().isPresent()) {
            return Optional.empty();
        }

        CallExpression call = aggregation.getCall();
        if (call.getArguments().size() != 1) {
            return Optional.empty();
        }
        RowExpression argumentExpression = call.getArguments().get(0);
        if (!(argumentExpression instanceof VariableReferenceExpression)) {
            return Optional.empty();
        }
        VariableReferenceExpression argumentVariable = (VariableReferenceExpression) argumentExpression;

        FunctionHandle functionHandle = call.getFunctionHandle();
        AggregationKind kind;
        if (functionResolution.isMaxFunction(functionHandle)) {
            kind = AggregationKind.MAX;
        }
        else if (functionResolution.isMinFunction(functionHandle)) {
            kind = AggregationKind.MIN;
        }
        else {
            return Optional.empty();
        }
        return Optional.of(new EligibleAggregate(aggregationOutput, argumentVariable, kind));
    }

    private Optional<PushdownResult> tryPushdown(RowExpression conjunct, EligibleAggregate eligibleAggregate, Set<VariableReferenceExpression> sourceOutputs, boolean isGlobalAggregation)
    {
        if (!(conjunct instanceof CallExpression)) {
            return Optional.empty();
        }
        CallExpression call = (CallExpression) conjunct;
        if (!functionResolution.isComparisonFunction(call.getFunctionHandle())) {
            return Optional.empty();
        }
        if (call.getArguments().size() != 2) {
            return Optional.empty();
        }
        // Don't move non-deterministic expressions across plan nodes — semantics change.
        if (!determinismEvaluator.isDeterministic(conjunct)) {
            return Optional.empty();
        }

        Optional<OperatorType> maybeOperator = metadata.getFunctionAndTypeManager().getFunctionMetadata(call.getFunctionHandle()).getOperatorType();
        if (!maybeOperator.isPresent()) {
            return Optional.empty();
        }
        OperatorType operator = maybeOperator.get();

        RowExpression leftArgument = call.getArguments().get(0);
        RowExpression rightArgument = call.getArguments().get(1);

        // Non-aggregate side must reference only variables available below the aggregation
        // (literals, grouping keys, raw source columns — but not other aggregate outputs).
        OperatorType normalizedOperator;
        RowExpression otherSide;
        if (eligibleAggregate.aggregationOutput.equals(leftArgument) && isEvaluableBelow(rightArgument, sourceOutputs)) {
            normalizedOperator = operator;
            otherSide = rightArgument;
        }
        else if (eligibleAggregate.aggregationOutput.equals(rightArgument) && isEvaluableBelow(leftArgument, sourceOutputs)) {
            normalizedOperator = OperatorType.flip(operator);
            otherSide = leftArgument;
        }
        else {
            return Optional.empty();
        }

        // Decide what operator to push and whether to keep the original HAVING above.
        // - MAX(x) >= c / > c → push x >= c / x > c, drop HAVING (REPLACE)
        // - MAX(x) = c       → push x >= c, keep HAVING (filter dominated rows; HAVING still excludes max > c groups)
        // - MIN(x) <= c / < c → push x <= c / x < c, drop HAVING (REPLACE)
        // - MIN(x) = c       → push x <= c, keep HAVING
        OperatorType pushedOperator;
        boolean keepOriginal;
        switch (eligibleAggregate.kind) {
            case MAX:
                if (normalizedOperator == OperatorType.GREATER_THAN || normalizedOperator == OperatorType.GREATER_THAN_OR_EQUAL) {
                    pushedOperator = normalizedOperator;
                    keepOriginal = false;
                }
                else if (normalizedOperator == OperatorType.EQUAL) {
                    pushedOperator = OperatorType.GREATER_THAN_OR_EQUAL;
                    keepOriginal = true;
                }
                else {
                    return Optional.empty();
                }
                break;
            case MIN:
                if (normalizedOperator == OperatorType.LESS_THAN || normalizedOperator == OperatorType.LESS_THAN_OR_EQUAL) {
                    pushedOperator = normalizedOperator;
                    keepOriginal = false;
                }
                else if (normalizedOperator == OperatorType.EQUAL) {
                    pushedOperator = OperatorType.LESS_THAN_OR_EQUAL;
                    keepOriginal = true;
                }
                else {
                    return Optional.empty();
                }
                break;
            default:
                return Optional.empty();
        }

        // For global aggregations, always keep the HAVING clause to preserve semantics.
        // Example: SELECT MAX(x) FROM t HAVING MAX(x) >= 100 returns empty when no rows
        // match, but SELECT MAX(x) FROM t WHERE x >= 100 returns one row with NULL.
        if (isGlobalAggregation) {
            keepOriginal = true;
        }

        FunctionHandle pushedHandle = functionResolution.comparisonFunction(pushedOperator, eligibleAggregate.argumentVariable.getType(), otherSide.getType());
        RowExpression pushed = new CallExpression(
                pushedOperator.name(),
                pushedHandle,
                BOOLEAN,
                ImmutableList.of(eligibleAggregate.argumentVariable, otherSide));
        return Optional.of(new PushdownResult(pushed, keepOriginal));
    }

    private static boolean isEvaluableBelow(RowExpression expression, Set<VariableReferenceExpression> sourceOutputs)
    {
        // The non-aggregate side can be anything whose variables are all available below the
        // aggregation: literals, CAST expressions, references to grouping keys / raw source
        // columns, etc. It just must not depend on any aggregate output.
        return sourceOutputs.containsAll(VariablesExtractor.extractAll(expression));
    }

    private enum AggregationKind
    {
        MAX,
        MIN
    }

    private static final class EligibleAggregate
    {
        final VariableReferenceExpression aggregationOutput;
        final VariableReferenceExpression argumentVariable;
        final AggregationKind kind;

        EligibleAggregate(VariableReferenceExpression aggregationOutput, VariableReferenceExpression argumentVariable, AggregationKind kind)
        {
            this.aggregationOutput = aggregationOutput;
            this.argumentVariable = argumentVariable;
            this.kind = kind;
        }
    }

    private static final class PushdownResult
    {
        final RowExpression pushed;
        final boolean keepOriginal;

        PushdownResult(RowExpression pushed, boolean keepOriginal)
        {
            this.pushed = pushed;
            this.keepOriginal = keepOriginal;
        }
    }
}
