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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isRewriteApproxDistinctIfToMaskEnabled;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.relational.Expressions.isNull;
import static java.util.Objects.requireNonNull;

/**
 * Moves an IF condition out of an approx_distinct argument and onto the aggregation as a mask.
 * <p>
 * From:
 * <pre>
 *   Aggregation (approx_distinct(IF(p1, e)), approx_distinct(IF(p2, e)))
 *   - Project (IF(p1, e), IF(p2, e))
 * </pre>
 * To:
 * <pre>
 *   Aggregation (approx_distinct(e) mask p1, approx_distinct(e) mask p2)
 *   - Project (e, p1, p2)
 * </pre>
 * <p>
 * This pays off in two independent ways, and which one dominates depends on the query.
 * <p>
 * It narrows the projection below the aggregation. Written with the condition inside the argument,
 * each aggregation materializes its own conditional copy of the value, so that projection grows with
 * the number of aggregations. Once the condition is a mask, aggregations over the same value share
 * one column and what grows is a boolean per predicate. This is worth nothing when the aggregations
 * already read a handful of shared columns, since common subexpression elimination has collapsed
 * them anyway, and worth a great deal when each one reads a different expression.
 * <p>
 * It also shrinks the state a partial aggregation builds and ships to its final. A group whose
 * predicate never holds is still fed a row when the condition sits inside the argument, so a sketch
 * is created and serialized for it; as a mask, those rows are filtered before the accumulator sees
 * them and such a group carries no state at all. Queries computing many differently-predicated
 * values over the same grouping keys leave most (predicate, group) pairs empty.
 * <p>
 * Both effects show up as CPU rather than as memory or network. Measured on a query with 465
 * conditional approx_distinct calls over 26 billion input rows, CPU fell by 14 percent, with
 * projection output down from 6.90 TB to 2.17 TB and aggregation output from 5.99 TB to 1.06 TB.
 * Those two are operator output volumes consumed within a node, so peak memory per node and shuffled
 * bytes were both unchanged; what the rewrite saves is the work of materializing and copying them.
 * On a second query whose aggregations shared only four distinct argument columns the projection was
 * unchanged and only the state effect appeared.
 * <p>
 * The rewrite is exact rather than approximate: {@code IF(p, e)} evaluates to NULL where p is false,
 * and approx_distinct does not count NULLs, so restricting the input to the rows where p holds feeds
 * the aggregation the same set of values. Results are unchanged.
 * <p>
 * Only a single IF is matched, which is sufficient because {@code SimplifyRowExpressions} has
 * already flattened nested conditionals bottom up: {@code IF(x, IF(y, v))} arrives here as
 * {@code IF(x AND y, v)}. Metric queries commonly write a window predicate around an inner
 * per metric predicate, and both end up in the mask.
 * <p>
 * The equivalent generic rewrite in {@link RewriteAggregationIfToFilter} does not apply here: it
 * refuses any function whose metadata reports {@code isCalledOnNullInput}, and approx_distinct
 * reports true because it returns 0 rather than NULL for an all null input. That flag describes the
 * function's result on null input, not whether nulls affect the result, and the two differ for this
 * function.
 */
public class RewriteApproxDistinctIfToMask
        implements Rule<AggregationNode>
{
    private static final String APPROX_DISTINCT = "approx_distinct";

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(RewriteApproxDistinctIfToMask::hasApproxDistinct);

    private final RowExpressionDeterminismEvaluator determinismEvaluator;

    public RewriteApproxDistinctIfToMask(FunctionAndTypeManager functionAndTypeManager)
    {
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(
                requireNonNull(functionAndTypeManager, "functionAndTypeManager is null"));
    }

    private static boolean hasApproxDistinct(AggregationNode aggregation)
    {
        return aggregation.getAggregations().values().stream().anyMatch(RewriteApproxDistinctIfToMask::isCandidate);
    }

    private static boolean isCandidate(Aggregation aggregation)
    {
        return aggregation.getCall().getDisplayName().equals(APPROX_DISTINCT)
                && !aggregation.getCall().getArguments().isEmpty()
                && !aggregation.isDistinct()
                && !aggregation.getOrderBy().isPresent();
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isRewriteApproxDistinctIfToMaskEnabled(session);
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        // Aggregation arguments are always variable references; the expression they stand for lives
        // in the projection below, which is where the IF has to be read from.
        PlanNode source = context.getLookup().resolve(aggregationNode.getSource());
        if (!(source instanceof ProjectNode)) {
            return Result.empty();
        }
        ProjectNode sourceProject = (ProjectNode) source;
        Map<VariableReferenceExpression, RowExpression> sourceExpressions = sourceProject.getAssignments().getMap();

        // A map rather than Assignments.Builder: several aggregations can reach the same variable,
        // and recording it more than once has to be harmless.
        Map<VariableReferenceExpression, RowExpression> newAssignments = new LinkedHashMap<>(sourceExpressions);
        Map<VariableReferenceExpression, Aggregation> newAggregations = new LinkedHashMap<>();
        boolean rewritten = false;

        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregationNode.getAggregations().entrySet()) {
            Optional<Aggregation> rewrite = rewriteAggregation(entry.getValue(), sourceExpressions, newAssignments, context);
            newAggregations.put(entry.getKey(), rewrite.orElse(entry.getValue()));
            rewritten |= rewrite.isPresent();
        }

        if (!rewritten) {
            return Result.empty();
        }

        // The projection is being rebuilt, so everything still read above it has to be carried over.
        aggregationNode.getGroupingKeys().forEach(variable -> newAssignments.putIfAbsent(variable, variable));
        for (Aggregation aggregation : newAggregations.values()) {
            aggregation.getArguments().stream()
                    .filter(VariableReferenceExpression.class::isInstance)
                    .map(VariableReferenceExpression.class::cast)
                    .forEach(variable -> newAssignments.putIfAbsent(variable, variable));
            aggregation.getMask().ifPresent(mask -> newAssignments.putIfAbsent(mask, mask));
            aggregation.getFilter().ifPresent(filter -> {
                if (filter instanceof VariableReferenceExpression) {
                    newAssignments.putIfAbsent((VariableReferenceExpression) filter, filter);
                }
            });
        }

        Assignments.Builder assignments = Assignments.builder();
        newAssignments.forEach(assignments::put);

        return Result.ofPlanNode(new AggregationNode(
                aggregationNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                new ProjectNode(context.getIdAllocator().getNextId(), sourceProject.getSource(), assignments.build()),
                ImmutableMap.copyOf(newAggregations),
                aggregationNode.getGroupingSets(),
                aggregationNode.getPreGroupedVariables(),
                aggregationNode.getStep(),
                aggregationNode.getHashVariable(),
                aggregationNode.getGroupIdVariable(),
                aggregationNode.getAggregationId()));
    }

    private Optional<Aggregation> rewriteAggregation(
            Aggregation aggregation,
            Map<VariableReferenceExpression, RowExpression> sourceExpressions,
            Map<VariableReferenceExpression, RowExpression> newAssignments,
            Context context)
    {
        if (!isCandidate(aggregation)) {
            return Optional.empty();
        }

        RowExpression argument = aggregation.getCall().getArguments().get(0);
        if (!(argument instanceof VariableReferenceExpression)) {
            return Optional.empty();
        }
        RowExpression value = sourceExpressions.get(argument);
        if (!(value instanceof SpecialFormExpression)) {
            return Optional.empty();
        }

        // Only IF without an else branch, so that a false condition really does yield NULL.
        SpecialFormExpression ifExpression = (SpecialFormExpression) value;
        if (ifExpression.getForm() != IF
                || !isNull(ifExpression.getArguments().get(2))
                || !determinismEvaluator.isDeterministic(ifExpression)) {
            return Optional.empty();
        }

        RowExpression condition = ifExpression.getArguments().get(0);
        VariableReferenceExpression valueVariable = asVariable(ifExpression.getArguments().get(1), newAssignments, context);

        RowExpression predicate = aggregation.getMask().isPresent()
                ? new SpecialFormExpression(AND, BOOLEAN, ImmutableList.of(aggregation.getMask().get(), condition))
                : condition;
        VariableReferenceExpression maskVariable = asVariable(predicate, newAssignments, context);

        CallExpression original = aggregation.getCall();
        ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
        arguments.add(valueVariable);
        // preserve an explicit standard error argument
        original.getArguments().stream().skip(1).forEach(arguments::add);

        CallExpression approxDistinct = new CallExpression(
                original.getSourceLocation(),
                original.getDisplayName(),
                original.getFunctionHandle(),
                original.getType(),
                arguments.build());

        return Optional.of(new Aggregation(
                approxDistinct,
                aggregation.getFilter(),
                aggregation.getOrderBy(),
                aggregation.isDistinct(),
                Optional.of(maskVariable)));
    }

    private static VariableReferenceExpression asVariable(
            RowExpression expression,
            Map<VariableReferenceExpression, RowExpression> newAssignments,
            Context context)
    {
        if (expression instanceof VariableReferenceExpression) {
            // Record it even so: the projection being rebuilt only carries what is put into it, and
            // the variable an IF unwraps to need not have been one of its outputs.
            VariableReferenceExpression variable = (VariableReferenceExpression) expression;
            newAssignments.putIfAbsent(variable, variable);
            return variable;
        }
        VariableReferenceExpression variable = context.getVariableAllocator().newVariable(expression);
        newAssignments.put(variable, expression);
        return variable;
    }
}
