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

import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

/**
 * Implements filtered aggregations by transforming plans of the following shape:
 * <pre>
 * - Aggregation
 *        F1(...) FILTER (WHERE C1(...)),
 *        F2(...) FILTER (WHERE C2(...))
 *     - X
 * </pre>
 * into
 * <pre>
 * - Aggregation
 *        F1(...) mask ($0)
 *        F2(...) mask ($1)
 *     - Filter(mask ($0) OR mask ($1))
 *     - Project
 *            &lt;identity projections for existing fields&gt;
 *            $0 = C1(...)
 *            $1 = C2(...)
 *         - X
 * </pre>
 */
public class ImplementFilteredAggregations
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(ImplementFilteredAggregations::hasFilters);

    private final LogicalRowExpressions logicalRowExpressions;

    public ImplementFilteredAggregations(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(functionAndTypeManager),
                new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver()),
                functionAndTypeManager);
    }

    private static boolean hasFilters(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .anyMatch(e -> e.getFilter().isPresent() &&
                        !e.getMask().isPresent()); // can't handle filtered aggregations with DISTINCT (conservatively, if they have a mask)
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        Assignments.Builder newAssignments = Assignments.builder();
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();
        ImmutableList.Builder<RowExpression> maskSymbols = ImmutableList.builder();
        boolean aggregateWithoutFilterPresent = false;

        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregation.getAggregations().entrySet()) {
            VariableReferenceExpression output = entry.getKey();

            // strip the filters
            Optional<VariableReferenceExpression> mask = entry.getValue().getMask();

            if (entry.getValue().getFilter().isPresent()) {
                RowExpression filter = entry.getValue().getFilter().get();
                VariableReferenceExpression variable = context.getVariableAllocator().newVariable(filter);
                verify(!mask.isPresent(), "Expected aggregation without mask symbols, see Rule pattern");
                newAssignments.put(variable, filter);
                mask = Optional.of(variable);

                maskSymbols.add(variable);
            }
            else {
                aggregateWithoutFilterPresent = true;
            }

            aggregations.put(output, new Aggregation(
                    entry.getValue().getCall(),
                    Optional.empty(),
                    entry.getValue().getOrderBy(),
                    entry.getValue().isDistinct(),
                    mask));
        }

        RowExpression predicate = TRUE_CONSTANT;
        if (!aggregation.hasNonEmptyGroupingSet() && !aggregateWithoutFilterPresent) {
            predicate = logicalRowExpressions.combineDisjunctsWithDefault(maskSymbols.build(), TRUE_CONSTANT);
        }

        // identity projection for all existing inputs
        newAssignments.putAll(identityAssignments(aggregation.getSource().getOutputVariables()));

        return Result.ofPlanNode(
                new AggregationNode(
                        aggregation.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        new FilterNode(
                                aggregation.getSourceLocation(),
                                context.getIdAllocator().getNextId(),
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        aggregation.getSource(),
                                        newAssignments.build()),
                                predicate),
                        aggregations.build(),
                        aggregation.getGroupingSets(),
                        ImmutableList.of(),
                        aggregation.getStep(),
                        aggregation.getHashVariable(),
                        aggregation.getGroupIdVariable(),
                        aggregation.getAggregationId()));
    }
}
