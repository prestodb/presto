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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;

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
            .matching(aggregation -> hasFilters(aggregation));

    private static boolean hasFilters(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .anyMatch(e -> e.getCall().getFilter().isPresent() &&
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
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();

        for (Map.Entry<Symbol, Aggregation> entry : aggregation.getAggregations().entrySet()) {
            Symbol output = entry.getKey();

            // strip the filters
            FunctionCall call = entry.getValue().getCall();
            Optional<Symbol> mask = entry.getValue().getMask();

            if (call.getFilter().isPresent()) {
                Expression filter = call.getFilter().get();
                Symbol symbol = context.getSymbolAllocator().newSymbol(filter, BOOLEAN);
                newAssignments.put(symbol, filter);
                mask = Optional.of(symbol);
            }
            aggregations.put(output, new Aggregation(
                    new FunctionCall(call.getName(), call.getWindow(), Optional.empty(), call.getOrderBy(), call.isDistinct(), call.getArguments()),
                    entry.getValue().getSignature(),
                    mask,
                    entry.getValue().getOrderBy(),
                    entry.getValue().getOrdering()));
        }

        // identity projection for all existing inputs
        newAssignments.putIdentities(aggregation.getSource().getOutputSymbols());

        return Result.ofPlanNode(
                new AggregationNode(
                        context.getIdAllocator().getNextId(),
                        new ProjectNode(
                                context.getIdAllocator().getNextId(),
                                aggregation.getSource(),
                                newAssignments.build()),
                        aggregations.build(),
                        aggregation.getGroupingSets(),
                        aggregation.getStep(),
                        aggregation.getHashSymbol(),
                        aggregation.getGroupIdSymbol()));
    }
}
