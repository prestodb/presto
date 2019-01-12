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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.prestosql.SystemSessionProperties;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.AggregationNode.Aggregation;
import io.prestosql.sql.planner.plan.MarkDistinctNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.tree.FunctionCall;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static java.util.stream.Collectors.toSet;

/**
 * Implements distinct aggregations with different inputs by transforming plans of the following shape:
 * <pre>
 * - Aggregation
 *        GROUP BY (k)
 *        F1(DISTINCT a0, a1, ...)
 *        F2(DISTINCT b0, b1, ...)
 *        F3(c0, c1, ...)
 *     - X
 * </pre>
 * into
 * <pre>
 * - Aggregation
 *        GROUP BY (k)
 *        F1(a0, a1, ...) mask ($0)
 *        F2(b0, b1, ...) mask ($1)
 *        F3(c0, c1, ...)
 *     - MarkDistinct (k, a0, a1, ...) -> $0
 *          - MarkDistinct (k, b0, b1, ...) -> $1
 *              - X
 * </pre>
 */
public class MultipleDistinctAggregationToMarkDistinct
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(
                    Predicates.and(
                            MultipleDistinctAggregationToMarkDistinct::hasNoDistinctWithFilterOrMask,
                            Predicates.or(
                                    MultipleDistinctAggregationToMarkDistinct::hasMultipleDistincts,
                                    MultipleDistinctAggregationToMarkDistinct::hasMixedDistinctAndNonDistincts)));

    private static boolean hasNoDistinctWithFilterOrMask(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .noneMatch(e -> e.getCall().isDistinct() && (e.getCall().getFilter().isPresent() || e.getMask().isPresent()));
    }

    private static boolean hasMultipleDistincts(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .filter(e -> e.getCall().isDistinct())
                .map(Aggregation::getCall)
                .map(FunctionCall::getArguments)
                .map(HashSet::new)
                .distinct()
                .count() > 1;
    }

    private static boolean hasMixedDistinctAndNonDistincts(AggregationNode aggregation)
    {
        long distincts = aggregation.getAggregations()
                .values().stream()
                .map(Aggregation::getCall)
                .filter(FunctionCall::isDistinct)
                .count();

        return distincts > 0 && distincts < aggregation.getAggregations().size();
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode parent, Captures captures, Context context)
    {
        if (!SystemSessionProperties.useMarkDistinct(context.getSession())) {
            return Result.empty();
        }

        // the distinct marker for the given set of input columns
        Map<Set<Symbol>, Symbol> markers = new HashMap<>();

        Map<Symbol, Aggregation> newAggregations = new HashMap<>();
        PlanNode subPlan = parent.getSource();

        for (Map.Entry<Symbol, Aggregation> entry : parent.getAggregations().entrySet()) {
            Aggregation aggregation = entry.getValue();
            FunctionCall call = aggregation.getCall();

            if (call.isDistinct() && !call.getFilter().isPresent() && !aggregation.getMask().isPresent()) {
                Set<Symbol> inputs = call.getArguments().stream()
                        .map(Symbol::from)
                        .collect(toSet());

                Symbol marker = markers.get(inputs);
                if (marker == null) {
                    marker = context.getSymbolAllocator().newSymbol(Iterables.getLast(inputs).getName(), BOOLEAN, "distinct");
                    markers.put(inputs, marker);

                    ImmutableSet.Builder<Symbol> distinctSymbols = ImmutableSet.<Symbol>builder()
                            .addAll(parent.getGroupingKeys())
                            .addAll(inputs);
                    parent.getGroupIdSymbol().ifPresent(distinctSymbols::add);

                    subPlan = new MarkDistinctNode(
                            context.getIdAllocator().getNextId(),
                            subPlan,
                            marker,
                            ImmutableList.copyOf(distinctSymbols.build()),
                            Optional.empty());
                }

                // remove the distinct flag and set the distinct marker
                newAggregations.put(entry.getKey(),
                        new Aggregation(
                                new FunctionCall(
                                        call.getName(),
                                        call.getWindow(),
                                        call.getFilter(),
                                        call.getOrderBy(),
                                        false,
                                        call.getArguments()),
                                aggregation.getSignature(),
                                Optional.of(marker)));
            }
            else {
                newAggregations.put(entry.getKey(), aggregation);
            }
        }

        return Result.ofPlanNode(
                new AggregationNode(
                        parent.getId(),
                        subPlan,
                        newAggregations,
                        parent.getGroupingSets(),
                        ImmutableList.of(),
                        parent.getStep(),
                        parent.getHashSymbol(),
                        parent.getGroupIdSymbol()));
    }
}
