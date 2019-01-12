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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.AggregationNode.Aggregation;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.prestosql.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static java.util.Collections.emptyList;

/**
 * Implements distinct aggregations with similar inputs by transforming plans of the following shape:
 * <pre>
 * - Aggregation
 *        GROUP BY (k)
 *        F1(DISTINCT s0, s1, ...),
 *        F2(DISTINCT s0, s1, ...),
 *     - X
 * </pre>
 * into
 * <pre>
 * - Aggregation
 *          GROUP BY (k)
 *          F1(x)
 *          F2(x)
 *      - Aggregation
 *             GROUP BY (k, s0, s1, ...)
 *          - X
 * </pre>
 * <p>
 * Assumes s0, s1, ... are symbol references (i.e., complex expressions have been pre-projected)
 */
public class SingleDistinctAggregationToGroupBy
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(SingleDistinctAggregationToGroupBy::hasSingleDistinctInput)
            .matching(SingleDistinctAggregationToGroupBy::allDistinctAggregates)
            .matching(SingleDistinctAggregationToGroupBy::noFilters)
            .matching(SingleDistinctAggregationToGroupBy::noMasks);

    private static boolean hasSingleDistinctInput(AggregationNode aggregation)
    {
        return extractArgumentSets(aggregation)
                .count() == 1;
    }

    private static boolean allDistinctAggregates(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .map(Aggregation::getCall)
                .allMatch(FunctionCall::isDistinct);
    }

    private static boolean noFilters(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .map(Aggregation::getCall)
                .noneMatch(call -> call.getFilter().isPresent());
    }

    private static boolean noMasks(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .noneMatch(e -> e.getMask().isPresent());
    }

    private static Stream<Set<Expression>> extractArgumentSets(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .map(Aggregation::getCall)
                .filter(FunctionCall::isDistinct)
                .map(FunctionCall::getArguments)
                .<Set<Expression>>map(HashSet::new)
                .distinct();
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        List<Set<Expression>> argumentSets = extractArgumentSets(aggregation)
                .collect(Collectors.toList());

        Set<Symbol> symbols = Iterables.getOnlyElement(argumentSets).stream()
                .map(Symbol::from)
                .collect(Collectors.toSet());

        return Result.ofPlanNode(
                new AggregationNode(
                        aggregation.getId(),
                        new AggregationNode(
                                context.getIdAllocator().getNextId(),
                                aggregation.getSource(),
                                ImmutableMap.of(),
                                singleGroupingSet(ImmutableList.<Symbol>builder()
                                        .addAll(aggregation.getGroupingKeys())
                                        .addAll(symbols)
                                        .build()),
                                ImmutableList.of(),
                                SINGLE,
                                Optional.empty(),
                                Optional.empty()),
                        // remove DISTINCT flag from function calls
                        aggregation.getAggregations()
                                .entrySet().stream()
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> removeDistinct(e.getValue()))),
                        aggregation.getGroupingSets(),
                        emptyList(),
                        aggregation.getStep(),
                        aggregation.getHashSymbol(),
                        aggregation.getGroupIdSymbol()));
    }

    private static AggregationNode.Aggregation removeDistinct(AggregationNode.Aggregation aggregation)
    {
        checkArgument(aggregation.getCall().isDistinct(), "Expected aggregation to have DISTINCT input");

        FunctionCall call = aggregation.getCall();
        return new AggregationNode.Aggregation(
                new FunctionCall(
                        call.getName(),
                        call.getWindow(),
                        call.getFilter(),
                        call.getOrderBy(),
                        false,
                        call.getArguments()),
                aggregation.getSignature(),
                aggregation.getMask());
    }
}
