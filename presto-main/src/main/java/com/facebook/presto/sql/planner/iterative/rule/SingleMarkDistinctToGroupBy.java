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

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.markDistinct;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Converts Single Distinct Aggregation into GroupBy
 * <p>
 * Rewrite if and only if
 * <ol>
 * <li>all aggregation functions have a single common distinct mask symbol
 * <li>all aggregation functions have mask
 * </ol>
 * Rewrite MarkDistinctNode into AggregationNode(use DistinctSymbols as GroupBy)
 * <p>
 * Remove Distincts in the original AggregationNode
 */
public class SingleMarkDistinctToGroupBy
        implements Rule<AggregationNode>
{
    private static final Capture<MarkDistinctNode> CHILD = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(aggregation -> hasFilters(aggregation))
            .with(source().matching(markDistinct().capturedAs(CHILD)));

    private static boolean hasFilters(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations().values().stream()
                .map(Aggregation::getCall)
                .map(FunctionCall::getFilter)
                .anyMatch(Optional::isPresent);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode parent, Captures captures, Context context)
    {
        MarkDistinctNode child = captures.get(CHILD);

        // optimize if and only if
        // all aggregation functions have a single common distinct mask symbol
        // AND all aggregation functions have mask
        Collection<Aggregation> aggregations = parent.getAggregations().values();

        List<Symbol> masks = aggregations.stream()
                .map(Aggregation::getMask)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());

        Set<Symbol> uniqueMasks = ImmutableSet.copyOf(masks);

        if (uniqueMasks.size() != 1 || masks.size() != aggregations.size()) {
            return Result.empty();
        }

        Symbol mask = Iterables.getOnlyElement(uniqueMasks);

        if (!child.getMarkerSymbol().equals(mask)) {
            return Result.empty();
        }

        return Result.ofPlanNode(
                new AggregationNode(
                        context.getIdAllocator().getNextId(),
                        new AggregationNode(
                                context.getIdAllocator().getNextId(),
                                child.getSource(),
                                Collections.emptyMap(),
                                ImmutableList.of(child.getDistinctSymbols()),
                                SINGLE,
                                child.getHashSymbol(),
                                Optional.empty()),
                        // remove DISTINCT flag from function calls
                        parent.getAggregations()
                                .entrySet().stream()
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> removeDistinct(e.getValue()))),
                        parent.getGroupingSets(),
                        parent.getStep(),
                        parent.getHashSymbol(),
                        parent.getGroupIdSymbol()));
    }

    private static AggregationNode.Aggregation removeDistinct(AggregationNode.Aggregation aggregation)
    {
        FunctionCall call = aggregation.getCall();
        return new AggregationNode.Aggregation(
                new FunctionCall(call.getName(), call.getWindow(), false, call.getArguments()),
                aggregation.getSignature(),
                Optional.empty(),
                aggregation.getOrderBy(),
                aggregation.getOrdering());
    }
}
