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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;

/**
 * Converts Single Distinct Aggregation into GroupBy
 *
 * Rewrite if and only if
 *  1 all aggregation functions have a single common distinct mask symbol
 *  2 all aggregation functions have mask
 *
 * Rewrite MarkDistinctNode into AggregationNode(use DistinctSymbols as GroupBy)
 * Remove Distincts in the original AggregationNode
 */
public class SingleMarkDistinctToGroupBy
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!(node instanceof AggregationNode)) {
            return Optional.empty();
        }

        AggregationNode parent = (AggregationNode) node;

        PlanNode source = lookup.resolve(parent.getSource());
        if (!(source instanceof MarkDistinctNode)) {
            return Optional.empty();
        }

        MarkDistinctNode child = (MarkDistinctNode) source;

        boolean hasFilters = parent.getAggregations().values().stream()
                .map(FunctionCall::getFilter)
                .anyMatch(Optional::isPresent);

        if (hasFilters) {
            return Optional.empty();
        }

        // optimize if and only if
        // all aggregation functions have a single common distinct mask symbol
        // AND all aggregation functions have mask
        Set<Symbol> masks = ImmutableSet.copyOf(parent.getMasks().values());
        if (masks.size() != 1 || parent.getMasks().size() != parent.getAggregations().size()) {
            return Optional.empty();
        }

        Symbol mask = Iterables.getOnlyElement(masks);

        if (!child.getMarkerSymbol().equals(mask)) {
            return Optional.empty();
        }

        return Optional.of(
                new AggregationNode(
                        idAllocator.getNextId(),
                        new AggregationNode(
                                idAllocator.getNextId(),
                                child.getSource(),
                                Collections.emptyMap(),
                                ImmutableList.of(child.getDistinctSymbols()),
                                SINGLE,
                                child.getHashSymbol(),
                                Optional.empty()),
                        // remove DISTINCT flag from function calls
                        parent.getAssignments()
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
                Optional.empty());
    }
}
