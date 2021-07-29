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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class AggregationMatcher
        implements Matcher
{
    private final PlanMatchPattern.GroupingSetDescriptor groupingSets;
    private final Map<Symbol, Symbol> masks;
    private final List<String> preGroupedSymbols;
    private final Optional<Symbol> groupId;
    private final Step step;

    public AggregationMatcher(PlanMatchPattern.GroupingSetDescriptor groupingSets, List<String> preGroupedSymbols, Map<Symbol, Symbol> masks, Optional<Symbol> groupId, Step step)
    {
        this.groupingSets = groupingSets;
        this.masks = masks;
        this.preGroupedSymbols = preGroupedSymbols;
        this.groupId = groupId;
        this.step = step;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof AggregationNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        AggregationNode aggregationNode = (AggregationNode) node;

        if (groupId.isPresent() != aggregationNode.getGroupIdVariable().isPresent()) {
            return NO_MATCH;
        }

        if (!matches(groupingSets.getGroupingKeys(), aggregationNode.getGroupingKeys(), symbolAliases)) {
            return NO_MATCH;
        }

        if (groupingSets.getGroupingSetCount() != aggregationNode.getGroupingSetCount()) {
            return NO_MATCH;
        }

        if (!groupingSets.getGlobalGroupingSets().equals(aggregationNode.getGlobalGroupingSets())) {
            return NO_MATCH;
        }

        List<VariableReferenceExpression> aggregationsWithMask = aggregationNode.getAggregations()
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().getMask().isPresent())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        if (aggregationsWithMask.size() != masks.keySet().size()) {
            return NO_MATCH;
        }

        for (VariableReferenceExpression variable : aggregationsWithMask) {
            if (!masks.keySet().contains(new Symbol(variable.getName()))) {
                return NO_MATCH;
            }
        }

        if (step != aggregationNode.getStep()) {
            return NO_MATCH;
        }

        if (!matches(preGroupedSymbols, aggregationNode.getPreGroupedVariables(), symbolAliases)) {
            return NO_MATCH;
        }

        return match();
    }

    static boolean matches(Collection<String> expectedAliases, Collection<VariableReferenceExpression> actualVariables, SymbolAliases symbolAliases)
    {
        if (expectedAliases.size() != actualVariables.size()) {
            return false;
        }

        List<String> expectedSymbolNames = expectedAliases
                .stream()
                .map(alias -> symbolAliases.get(alias).getName())
                .collect(toImmutableList());
        Set<String> actualVariableNames = actualVariables.stream().map(VariableReferenceExpression::getName).collect(toImmutableSet());
        for (String symbolName : expectedSymbolNames) {
            if (!actualVariableNames.contains(symbolName)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("groupingSets", groupingSets)
                .add("preGroupedSymbols", preGroupedSymbols)
                .add("masks", masks)
                .add("groudId", groupId)
                .add("step", step)
                .toString();
    }
}
