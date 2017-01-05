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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;

public class AggregationMatcher
        implements Matcher
{
    private final Map<Symbol, Symbol> masks;
    private final List<List<Symbol>> groupingSets;
    private final Optional<Symbol> groupId;

    public AggregationMatcher(List<List<Symbol>> groupingSets, Map<Symbol, Symbol> masks, Optional<Symbol> groupId)
    {
        this.masks = masks;
        this.groupingSets = groupingSets;
        this.groupId = groupId;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof AggregationNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        AggregationNode aggregationNode = (AggregationNode) node;

        if (groupId.isPresent() != aggregationNode.getGroupIdSymbol().isPresent()) {
            return NO_MATCH;
        }

        if (groupingSets.size() != aggregationNode.getGroupingSets().size()) {
            return NO_MATCH;
        }

        List<Symbol> aggregationsWithMask = aggregationNode.getAggregations()
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().isDistinct())
                .map(entry -> entry.getKey())
                .collect(Collectors.toList());

        if (aggregationsWithMask.size() != masks.keySet().size()) {
            return NO_MATCH;
        }

        for (Symbol symbol : aggregationsWithMask) {
            if (!masks.keySet().contains(symbol)) {
                return NO_MATCH;
            }
        }

        for (int i = 0; i < groupingSets.size(); i++) {
            if (!matches(groupingSets.get(i), aggregationNode.getGroupingSets().get(i))) {
                return NO_MATCH;
            }
        }

        return match();
    }

    static <T> boolean matches(Collection<T> expected, Collection<T> actual)
    {
        if (expected.size() != actual.size()) {
            return false;
        }

        for (T symbol : expected) {
            if (!actual.contains(symbol)) {
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
                .add("masks", masks)
                .add("groudId", groupId)
                .toString();
    }
}
