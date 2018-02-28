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
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class UnnestMatcher
        implements Matcher
{
    private final List<String> replicateSymbols;
    private final Map<String, List<String>> unnestSymbols;
    private final Optional<String> ordinalitySymbol;

    public UnnestMatcher(List<String> replicateSymbols, Map<String, List<String>> unnestSymbols, Optional<String> ordinalitySymbol)
    {
        this.replicateSymbols = requireNonNull(replicateSymbols, "replicateSymbols is null");
        this.unnestSymbols = requireNonNull(unnestSymbols);
        this.ordinalitySymbol = requireNonNull(ordinalitySymbol);
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof UnnestNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        UnnestNode unnestNode = (UnnestNode) node;

        ImmutableList.Builder<String> aliasBuilder = ImmutableList.<String>builder().addAll(replicateSymbols).addAll(Iterables.concat(unnestSymbols.values()));
        ordinalitySymbol.ifPresent(aliasBuilder::add);
        List<String> alias = aliasBuilder.build();

        if (alias.size() != unnestNode.getOutputSymbols().size()) {
            return MatchResult.NO_MATCH;
        }

        SymbolAliases.Builder builder = SymbolAliases.builder().putAll(symbolAliases);
        IntStream.range(0, alias.size()).forEach(i -> builder.put(alias.get(i), unnestNode.getOutputSymbols().get(i).toSymbolReference()));
        return match(builder.build());
    }
}
