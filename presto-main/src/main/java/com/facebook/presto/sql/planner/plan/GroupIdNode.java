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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.concurrent.Immutable;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.util.MoreLists.listOfListsCopy;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

@Immutable
public class GroupIdNode
        extends PlanNode
{
    private final PlanNode source;

    // in terms of output symbols
    private final List<List<Symbol>> groupingSets;

    // tracks how each grouping set column is derived from an input column
    private final Map<Symbol, Symbol> groupingColumns;
    private final List<Symbol> aggregationArguments;

    private final Symbol groupIdSymbol;

    @JsonCreator
    public GroupIdNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("groupingSets") List<List<Symbol>> groupingSets,
            @JsonProperty("groupingColumns") Map<Symbol, Symbol> groupingColumns,
            @JsonProperty("aggregationArguments") List<Symbol> aggregationArguments,
            @JsonProperty("groupIdSymbol") Symbol groupIdSymbol)
    {
        super(id);
        this.source = requireNonNull(source);
        this.groupingSets = listOfListsCopy(requireNonNull(groupingSets, "groupingSets is null"));
        this.groupingColumns = ImmutableMap.copyOf(requireNonNull(groupingColumns));
        this.aggregationArguments = ImmutableList.copyOf(aggregationArguments);
        this.groupIdSymbol = requireNonNull(groupIdSymbol);

        checkArgument(Sets.intersection(groupingColumns.keySet(), ImmutableSet.copyOf(aggregationArguments)).isEmpty(), "aggregation columns and grouping set columns must be a disjoint set");
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(groupingSets.stream()
                        .flatMap(Collection::stream)
                        .collect(toSet()))
                .addAll(aggregationArguments)
                .add(groupIdSymbol)
                .build();
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public List<List<Symbol>> getGroupingSets()
    {
        return groupingSets;
    }

    @JsonProperty
    public Map<Symbol, Symbol> getGroupingColumns()
    {
        return groupingColumns;
    }

    @JsonProperty
    public List<Symbol> getAggregationArguments()
    {
        return aggregationArguments;
    }

    @JsonProperty
    public Symbol getGroupIdSymbol()
    {
        return groupIdSymbol;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitGroupId(this, context);
    }

    public Set<Symbol> getInputSymbols()
    {
        return ImmutableSet.<Symbol>builder()
                .addAll(aggregationArguments)
                .addAll(groupingSets.stream()
                        .map(set -> set.stream()
                                .map(groupingColumns::get).collect(Collectors.toList()))
                        .flatMap(Collection::stream)
                        .collect(toSet()))
                .build();
    }

    // returns the common grouping columns in terms of output symbols
    public Set<Symbol> getCommonGroupingColumns()
    {
        Set<Symbol> intersection = new HashSet<>(groupingSets.get(0));
        for (int i = 1; i < groupingSets.size(); i++) {
            intersection.retainAll(groupingSets.get(i));
        }
        return ImmutableSet.copyOf(intersection);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new GroupIdNode(getId(), Iterables.getOnlyElement(newChildren), groupingSets, groupingColumns, aggregationArguments, groupIdSymbol);
    }
}
