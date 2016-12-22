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

    // from output to input symbols
    private final Map<Symbol, Symbol> groupingSetMappings;
    private final Map<Symbol, Symbol> argumentMappings;

    private final Symbol groupIdSymbol;

    @JsonCreator
    public GroupIdNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("groupingSets") List<List<Symbol>> groupingSets,
            @JsonProperty("groupingSetMappings") Map<Symbol, Symbol> groupingSetMappings,
            @JsonProperty("argumentMappings") Map<Symbol, Symbol> argumentMappings,
            @JsonProperty("groupIdSymbol") Symbol groupIdSymbol)
    {
        super(id);
        this.source = requireNonNull(source);
        this.groupingSets = ImmutableList.copyOf(requireNonNull(groupingSets));
        this.groupingSetMappings = ImmutableMap.copyOf(requireNonNull(groupingSetMappings));
        this.argumentMappings = ImmutableMap.copyOf(requireNonNull(argumentMappings));
        this.groupIdSymbol = requireNonNull(groupIdSymbol);

        checkArgument(Sets.intersection(groupingSetMappings.keySet(), argumentMappings.keySet()).isEmpty(), "argument outputs and grouping outputs must be a disjoint set");
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(groupingSets.stream()
                        .flatMap(Collection::stream)
                        .collect(toSet()))
                .addAll(argumentMappings.keySet())
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
    public Map<Symbol, Symbol> getGroupingSetMappings()
    {
        return groupingSetMappings;
    }

    @JsonProperty
    public Map<Symbol, Symbol> getArgumentMappings()
    {
        return argumentMappings;
    }

    @JsonProperty
    public Symbol getGroupIdSymbol()
    {
        return groupIdSymbol;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitGroupId(this, context);
    }

    public Set<Symbol> getInputSymbols()
    {
        return ImmutableSet.<Symbol>builder()
                .addAll(argumentMappings.values())
                .addAll(groupingSets.stream()
                        .map(set -> set.stream()
                                .map(groupingSetMappings::get).collect(Collectors.toList()))
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
        return new GroupIdNode(getId(), Iterables.getOnlyElement(newChildren), groupingSets, groupingSetMappings, argumentMappings, groupIdSymbol);
    }
}
