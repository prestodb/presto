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

import javax.annotation.concurrent.Immutable;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Immutable
public class GroupIdNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<Symbol> inputSymbols;
    private final List<List<Symbol>> groupingSets;
    private final Symbol groupIdSymbol;

    @JsonCreator
    public GroupIdNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("inputSymbols") List<Symbol> inputSymbols,
            @JsonProperty("groupingSets") List<List<Symbol>> groupingSets,
            @JsonProperty("groupIdSymbol") Symbol groupIdSymbol)
    {
        super(id);
        this.source = requireNonNull(source);
        this.inputSymbols = ImmutableList.copyOf(requireNonNull(inputSymbols));
        this.groupingSets = ImmutableList.copyOf(requireNonNull(groupingSets));
        this.groupIdSymbol = requireNonNull(groupIdSymbol);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(source.getOutputSymbols())
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
    public List<Symbol> getInputSymbols()
    {
        return inputSymbols;
    }

    @JsonProperty
    public List<List<Symbol>> getGroupingSets()
    {
        return groupingSets;
    }

    public List<Symbol> getDistinctGroupingColumns()
    {
        return groupingSets.stream()
                .flatMap(Collection::stream)
                .distinct()
                .collect(toList());
    }

    public List<Symbol> getCommonGroupingColumns()
    {
        Set<Symbol> intersection = new HashSet<>(groupingSets.get(0));
        for (int i = 1; i < getGroupingSets().size(); i++) {
            intersection.retainAll(groupingSets.get(i));
        }
        return ImmutableList.copyOf(intersection);
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
}
