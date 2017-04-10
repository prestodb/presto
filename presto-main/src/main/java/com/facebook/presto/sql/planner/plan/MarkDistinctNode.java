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
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class MarkDistinctNode
        extends PlanNode
{
    private final PlanNode source;
    private final Symbol markerSymbol;

    private final Optional<Symbol> hashSymbol;
    private final List<Symbol> distinctSymbols;

    @JsonCreator
    public MarkDistinctNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("markerSymbol") Symbol markerSymbol,
            @JsonProperty("distinctSymbols") List<Symbol> distinctSymbols,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol)
    {
        super(id);
        this.source = source;
        this.markerSymbol = markerSymbol;
        this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
        this.distinctSymbols = ImmutableList.copyOf(requireNonNull(distinctSymbols, "distinctSymbols is null"));
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(source.getOutputSymbols())
                .add(markerSymbol)
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
    public Symbol getMarkerSymbol()
    {
        return markerSymbol;
    }

    @JsonProperty
    public List<Symbol> getDistinctSymbols()
    {
        return distinctSymbols;
    }

    @JsonProperty
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitMarkDistinct(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new MarkDistinctNode(getId(), Iterables.getOnlyElement(newChildren), markerSymbol, distinctSymbols, hashSymbol);
    }
}
