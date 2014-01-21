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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class MarkDistinctNode
        extends PlanNode
{
    private final PlanNode source;
    private final Symbol markerSymbol;
    private final List<Symbol> distinctSymbols;
    private final Optional<Symbol> sampleWeightSymbol;

    @JsonCreator
    public MarkDistinctNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("markerSymbol") Symbol markerSymbol,
            @JsonProperty("distinctSymbols") List<Symbol> distinctSymbols,
            @JsonProperty("sampleWeightSymbol") Optional<Symbol> sampleWeightSymbol)
    {
        super(id);

        this.source = source;
        this.markerSymbol = markerSymbol;
        this.distinctSymbols = ImmutableList.copyOf(checkNotNull(distinctSymbols, "distinctSymbols is null"));
        this.sampleWeightSymbol = checkNotNull(sampleWeightSymbol, "sampleWeightSymbol is null");
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
    public Optional<Symbol> getSampleWeightSymbol()
    {
        return sampleWeightSymbol;
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

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitMarkDistinct(this, context);
    }
}
