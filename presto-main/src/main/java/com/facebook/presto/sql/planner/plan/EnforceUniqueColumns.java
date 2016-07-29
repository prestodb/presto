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

import java.util.List;

import static java.util.Objects.requireNonNull;

public class EnforceUniqueColumns
        extends PlanNode
{
    private final PlanNode source;
    private final Symbol mostSigBitsSymbol;
    private final Symbol leastSigBitsSymbol;

    @JsonCreator
    public EnforceUniqueColumns(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("mostSigBitsSymbol") Symbol mostSigBitsSymbol,
            @JsonProperty("leastSigBitsSymbol") Symbol leastSigBitsSymbol)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.mostSigBitsSymbol = requireNonNull(mostSigBitsSymbol, "mostSigBitsSymbol is null");
        this.leastSigBitsSymbol = requireNonNull(leastSigBitsSymbol, "leastSigBitsSymbol is null");
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(source.getOutputSymbols())
                .add(mostSigBitsSymbol)
                .add(leastSigBitsSymbol)
                .build();
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty
    public Symbol getLeastSigBitsSymbol()
    {
        return leastSigBitsSymbol;
    }

    @JsonProperty
    public Symbol getMostSigBitsSymbol()
    {
        return mostSigBitsSymbol;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitEnforceUniqueColumns(this, context);
    }
}
