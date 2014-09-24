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
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class UnnestNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<Symbol> replicateSymbols;
    private final Map<Symbol, List<Symbol>> unnestSymbols;

    @JsonCreator
    public UnnestNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("replicateSymbols") List<Symbol> replicateSymbols,
            @JsonProperty("unnestSymbols") Map<Symbol, List<Symbol>> unnestSymbols)
    {
        super(id);
        this.source = checkNotNull(source, "source is null");
        this.replicateSymbols = ImmutableList.copyOf(checkNotNull(replicateSymbols, "replicateSymbols is null"));
        checkNotNull(unnestSymbols, "unnestSymbols is null");
        checkArgument(!unnestSymbols.isEmpty(), "unnestSymbols is empty");
        ImmutableMap.Builder<Symbol, List<Symbol>> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, List<Symbol>> entry : unnestSymbols.entrySet()) {
            builder.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        this.unnestSymbols = builder.build();
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(replicateSymbols)
                .addAll(Iterables.concat(unnestSymbols.values()))
                .build();
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public List<Symbol> getReplicateSymbols()
    {
        return replicateSymbols;
    }

    @JsonProperty
    public Map<Symbol, List<Symbol>> getUnnestSymbols()
    {
        return unnestSymbols;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitUnnest(this, context);
    }
}
