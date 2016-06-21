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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.not;
import static java.util.Objects.requireNonNull;

@Immutable
public class DistinctLimitNode
        extends PlanNode
{
    private final PlanNode source;
    private final long limit;
    private final boolean partial;
    private final Optional<Symbol> hashSymbol;
    private final List<Symbol> distinctSymbols;

    @JsonCreator
    public DistinctLimitNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("limit") long limit,
            @JsonProperty("partial") boolean partial,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol,
            @JsonProperty("distinctSymbols") List<Symbol> distinctSymbols)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        checkArgument(limit >= 0, "limit must be greater than or equal to zero");
        this.limit = limit;
        this.partial = partial;
        this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
        this.distinctSymbols = ImmutableList.copyOf(requireNonNull(distinctSymbols, "distinctSymbols is null"));
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
    public long getLimit()
    {
        return limit;
    }

    @JsonProperty
    public boolean isPartial()
    {
        return partial;
    }

    @JsonProperty
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @JsonProperty("distinctSymbols")
    public List<Symbol> getDistinctSymbols()
    {
        if (hashSymbol.isPresent()) {
            return ImmutableList.copyOf(Iterables.filter(distinctSymbols, not(hashSymbol.get()::equals)));
        }
        return distinctSymbols;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitDistinctLimit(this, context);
    }
}
