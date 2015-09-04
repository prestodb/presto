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

import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.concat;
import static java.util.Objects.requireNonNull;

@Immutable
public final class TopNRowNumberNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<Symbol> partitionBy;
    private final List<Symbol> orderBy;
    private final Map<Symbol, SortOrder> orderings;
    private final Symbol rowNumberSymbol;
    private final int maxRowCountPerPartition;
    private final boolean partial;
    private final Optional<Symbol> hashSymbol;

    @JsonCreator
    public TopNRowNumberNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("partitionBy") List<Symbol> partitionBy,
            @JsonProperty("orderBy") List<Symbol> orderBy,
            @JsonProperty("orderings") Map<Symbol, SortOrder> orderings,
            @JsonProperty("rowNumberSymbol") Symbol rowNumberSymbol,
            @JsonProperty("maxRowCountPerPartition") int maxRowCountPerPartition,
            @JsonProperty("partial") boolean partial,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(partitionBy, "partitionBy is null");
        requireNonNull(orderBy, "orderBy is null");
        requireNonNull(orderings, "orderings is null");
        checkArgument(orderings.size() == orderBy.size(), "orderBy and orderings sizes don't match");
        requireNonNull(rowNumberSymbol, "rowNumberSymbol is null");
        checkArgument(maxRowCountPerPartition > 0, "maxRowCountPerPartition must be > 0");
        requireNonNull(hashSymbol, "hashSymbol is null");

        this.source = source;
        this.partitionBy = ImmutableList.copyOf(partitionBy);
        this.orderBy = ImmutableList.copyOf(orderBy);
        this.orderings = ImmutableMap.copyOf(orderings);
        this.rowNumberSymbol = rowNumberSymbol;
        this.maxRowCountPerPartition = maxRowCountPerPartition;
        this.partial = partial;
        this.hashSymbol = hashSymbol;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        if (!partial) {
            return ImmutableList.copyOf(concat(source.getOutputSymbols(), ImmutableList.of(rowNumberSymbol)));
        }
        return ImmutableList.copyOf(source.getOutputSymbols());
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public List<Symbol> getPartitionBy()
    {
        return partitionBy;
    }

    @JsonProperty
    public List<Symbol> getOrderBy()
    {
        return orderBy;
    }

    @JsonProperty
    public Map<Symbol, SortOrder> getOrderings()
    {
        return orderings;
    }

    @JsonProperty
    public Symbol getRowNumberSymbol()
    {
        return rowNumberSymbol;
    }

    @JsonProperty
    public int getMaxRowCountPerPartition()
    {
        return maxRowCountPerPartition;
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

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitTopNRowNumber(this, context);
    }
}
