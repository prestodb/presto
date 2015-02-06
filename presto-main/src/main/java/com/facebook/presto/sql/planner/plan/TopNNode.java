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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

@Immutable
public class TopNNode
        extends PlanNode
{
    private final PlanNode source;
    private final long count;
    private final List<Symbol> orderBy;
    private final Map<Symbol, SortOrder> orderings;
    private final boolean partial;

    @JsonCreator
    public TopNNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("count") long count,
            @JsonProperty("orderBy") List<Symbol> orderBy,
            @JsonProperty("orderings") Map<Symbol, SortOrder> orderings,
            @JsonProperty("partial") boolean partial)
    {
        super(id);

        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkArgument(count >= 0, "count must be positive");
        Preconditions.checkNotNull(orderBy, "orderBy is null");
        Preconditions.checkArgument(!orderBy.isEmpty(), "orderBy is empty");
        Preconditions.checkArgument(orderings.size() == orderBy.size(), "orderBy and orderings sizes don't match");

        this.source = source;
        this.count = count;
        this.orderBy = ImmutableList.copyOf(orderBy);
        this.orderings = ImmutableMap.copyOf(orderings);
        this.partial = partial;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    @JsonProperty("count")
    public long getCount()
    {
        return count;
    }

    @JsonProperty("orderBy")
    public List<Symbol> getOrderBy()
    {
        return orderBy;
    }

    @JsonProperty("orderings")
    public Map<Symbol, SortOrder> getOrderings()
    {
        return orderings;
    }

    @JsonProperty("partial")
    public boolean isPartial()
    {
        return partial;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitTopN(this, context);
    }
}
