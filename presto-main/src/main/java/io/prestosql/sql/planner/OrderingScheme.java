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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.block.SortOrder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrderingScheme
{
    private final List<Symbol> orderBy;
    private final Map<Symbol, SortOrder> orderings;

    @JsonCreator
    public OrderingScheme(@JsonProperty("orderBy") List<Symbol> orderBy, @JsonProperty("orderings") Map<Symbol, SortOrder> orderings)
    {
        requireNonNull(orderBy, "orderBy is null");
        requireNonNull(orderings, "orderings is null");
        checkArgument(!orderBy.isEmpty(), "orderBy is empty");
        checkArgument(orderings.keySet().equals(ImmutableSet.copyOf(orderBy)), "orderBy keys and orderings don't match");
        this.orderBy = ImmutableList.copyOf(orderBy);
        this.orderings = ImmutableMap.copyOf(orderings);
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

    public List<SortOrder> getOrderingList()
    {
        return orderBy.stream()
                .map(orderings::get)
                .collect(toImmutableList());
    }

    public SortOrder getOrdering(Symbol symbol)
    {
        checkArgument(orderings.containsKey(symbol), format("No ordering for symbol: %s", symbol));
        return orderings.get(symbol);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OrderingScheme that = (OrderingScheme) o;
        return Objects.equals(orderBy, that.orderBy) &&
                Objects.equals(orderings, that.orderings);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(orderBy, orderings);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("orderBy", orderBy)
                .add("orderings", orderings)
                .toString();
    }
}
