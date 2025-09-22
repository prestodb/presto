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
package com.facebook.presto.plugin.clp.optimization;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents a Top-N specification for a query, including the limit of rows to return
 * and the ordering of columns.
 */
public class ClpTopNSpec
{
    /**
     * Enum representing the order direction: ascending or descending.
     */
    public enum Order
    {
        ASC,
        DESC
    }

    /**
     * Represents the ordering of one or more columns with a specified order (ASC or DESC).
     */
    public static final class Ordering
    {
        private final List<String> columns;
        private final Order order;

        @JsonCreator
        public Ordering(@JsonProperty("columns") List<String> columns, @JsonProperty("order") Order order)
        {
            this.columns = requireNonNull(columns, "column is null");
            this.order = requireNonNull(order, "order is null");
        }

        @JsonProperty
        public List<String> getColumns()
        {
            return columns;
        }

        @JsonProperty
        public Order getOrder()
        {
            return order;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(columns, order);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Ordering other = (Ordering) obj;
            return this.order == other.order && this.columns.equals(other.columns);
        }

        @Override
        public String toString()
        {
            return columns + ":" + order;
        }
    }

    private final long limit;
    private final List<Ordering> orderings;

    @JsonCreator
    public ClpTopNSpec(@JsonProperty("limit") long limit, @JsonProperty("orderings") List<Ordering> orderings)
    {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be > 0");
        }
        if (orderings == null || orderings.isEmpty()) {
            throw new IllegalArgumentException("orderings must be non-empty");
        }
        this.limit = limit;
        this.orderings = orderings;
    }

    @JsonProperty
    public long getLimit()
    {
        return limit;
    }

    @JsonProperty
    public List<Ordering> getOrderings()
    {
        return orderings;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(limit, orderings);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ClpTopNSpec other = (ClpTopNSpec) obj;
        return this.limit == other.limit && this.orderings.equals(other.orderings);
    }

    @Override
    public String toString()
    {
        return "ClpTopNSpec (limit=" + limit + ", order=" + orderings + ")";
    }
}
