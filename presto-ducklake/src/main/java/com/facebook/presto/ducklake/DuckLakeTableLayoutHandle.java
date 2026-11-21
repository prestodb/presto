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
package com.facebook.presto.ducklake;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * The layout the split manager prunes with. {@code domainPredicate} is the tuple domain pushed
 * down so far (Iceberg's {@code partitionColumnPredicate} plays the same role); this connector has
 * no filter pushdown beyond tuple domains, so there is no remaining-predicate {@code RowExpression}
 * or subfield domain to carry.
 */
public class DuckLakeTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final DuckLakeTableHandle table;
    private final TupleDomain<ColumnHandle> domainPredicate;
    private final Map<String, DuckLakeColumnHandle> predicateColumns;
    private final Optional<Set<DuckLakeColumnHandle>> requestedColumns;

    @JsonCreator
    public DuckLakeTableLayoutHandle(
            @JsonProperty("table") DuckLakeTableHandle table,
            @JsonProperty("domainPredicate") TupleDomain<ColumnHandle> domainPredicate,
            @JsonProperty("predicateColumns") Map<String, DuckLakeColumnHandle> predicateColumns,
            @JsonProperty("requestedColumns") Optional<Set<DuckLakeColumnHandle>> requestedColumns)
    {
        this.table = requireNonNull(table, "table is null");
        this.domainPredicate = requireNonNull(domainPredicate, "domainPredicate is null");
        this.predicateColumns = ImmutableMap.copyOf(requireNonNull(predicateColumns, "predicateColumns is null"));
        this.requestedColumns = requireNonNull(requestedColumns, "requestedColumns is null");
    }

    @JsonProperty
    public DuckLakeTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getDomainPredicate()
    {
        return domainPredicate;
    }

    @JsonProperty
    public Map<String, DuckLakeColumnHandle> getPredicateColumns()
    {
        return predicateColumns;
    }

    @JsonProperty
    public Optional<Set<DuckLakeColumnHandle>> getRequestedColumns()
    {
        return requestedColumns;
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
        DuckLakeTableLayoutHandle that = (DuckLakeTableLayoutHandle) o;
        return table.equals(that.table) &&
                domainPredicate.equals(that.domainPredicate) &&
                predicateColumns.equals(that.predicateColumns) &&
                requestedColumns.equals(that.requestedColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, domainPredicate, predicateColumns, requestedColumns);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
