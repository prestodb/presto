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
package com.facebook.presto.tablestore;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TablestoreTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final TablestoreTableHandle table;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public TablestoreTableLayoutHandle(
            @JsonProperty("table") TablestoreTableHandle table,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain)
    {
        this.table = requireNonNull(table, "table is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
    }

    @JsonProperty
    public TablestoreTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public Optional<TupleDomain<ColumnHandle>> getTupleDomain()
    {
        return Optional.of(tupleDomain);
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
        TablestoreTableLayoutHandle that = (TablestoreTableLayoutHandle) o;
        return Objects.equals(table, that.table) && Objects.equals(tupleDomain, that.tupleDomain);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, tupleDomain);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
