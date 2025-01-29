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
package com.facebook.plugin.arrow;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ArrowTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final ArrowTableHandle table;
    private final List<ArrowColumnHandle> columnHandles;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public ArrowTableLayoutHandle(
            @JsonProperty("table") ArrowTableHandle table,
            @JsonProperty("columnHandles") List<ArrowColumnHandle> columnHandles,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> domain)
    {
        this.table = requireNonNull(table, "table is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.tupleDomain = requireNonNull(domain, "tupleDomain is null");
    }

    @JsonProperty("table")
    public ArrowTableHandle getTable()
    {
        return table;
    }

    @JsonProperty("tupleDomain")
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @JsonProperty("columnHandles")
    public List<ArrowColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @Override
    public String toString()
    {
        return "table:" + table + ", columnHandles:" + columnHandles + ", tupleDomain:" + tupleDomain;
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
        ArrowTableLayoutHandle arrowTableLayoutHandle = (ArrowTableLayoutHandle) o;
        return Objects.equals(table, arrowTableLayoutHandle.table) && Objects.equals(columnHandles, arrowTableLayoutHandle.columnHandles) && Objects.equals(tupleDomain, arrowTableLayoutHandle.tupleDomain);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, columnHandles, tupleDomain);
    }
}
