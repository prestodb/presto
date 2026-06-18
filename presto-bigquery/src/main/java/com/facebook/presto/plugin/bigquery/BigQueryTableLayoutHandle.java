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
package com.facebook.presto.plugin.bigquery;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class BigQueryTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final BigQueryTableHandle table;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public BigQueryTableLayoutHandle(
            @JsonProperty("table") BigQueryTableHandle table,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain)
    {
        this.table = table;
        this.tupleDomain = tupleDomain;
    }

    BigQueryTableLayoutHandle(BigQueryTableHandle table)
    {
        this(table, TupleDomain.none());
    }

    @JsonProperty
    public BigQueryTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
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
        BigQueryTableLayoutHandle that = (BigQueryTableLayoutHandle) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(tupleDomain, that.tupleDomain);
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
