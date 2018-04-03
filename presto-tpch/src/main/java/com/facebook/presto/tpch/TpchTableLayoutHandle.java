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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TpchTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final TpchTableHandle table;
    private final TupleDomain<ColumnHandle> predicate;
    private final long limit;

    @JsonCreator
    public TpchTableLayoutHandle(
            @JsonProperty("table") TpchTableHandle table,
            @JsonProperty("predicate") TupleDomain<ColumnHandle> predicate,
            @JsonProperty("limit") long limit)
    {
        this.table = table;
        this.predicate = predicate;
        this.limit = limit;
    }

    @JsonProperty
    public TpchTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getPredicate()
    {
        return predicate;
    }

    @JsonProperty
    public long getLimit()
    {
        return limit;
    }

    public String getConnectorId()
    {
        return table.getConnectorId();
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
