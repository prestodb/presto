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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

public class TpchTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final TpchTableHandle table;
    private final TupleDomain<ColumnHandle> predicate;

    @JsonCreator
    public TpchTableLayoutHandle(@JsonProperty("table") TpchTableHandle table, @JsonProperty("predicate") TupleDomain<ColumnHandle> predicate)
    {
        this.table = table;
        this.predicate = predicate;
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

    @Override
    public String toString()
    {
        return table.toString();
    }

    @Override
    public Object getIdentifier(Optional<ConnectorSplit> split)
    {
        return ImmutableMap.builder()
                .put("table", table)
                .put("predicate", predicate)
                .build();
    }
}
