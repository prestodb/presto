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
package com.facebook.presto.plugin.memory;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class MemoryTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final MemoryTableHandle table;
    private final List<MemoryDataFragment> dataFragments;

    @JsonCreator
    public MemoryTableLayoutHandle(
            @JsonProperty("table") MemoryTableHandle table,
            @JsonProperty("dataFragments") List<MemoryDataFragment> dataFragments)
    {
        this.table = requireNonNull(table, "table is null");
        this.dataFragments = requireNonNull(dataFragments, "dataFragments is null");
    }

    @JsonProperty
    public MemoryTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public List<MemoryDataFragment> getDataFragments()
    {
        return dataFragments;
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
