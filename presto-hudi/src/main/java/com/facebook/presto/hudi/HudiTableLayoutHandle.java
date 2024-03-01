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

package com.facebook.presto.hudi;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class HudiTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final HudiTableHandle table;
    private final List<HudiColumnHandle> dataColumns;
    private final List<HudiColumnHandle> partitionColumns;
    private final Map<String, String> tableParameters;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public HudiTableLayoutHandle(
            @JsonProperty("table") HudiTableHandle table,
            @JsonProperty("dataColumns") List<HudiColumnHandle> dataColumns,
            @JsonProperty("partitionColumns") List<HudiColumnHandle> partitionColumns,
            @JsonProperty("tableParameters") Map<String, String> tableParameters,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain)
    {
        this.table = requireNonNull(table, "table is null");
        this.dataColumns = requireNonNull(dataColumns, "dataColumns is null");
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
        this.tableParameters = requireNonNull(tableParameters, "tableParameters is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
    }

    @JsonProperty
    public HudiTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public List<HudiColumnHandle> getDataColumns()
    {
        return dataColumns;
    }

    @JsonProperty
    public List<HudiColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    @JsonProperty
    public Map<String, String> getTableParameters()
    {
        return tableParameters;
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
        HudiTableLayoutHandle that = (HudiTableLayoutHandle) o;
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
