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
package io.prestosql.plugin.tpcds;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TpcdsPartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final String table;
    private final long totalRows;

    @JsonCreator
    public TpcdsPartitioningHandle(@JsonProperty("table") String table, @JsonProperty("totalRows") long totalRows)
    {
        this.table = requireNonNull(table, "table is null");

        checkArgument(totalRows > 0, "totalRows must be at least 1");
        this.totalRows = totalRows;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public long getTotalRows()
    {
        return totalRows;
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
        TpcdsPartitioningHandle that = (TpcdsPartitioningHandle) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(totalRows, that.totalRows);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, totalRows);
    }

    @Override
    public String toString()
    {
        return table + ":" + totalRows;
    }
}
