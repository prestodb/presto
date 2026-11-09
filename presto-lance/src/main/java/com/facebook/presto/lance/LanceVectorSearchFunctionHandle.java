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
package com.facebook.presto.lance;

import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Result of analyzing a {@code system.vector_search} invocation: the fully-resolved
 * {@link LanceTableHandle} (version pinned, {@link LanceVectorSearch} attached with
 * {@code requiresGlobalTopK=true}) plus the output columns in declared order
 * (table columns followed by the synthetic {@code _distance} column).
 */
public class LanceVectorSearchFunctionHandle
        implements ConnectorTableFunctionHandle
{
    private final LanceTableHandle tableHandle;
    private final List<LanceColumnHandle> columns;

    @JsonCreator
    public LanceVectorSearchFunctionHandle(
            @JsonProperty("tableHandle") LanceTableHandle tableHandle,
            @JsonProperty("columns") List<LanceColumnHandle> columns)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    @JsonProperty
    public LanceTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public List<LanceColumnHandle> getColumns()
    {
        return columns;
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
        LanceVectorSearchFunctionHandle that = (LanceVectorSearchFunctionHandle) o;
        return Objects.equals(tableHandle, that.tableHandle) && Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableHandle, columns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableHandle", tableHandle)
                .add("columns", columns)
                .toString();
    }
}
