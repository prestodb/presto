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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class MemoryColumnHandle
        implements ColumnHandle
{
    private final String name;
    private final Type columnType;
    private final int columnIndex;

    public MemoryColumnHandle(ColumnMetadata columnMetadata, int columnIndex)
    {
        this(columnMetadata.getName(), columnMetadata.getType(), columnIndex);
    }

    @JsonCreator
    public MemoryColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("columnIndex") int columnIndex)
    {
        this.name = requireNonNull(name, "name is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.columnIndex = columnIndex;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public int getColumnIndex()
    {
        return columnIndex;
    }

    public ColumnMetadata toColumnMetadata()
    {
        return new ColumnMetadata(name, columnType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, columnType);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MemoryColumnHandle other = (MemoryColumnHandle) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.columnType, other.columnType) &&
                Objects.equals(this.columnIndex, other.columnIndex);
    }

    public static List<MemoryColumnHandle> extractColumnHandles(List<ColumnMetadata> columns)
    {
        ImmutableList.Builder<MemoryColumnHandle> columnHandles = ImmutableList.builder();
        int columnIndex = 0;
        for (ColumnMetadata column : columns) {
            columnHandles.add(new MemoryColumnHandle(column, columnIndex));
            columnIndex++;
        }
        return columnHandles.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("columnType", columnType)
                .add("columnIndex", columnIndex)
                .toString();
    }
}
