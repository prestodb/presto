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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class BlackHoleColumnHandle
        implements ColumnHandle
{
    private final String name;
    private final Type columnType;

    public BlackHoleColumnHandle(ColumnMetadata columnMetadata)
    {
        this(columnMetadata.getName(), columnMetadata.getType());
    }

    @JsonCreator
    public BlackHoleColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("columnType") Type columnType)
    {
        this.name = name;
        this.columnType = columnType;
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
        BlackHoleColumnHandle other = (BlackHoleColumnHandle) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.columnType, other.columnType);
    }
}
