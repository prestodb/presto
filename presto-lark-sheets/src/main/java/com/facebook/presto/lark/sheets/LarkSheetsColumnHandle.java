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
package com.facebook.presto.lark.sheets;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class LarkSheetsColumnHandle
        implements ColumnHandle
{
    private final String name;
    private final Type type;
    private final int index;

    @JsonCreator
    public LarkSheetsColumnHandle(@JsonProperty("name") String name, @JsonProperty("type") Type type, @JsonProperty("index") int index)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.index = index;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public int getIndex()
    {
        return index;
    }

    public ColumnMetadata toColumnMetadata()
    {
        return new ColumnMetadata(name, type);
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
        LarkSheetsColumnHandle that = (LarkSheetsColumnHandle) o;
        return index == that.index && Objects.equals(name, that.name) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, index);
    }

    @Override
    public String toString()
    {
        return name + ":" + type + ":" + index;
    }
}
