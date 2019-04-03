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
import com.facebook.presto.spi.SubfieldPath;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class TpchColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type type;
    private final List<SubfieldPath> subfieldPaths;

    @JsonCreator
    public TpchColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("type") Type type,
            @JsonProperty("subfieldPaths") List<SubfieldPath> subfieldPaths)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.type = requireNonNull(type, "type is null");
        this.subfieldPaths = requireNonNull(subfieldPaths, "subfieldPaths is null");
    }

    public TpchColumnHandle(String columnName, Type type)
    {
        this(columnName, type, ImmutableList.of());
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public List<SubfieldPath> getSubfieldPaths()
    {
        return subfieldPaths;
    }

    @Override
    public String toString()
    {
        if (!subfieldPaths.isEmpty()) {
            return "tpch:" + columnName + subfieldPaths;
        }
        return "tpch:" + columnName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        TpchColumnHandle other = (TpchColumnHandle) o;
        return Objects.equals(columnName, other.columnName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName);
    }
}
