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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
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
    private final List<Subfield> requiredSubfields;

    @JsonCreator
    public TpchColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("type") Type type)
    {
        this(columnName, type, ImmutableList.of());
    }

    public TpchColumnHandle(
            String columnName,
            Type type,
            List<Subfield> requiredSubfields)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.type = requireNonNull(type, "type is null");
        this.requiredSubfields = requireNonNull(requiredSubfields, "requiredSubfields is null");
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

    @Override
    public String toString()
    {
        return "tpch:" + columnName;
    }

    public List<Subfield> getRequiredSubfields()
    {
        return requiredSubfields;
    }

    @Override
    public ColumnHandle withRequiredSubfields(List<Subfield> subfields)
    {
        if (!requiredSubfields.isEmpty()) {
            // This column is already a pushed down subfield column.
            return this;
        }

        return new TpchColumnHandle(columnName, type, subfields);
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
