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
package com.facebook.presto.pinot;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class PinotColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type dataType;
    private final PinotColumnType type;

    public PinotColumnHandle(
            VariableReferenceExpression variable,
            PinotColumnType type)
    {
        this(variable.getName(), variable.getType(), type);
    }

    @JsonCreator
    public PinotColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("dataType") Type dataType,
            @JsonProperty("type") PinotColumnType type)
    {
        this.columnName = requireNonNull(columnName, "column name is null");
        this.dataType = requireNonNull(dataType, "data type name is null");
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty("columnName")
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty("dataType")
    public Type getDataType()
    {
        return dataType;
    }

    @JsonProperty
    public PinotColumnType getType()
    {
        return type;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(getColumnName(), getDataType());
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

        PinotColumnHandle that = (PinotColumnHandle) o;
        return Objects.equals(getColumnName(), that.getColumnName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("dataType", dataType)
                .add("type", type)
                .toString();
    }

    public enum PinotColumnType
    {
        REGULAR, // refers to the column in table
        DERIVED, // refers to a derived column that is created after a pushdown expression
    }
}
