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
package com.facebook.presto.druid;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DruidColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type columnType;
    private final DruidColumnType type;

    public DruidColumnHandle(
            VariableReferenceExpression variable,
            DruidColumnType type)
    {
        this(variable.getName(), variable.getType(), type);
    }

    public DruidColumnHandle(
            String columnName,
            Type columnType)
    {
        this(columnName, columnType, DruidColumnType.REGULAR);
    }

    @JsonCreator
    public DruidColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("type") DruidColumnType type)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public DruidColumnType getType()
    {
        return type;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(getColumnName(), getColumnType());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        DruidColumnHandle other = (DruidColumnHandle) obj;
        return Objects.equals(this.columnName, other.columnName) &&
                Objects.equals(this.columnType, other.columnType);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("type", type)
                .toString();
    }

    public enum DruidColumnType
    {
        REGULAR, // refers to the column in table
        DERIVED, // refers to a derived column that is created after a pushdown expression
    }
}
