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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ClpColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final String originalColumnName;
    private final Type columnType;
    private final boolean nullable;

    @JsonCreator
    public ClpColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("originalColumnName") String originalColumnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("nullable") boolean nullable)
    {
        this.columnName = columnName;
        this.originalColumnName = originalColumnName;
        this.columnType = columnType;
        this.nullable = nullable;
    }

    public ClpColumnHandle(String columnName, Type columnType, boolean nullable)
    {
        this(columnName, columnName, columnType, nullable);
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public String getOriginalColumnName()
    {
        return originalColumnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public boolean isNullable()
    {
        return nullable;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(columnType)
                .setNullable(nullable)
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, originalColumnName, columnType, nullable);
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
        ClpColumnHandle other = (ClpColumnHandle) obj;
        return Objects.equals(this.columnName, other.columnName) &&
                Objects.equals(this.originalColumnName, other.originalColumnName) &&
                Objects.equals(this.columnType, other.columnType) &&
                Objects.equals(this.nullable, other.nullable);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("originalColumnName", originalColumnName)
                .add("columnType", columnType)
                .add("nullable", nullable)
                .toString();
    }
}
