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
package com.facebook.presto.tablestore;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TablestoreColumnHandle
        implements ColumnHandle, Comparable<TablestoreColumnHandle>
{
    private final String columnName;
    private final boolean primaryKey;
    private final int pkPosition;
    private final Type columnType;

    @JsonCreator
    public TablestoreColumnHandle(@JsonProperty("columnName") String columnName,
            @JsonProperty("primaryKey") boolean primaryKey,
            @JsonProperty("pkPosition") int pkPosition,
            @JsonProperty("columnType") Type columnType)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.primaryKey = primaryKey;
        this.pkPosition = pkPosition;
        this.columnType = requireNonNull(columnType, "columnType is null");
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
    public int getPkPosition()
    {
        return pkPosition;
    }

    @JsonProperty
    public boolean isPrimaryKey()
    {
        return primaryKey;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType);
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
        TablestoreColumnHandle that = (TablestoreColumnHandle) o;
        return primaryKey == that.primaryKey
                && pkPosition == that.pkPosition
                && Objects.equals(columnName, that.columnName)
                && Objects.equals(columnType, that.columnType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, primaryKey, pkPosition, columnType);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("primaryKey", primaryKey)
                .add("columnType", columnType)
                .toString();
    }

    @Override
    public int compareTo(@NotNull TablestoreColumnHandle o)
    {
        return columnName.compareTo(o.columnName);
    }
}
