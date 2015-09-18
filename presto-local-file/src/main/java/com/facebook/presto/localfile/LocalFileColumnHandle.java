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
package com.facebook.presto.localfile;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LocalFileColumnHandle
        implements ColumnHandle
{
    public static final int SERVER_ADDRESS_ORDINAL_POSITION = -1;
    public static final String SERVER_ADDRESS_COLUMN_NAME = "server_address";

    private final String columnName;
    private final Type columnType;
    private final int ordinalPosition;

    @JsonCreator
    public LocalFileColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("ordinalPosition") int ordinalPosition)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.ordinalPosition = ordinalPosition;
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
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public ColumnMetadata toColumnMetadata()
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
        LocalFileColumnHandle that = (LocalFileColumnHandle) o;
        return Objects.equals(columnName, that.columnName) &&
                Objects.equals(columnType, that.columnType) &&
                Objects.equals(ordinalPosition, that.ordinalPosition);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, columnType, ordinalPosition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("ordinalPosition", ordinalPosition)
                .toString();
    }
}
