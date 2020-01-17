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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ElasticsearchColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type columnType;
    private final String columnJsonPath;
    private final String columnJsonType;
    private final int ordinalPosition;
    private final boolean isList;

    @JsonCreator
    public ElasticsearchColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("columnJsonPath") String columnJsonPath,
            @JsonProperty("columnJsonType") String columnJsonType,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("isList") boolean isList)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.columnJsonPath = requireNonNull(columnJsonPath, "columnJsonPath is null");
        this.columnJsonType = requireNonNull(columnJsonType, "columnJsonType is null");
        this.ordinalPosition = ordinalPosition;
        this.isList = isList;
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
    public String getColumnJsonPath()
    {
        return columnJsonPath;
    }

    @JsonProperty
    public String getColumnJsonType()
    {
        return columnJsonType;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @JsonProperty
    public boolean getIsList()
    {
        return isList;
    }

    public ColumnMetadata getColumnMetadata()
    {
        Map<String, Object> properties = new HashMap<>();
        properties.put("jsonPath", columnJsonPath);
        properties.put("jsonType", columnJsonType);
        properties.put("isList", isList);
        properties.put("ordinalPosition", ordinalPosition);
        return new ColumnMetadata(columnName, columnType, "", "", false, properties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                columnName,
                columnType,
                columnJsonPath,
                columnJsonType,
                ordinalPosition,
                isList);
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

        ElasticsearchColumnHandle other = (ElasticsearchColumnHandle) obj;
        return Objects.equals(this.getColumnName(), other.getColumnName()) &&
                Objects.equals(this.getColumnType(), other.getColumnType()) &&
                Objects.equals(this.getColumnJsonPath(), other.getColumnJsonPath()) &&
                Objects.equals(this.getColumnJsonType(), other.getColumnJsonType()) &&
                this.getOrdinalPosition() == other.getOrdinalPosition() &&
                this.getIsList() == other.getIsList();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", getColumnName())
                .add("columnType", getColumnType())
                .add("columnJsonPath", getColumnJsonPath())
                .add("columnJsonType", getColumnJsonType())
                .add("ordinalPosition", getOrdinalPosition())
                .add("isList", getIsList())
                .toString();
    }
}
