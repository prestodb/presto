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
import com.google.common.collect.ComparisonChain;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ElasticsearchColumnHandle
        implements ColumnHandle, Comparable<ElasticsearchColumnHandle>
{
    private final ElasticsearchColumnMetadata column;

    @JsonCreator
    public ElasticsearchColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("columnJsonPath") String columnJsonPath,
            @JsonProperty("columnJsonType") String columnJsonType,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("isList") boolean isList)
    {
        this.column = new ElasticsearchColumnMetadata(
                requireNonNull(columnName, "columnName is null"),
                requireNonNull(columnType, "columnType is null"),
                requireNonNull(columnJsonPath, "columnJsonPath is null"),
                requireNonNull(columnJsonType, "columnJsonType is null"),
                isList,
                ordinalPosition);
    }

    @JsonProperty
    public String getColumnName()
    {
        return column.getName();
    }

    @JsonProperty
    public Type getColumnType()
    {
        return column.getType();
    }

    @JsonProperty
    public String getColumnJsonPath()
    {
        return column.getJsonPath();
    }

    @JsonProperty
    public String getColumnJsonType()
    {
        return column.getJsonType();
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return column.getOrdinalPosition();
    }

    @JsonProperty
    public boolean getIsList()
    {
        return column.isList();
    }

    public ColumnMetadata getColumnMetadata()
    {
        return column;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                column.getName(),
                column.getType(),
                column.getOrdinalPosition(),
                column.getJsonPath(),
                column.getJsonType(),
                column.isList());
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
                Objects.equals(this.getOrdinalPosition(), other.getOrdinalPosition()) &&
                Objects.equals(this.getColumnType(), other.getColumnType()) &&
                Objects.equals(this.getColumnJsonPath(), other.getColumnJsonPath()) &&
                Objects.equals(this.getColumnJsonType(), other.getColumnJsonType()) &&
                Objects.equals(this.getIsList(), other.getIsList());
    }

    @Override
    public int compareTo(ElasticsearchColumnHandle other)
    {
        return ComparisonChain.start()
                .compare(this.getColumnName(), other.getColumnName())
                .compare(this.getOrdinalPosition(), other.getOrdinalPosition())
                .compare(this.getColumnType().getTypeSignature().toString(), other.getColumnType().getTypeSignature().toString())
                .compare(this.getColumnJsonPath(), other.getColumnJsonPath())
                .compare(this.getColumnJsonType(), other.getColumnJsonType())
                .compare(this.getIsList(), other.getIsList())
                .result();
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
