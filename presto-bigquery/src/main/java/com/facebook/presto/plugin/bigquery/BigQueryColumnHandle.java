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
package com.facebook.presto.plugin.bigquery;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.plugin.bigquery.BigQueryType.Adaptor;
import static com.google.cloud.bigquery.Field.Mode;
import static com.google.cloud.bigquery.Field.Mode.NULLABLE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BigQueryColumnHandle
        implements ColumnHandle, Adaptor
{
    private final String name;
    private final BigQueryType bigQueryType;
    private final Mode mode;
    private final List<BigQueryColumnHandle> subColumns;
    private final String description;

    @JsonCreator
    public BigQueryColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("bigQueryType") BigQueryType bigQueryType,
            @JsonProperty("mode") Mode mode,
            @JsonProperty("subColumns") List<BigQueryColumnHandle> subColumns,
            @JsonProperty("description") String description)
    {
        this.name = requireNonNull(name, "column name cannot be null");
        this.bigQueryType = requireNonNull(bigQueryType, () -> format("column type cannot be null for column [%s]", name));
        this.mode = requireNonNull(mode, "Field mode cannot be null");
        this.subColumns = ImmutableList.copyOf(requireNonNull(subColumns, "subColumns is null"));
        this.description = description;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @Override
    @JsonProperty
    public BigQueryType getBigQueryType()
    {
        return bigQueryType;
    }

    @Override
    public Map<String, Adaptor> getBigQuerySubTypes()
    {
        return subColumns.stream().collect(toImmutableMap(BigQueryColumnHandle::getName, column -> column));
    }

    @Override
    @JsonProperty
    public Mode getMode()
    {
        return mode;
    }

    @JsonProperty
    public List<BigQueryColumnHandle> getSubColumns()
    {
        return subColumns;
    }

    @JsonProperty
    public String description()
    {
        return description;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(
                name,
                getPrestoType(),
                mode == NULLABLE,
                description,
                null,
                false,
                ImmutableMap.of());
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
        BigQueryColumnHandle that = (BigQueryColumnHandle) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(bigQueryType, that.bigQueryType) &&
                Objects.equals(mode, that.mode) &&
                Objects.equals(subColumns, that.subColumns) &&
                Objects.equals(description, that.description);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, bigQueryType, mode, subColumns, description);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", bigQueryType)
                .add("mode", mode)
                .add("subColumns", subColumns)
                .add("description", description)
                .toString();
    }
}
