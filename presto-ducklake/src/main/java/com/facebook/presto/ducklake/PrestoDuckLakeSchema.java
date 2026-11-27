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
package com.facebook.presto.ducklake;

import com.facebook.presto.ducklake.catalog.DuckLakePartitionField;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * The JSON-serializable snapshot of a table's columns and partition fields, built once by {@link
 * SchemaBuilder} and carried in the table handle so that workers never need to query the catalog
 * to know a table's shape. {@code initialDefaults} maps a top-level column's id to the raw {@code
 * initial_default} string recorded for it in {@code ducklake_column} (for example {@code "42"});
 * nested columns never appear as keys.
 */
public class PrestoDuckLakeSchema
{
    private final List<DuckLakeColumnIdentity> columns;
    private final Map<Long, String> initialDefaults;
    private final List<DuckLakePartitionField> partitionFields;

    @JsonCreator
    public PrestoDuckLakeSchema(
            @JsonProperty("columns") List<DuckLakeColumnIdentity> columns,
            @JsonProperty("initialDefaults") Map<Long, String> initialDefaults,
            @JsonProperty("partitionFields") List<DuckLakePartitionField> partitionFields)
    {
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.initialDefaults = ImmutableMap.copyOf(requireNonNull(initialDefaults, "initialDefaults is null"));
        this.partitionFields = ImmutableList.copyOf(requireNonNull(partitionFields, "partitionFields is null"));
    }

    @JsonProperty
    public List<DuckLakeColumnIdentity> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Map<Long, String> getInitialDefaults()
    {
        return initialDefaults;
    }

    @JsonProperty
    public List<DuckLakePartitionField> getPartitionFields()
    {
        return partitionFields;
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
        PrestoDuckLakeSchema that = (PrestoDuckLakeSchema) o;
        return columns.equals(that.columns) &&
                initialDefaults.equals(that.initialDefaults) &&
                partitionFields.equals(that.partitionFields);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columns, initialDefaults, partitionFields);
    }

    @Override
    public String toString()
    {
        return "PrestoDuckLakeSchema{" +
                "columns=" + columns +
                ", initialDefaults=" + initialDefaults +
                ", partitionFields=" + partitionFields +
                '}';
    }
}
