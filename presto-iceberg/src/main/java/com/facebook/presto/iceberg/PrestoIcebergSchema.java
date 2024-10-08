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
package com.facebook.presto.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class PrestoIcebergSchema
{
    private final int schemaId;
    private final List<PrestoIcebergNestedField> columns;
    private final Map<String, Integer> columnNameToIdMapping;
    private final Map<String, Integer> aliases;
    private final Set<Integer> identifierFieldIds;

    @JsonCreator
    public PrestoIcebergSchema(
            @JsonProperty("schemaId") int schemaId,
            @JsonProperty("columns") List<PrestoIcebergNestedField> columns,
            @JsonProperty("columnNameToIdMapping") Map<String, Integer> columnNameToIdMapping,
            @JsonProperty("aliases") Map<String, Integer> aliases,
            @JsonProperty("identifierFieldIds") Set<Integer> identifierFieldIds)
    {
        this.schemaId = schemaId;
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.columnNameToIdMapping = ImmutableMap.copyOf(requireNonNull(columnNameToIdMapping, "columnNameToIdMapping is null"));
        this.aliases = aliases != null ? ImmutableMap.copyOf(aliases) : ImmutableMap.of();
        this.identifierFieldIds = ImmutableSet.copyOf(requireNonNull(identifierFieldIds, "identifierFieldIds is null"));
    }

    @JsonProperty
    public int getSchemaId()
    {
        return schemaId;
    }

    @JsonProperty
    public List<PrestoIcebergNestedField> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Map<String, Integer> getColumnNameToIdMapping()
    {
        return columnNameToIdMapping;
    }

    @JsonProperty
    public Map<String, Integer> getAliases()
    {
        return aliases;
    }

    @JsonProperty
    public Set<Integer> getIdentifierFieldIds()
    {
        return identifierFieldIds;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaId, columns, columnNameToIdMapping, aliases, identifierFieldIds);
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

        PrestoIcebergSchema other = (PrestoIcebergSchema) obj;
        return Objects.equals(this.schemaId, other.schemaId) &&
                Objects.equals(this.columns, other.columns) &&
                Objects.equals(this.columnNameToIdMapping, other.columnNameToIdMapping) &&
                Objects.equals(this.aliases, other.aliases) &&
                Objects.equals(this.identifierFieldIds, other.identifierFieldIds);
    }
}
