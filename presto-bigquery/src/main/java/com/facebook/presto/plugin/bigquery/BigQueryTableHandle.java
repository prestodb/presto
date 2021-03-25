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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class BigQueryTableHandle
        implements ConnectorTableHandle
{
    private final String projectId;
    private final String schemaName;
    private final String tableName;
    private final String type;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<List<ColumnHandle>> projectedColumns;
    private final OptionalLong limit;

    @JsonCreator
    public BigQueryTableHandle(
            @JsonProperty("projectId") String projectId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("type") String type,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("projectedColumns") Optional<List<ColumnHandle>> projectedColumns,
            @JsonProperty("limit") OptionalLong limit)
    {
        this.projectId = requireNonNull(projectId, "projectId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.type = requireNonNull(type, "type is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.projectedColumns = requireNonNull(projectedColumns, "projectedColumns is null");
        this.limit = requireNonNull(limit, "limit is null");
    }

    public static BigQueryTableHandle from(TableInfo tableInfo)
    {
        TableId tableId = tableInfo.getTableId();
        String type = tableInfo.getDefinition().getType().toString();
        return new BigQueryTableHandle(tableId.getProject(), tableId.getDataset(), tableId.getTable(), type, TupleDomain.none(), Optional.empty(), OptionalLong.empty());
    }

    @JsonProperty
    public String getProjectId()
    {
        return projectId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getProjectedColumns()
    {
        return projectedColumns;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
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
        BigQueryTableHandle that = (BigQueryTableHandle) o;
        return Objects.equals(projectId, that.projectId) &&
                Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(type, that.tableName) &&
                Objects.equals(constraint, that.constraint) &&
                Objects.equals(projectedColumns, that.projectedColumns) &&
                Objects.equals(limit, that.limit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(projectId, schemaName, tableName, type, constraint, projectedColumns, limit);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("projectId", projectId)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("type", type)
                .add("constraint", constraint)
                .add("projectedColumns", projectedColumns)
                .add("limit", limit)
                .toString();
    }

    public TableId getTableId()
    {
        return TableId.of(projectId, schemaName, tableName);
    }

    BigQueryTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint)
    {
        return new BigQueryTableHandle(projectId, schemaName, tableName, type, newConstraint, projectedColumns, limit);
    }

    BigQueryTableHandle withProjectedColumns(List<ColumnHandle> newProjectedColumns)
    {
        return new BigQueryTableHandle(projectId, schemaName, tableName, type, constraint, Optional.of(newProjectedColumns), limit);
    }
}
