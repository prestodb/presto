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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ClickHouseOutputTableHandle
        implements ConnectorOutputTableHandle, ConnectorInsertTableHandle
{
    private final String connectorId;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final String temporaryTableName;

    @JsonCreator
    public ClickHouseOutputTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("temporaryTableName") String temporaryTableName)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.temporaryTableName = requireNonNull(temporaryTableName, "temporaryTableName is null");

        requireNonNull(columnNames, "columnNames is null");
        requireNonNull(columnTypes, "columnTypes is null");
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes sizes don't match");
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    @Nullable
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
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @JsonProperty
    public String getTemporaryTableName()
    {
        return temporaryTableName;
    }

    @Override
    public String toString()
    {
        return format("jdbc:%s.%s.%s", catalogName, schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                connectorId,
                catalogName,
                schemaName,
                tableName,
                columnNames,
                columnTypes,
                temporaryTableName);
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
        ClickHouseOutputTableHandle other = (ClickHouseOutputTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.catalogName, other.catalogName) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.columnNames, other.columnNames) &&
                Objects.equals(this.columnTypes, other.columnTypes) &&
                Objects.equals(this.temporaryTableName, other.temporaryTableName);
    }
}
