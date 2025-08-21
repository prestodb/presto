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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class JdbcTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final SchemaTableName schemaTableName;

    // catalog, schema and table names are reported by the remote database
    private final String catalogName;
    private final String schemaName;
    private final String tableName;

//    joinTables holds all the table handles for the same connector which are participating for the join push down
//    this is used later in query builder to rebuild the join query

    private List<ConnectorTableHandle> joinTables;
    private Optional<String> tableAlias;

    @JsonCreator
    public JdbcTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("joinTables") List<ConnectorTableHandle> joinTables,
            @JsonProperty("tableAlias") Optional<String> tableAlias)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.joinTables = requireNonNull(joinTables, "joinTables is null");
        this.tableAlias = requireNonNull(tableAlias, "tableAlias is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
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
    public List<ConnectorTableHandle> getJoinTables()
    {
        return joinTables;
    }

    @JsonProperty
    public Optional<String> getTableAlias()
    {
        return tableAlias;
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
        JdbcTableHandle o = (JdbcTableHandle) obj;
        return Objects.equals(this.connectorId, o.connectorId) &&
                Objects.equals(this.schemaTableName, o.schemaTableName) &&
                Objects.equals(this.tableAlias, o.tableAlias) &&
                Objects.equals(this.joinTables, o.joinTables);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaTableName, tableAlias);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schemaTableName", schemaTableName)
                .add("catalogName", catalogName)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("joinTables", joinTables)
                .toString();
    }

    public boolean hasJoinTables()
    {
        return !this.getJoinTables().isEmpty();
    }
}
