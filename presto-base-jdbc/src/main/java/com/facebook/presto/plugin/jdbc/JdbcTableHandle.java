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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import jakarta.annotation.Nullable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class JdbcTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final SchemaTableName schemaTableName;

    // catalog, schema and table names are reported by the remote database
    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    @ThriftConstructor
    public JdbcTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @JsonProperty
    @ThriftField(1)
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    @ThriftField(2)
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    @ThriftField(3)
    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    @ThriftField(4)
    @Nullable
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    @ThriftField(5)
    public String getTableName()
    {
        return tableName;
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
                Objects.equals(this.schemaTableName, o.schemaTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaTableName);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").useForNull("null").join(connectorId, schemaTableName, catalogName, schemaName, tableName);
    }
}
