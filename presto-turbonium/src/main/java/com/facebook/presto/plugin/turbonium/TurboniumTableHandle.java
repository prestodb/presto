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

package com.facebook.presto.plugin.turbonium;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class TurboniumTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final Long tableId;
    private final List<TurboniumColumnHandle> columnHandles;
    private final List<HostAddress> hosts;
    private final long splitsPerWorker;

    public TurboniumTableHandle(
            String connectorId,
            Long tableId,
            ConnectorTableMetadata tableMetadata,
            List<HostAddress> hosts,
            long splitsPerWorker)
    {
        this(connectorId,
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                tableId,
                TurboniumColumnHandle.extractColumnHandles(tableMetadata.getColumns()),
                hosts,
                splitsPerWorker);
    }

    @JsonCreator
    public TurboniumTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableId") Long tableId,
            @JsonProperty("columnHandles") List<TurboniumColumnHandle> columnHandles,
            @JsonProperty("hosts") List<HostAddress> hosts,
            @JsonProperty("splitsPerWorker") long splitsPerWorker)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableId = requireNonNull(tableId, "tableId is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.hosts = requireNonNull(hosts, "hosts is null");
        this.splitsPerWorker = splitsPerWorker;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
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
    public Long getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public List<TurboniumColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @JsonProperty
    public List<HostAddress> getHosts()
    {
        return hosts;
    }

    @JsonProperty
    public long getSplitsPerWorker()
    {
        return splitsPerWorker;
    }

    public ConnectorTableMetadata toTableMetadata()
    {
        return new ConnectorTableMetadata(
                toSchemaTableName(),
                columnHandles.stream().map(TurboniumColumnHandle::toColumnMetadata).collect(toList()));
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getConnectorId(), getTableId());
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
        TurboniumTableHandle other = (TurboniumTableHandle) obj;
        return Objects.equals(this.getConnectorId(), other.getConnectorId()) &&
                Objects.equals(this.getTableId(), other.getTableId()) &&
                this.getSplitsPerWorker() == other.getSplitsPerWorker();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("tableId", tableId)
                .add("columnHandles", columnHandles)
                .add("splitsPerWorker", splitsPerWorker)
                .toString();
    }
}
