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
package com.facebook.presto.connector.system;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.connector.system.SystemColumnHandle.toSystemColumnHandles;
import static com.facebook.presto.metadata.MetadataUtil.findColumnMetadata;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class SystemTablesMetadata
        implements ConnectorMetadata
{
    private final ConnectorId connectorId;
    private final Map<SchemaTableName, ConnectorTableMetadata> tables;

    public SystemTablesMetadata(ConnectorId connectorId, Set<SystemTable> tables)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId");
        this.tables = tables.stream()
                .map(SystemTable::getTableMetadata)
                .collect(toMap(ConnectorTableMetadata::getTable, identity()));
    }

    private SystemTableHandle checkTableHandle(ConnectorTableHandle tableHandle)
    {
        SystemTableHandle systemTableHandle = checkType(tableHandle, SystemTableHandle.class, "tableHandle");
        checkArgument(tables.containsKey(systemTableHandle.getSchemaTableName()));
        return systemTableHandle;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return tables.keySet().stream()
                .map(SchemaTableName::getSchemaName)
                .distinct()
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (!tables.containsKey(tableName)) {
            return null;
        }
        return SystemTableHandle.fromSchemaTableName(connectorId, tableName);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        SystemTableHandle tableHandle = checkType(table, SystemTableHandle.class, "table");
        ConnectorTableLayout layout = new ConnectorTableLayout(new SystemTableLayoutHandle(tableHandle.getConnectorId(), tableHandle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SystemTableHandle systemTableHandle = checkTableHandle(tableHandle);
        return tables.get(systemTableHandle.getSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return ImmutableList.copyOf(tables.keySet());
        }

        return tables.keySet().stream()
                .filter(table -> table.getSchemaName().equals(schemaNameOrNull))
                .collect(toImmutableList());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        SystemTableHandle systemTableHandle = checkTableHandle(tableHandle);
        ConnectorTableMetadata tableMetadata = tables.get(systemTableHandle.getSchemaTableName());

        String columnName = checkType(columnHandle, SystemColumnHandle.class, "columnHandle").getColumnName();

        ColumnMetadata columnMetadata = findColumnMetadata(tableMetadata, columnName);
        checkArgument(columnMetadata != null, "Column %s on table %s does not exist", columnName, tableMetadata.getTable());
        return columnMetadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SystemTableHandle systemTableHandle = checkTableHandle(tableHandle);

        return toSystemColumnHandles(systemTableHandle.getConnectorId(), tables.get(systemTableHandle.getSchemaTableName()));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        for (Entry<SchemaTableName, ConnectorTableMetadata> entry : tables.entrySet()) {
            if (prefix.matches(entry.getKey())) {
                builder.put(entry.getKey(), entry.getValue().getColumns());
            }
        }
        return builder.build();
    }
}
