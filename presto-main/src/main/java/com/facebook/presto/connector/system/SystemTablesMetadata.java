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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.connector.system.SystemColumnHandle.toSystemColumnHandles;
import static com.facebook.presto.metadata.MetadataUtil.findColumnMetadata;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;

public class SystemTablesMetadata
        implements ConnectorMetadata
{
    private final ConnectorId connectorId;

    private final SystemTablesProvider tables;

    public SystemTablesMetadata(ConnectorId connectorId, SystemTablesProvider tables)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId");
        this.tables = requireNonNull(tables, "tables is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return tables.listSystemTables(session).stream()
                .map(table -> table.getTableMetadata().getTable().getSchemaName())
                .distinct()
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<SystemTable> table = tables.getSystemTable(session, tableName);
        if (!table.isPresent()) {
            return null;
        }
        return SystemTableHandle.fromSchemaTableName(connectorId, tableName);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        SystemTableHandle tableHandle = (SystemTableHandle) table;
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
        return checkAndGetTable(session, tableHandle).getTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return tables.listSystemTables(session).stream()
                .map(SystemTable::getTableMetadata)
                .map(ConnectorTableMetadata::getTable)
                .filter(table -> schemaNameOrNull == null || table.getSchemaName().equals(schemaNameOrNull))
                .collect(toImmutableList());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ConnectorTableMetadata tableMetadata = checkAndGetTable(session, tableHandle).getTableMetadata();

        String columnName = ((SystemColumnHandle) columnHandle).getColumnName();

        ColumnMetadata columnMetadata = findColumnMetadata(tableMetadata, columnName);
        checkArgument(columnMetadata != null, "Column %s on table %s does not exist", columnName, tableMetadata.getTable());
        return columnMetadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ConnectorTableMetadata tableMetadata = checkAndGetTable(session, tableHandle).getTableMetadata();
        return toSystemColumnHandles(((SystemTableHandle) tableHandle).getConnectorId(), tableMetadata);
    }

    private SystemTable checkAndGetTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SystemTableHandle systemTableHandle = (SystemTableHandle) tableHandle;
        return tables.getSystemTable(session, systemTableHandle.getSchemaTableName())
                // table might disappear in the meantime
                .orElseThrow(() -> new PrestoException(NOT_FOUND, format("Table %s not found", systemTableHandle.getSchemaTableName())));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        if (prefix.getTableName() != null) {
            // if table is concrete we just use tables.getSystemTable to support tables which are not listable
            SchemaTableName tableName = prefix.toSchemaTableName();
            return tables.getSystemTable(session, tableName)
                    .map(systemTable -> singletonMap(tableName, systemTable.getTableMetadata().getColumns()))
                    .orElseGet(ImmutableMap::of);
        }

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        for (SystemTable table : tables.listSystemTables(session)) {
            ConnectorTableMetadata tableMetadata = table.getTableMetadata();
            if (prefix.matches(tableMetadata.getTable())) {
                builder.put(tableMetadata.getTable(), tableMetadata.getColumns());
            }
        }
        return builder.build();
    }
}
