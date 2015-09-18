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
package com.facebook.presto.localfile;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.localfile.LocalFileTables.HttpRequestLogTable;
import static com.facebook.presto.localfile.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class LocalFileMetadata
        implements ConnectorMetadata
{
    public static final String PRESTO_LOGS_SCHEMA = "logs";
    public static final ColumnMetadata SERVER_ADDRESS_COLUMN = new ColumnMetadata("server_address", VarcharType.VARCHAR, false);
    private static final List<String> SCHEMA_NAMES = ImmutableList.of(PRESTO_LOGS_SCHEMA);

    private final LocalFileConnectorId connectorId;
    private final Map<SchemaTableName, LocalFileTableHandle> tables;
    private final Map<SchemaTableName, List<ColumnMetadata>> tableColumns;

    @Inject
    public LocalFileMetadata(LocalFileConnectorId connectorId, LocalFileConfig config)
    {
        this(connectorId, config.getHttpRequestLog().toURI());
    }

    public LocalFileMetadata(LocalFileConnectorId connectorId, URI httpRequestLog)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        ImmutableMap.Builder<SchemaTableName, LocalFileTableHandle> tables = ImmutableMap.builder();
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumns = ImmutableMap.builder();

        if (httpRequestLog != null) {
            SchemaTableName table = HttpRequestLogTable.getSchemaTableName();
            // TODO Add support for reading all files in a specified directory
            LocalFileTableHandle tableHandle = new LocalFileTableHandle(connectorId, table, httpRequestLog);
            tables.put(table, tableHandle);
            tableColumns.put(table, HttpRequestLogTable.getColumns());
        }

        this.tables = tables.build();
        this.tableColumns = tableColumns.build();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return SCHEMA_NAMES;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        checkArgument(SCHEMA_NAMES.contains(tableName.getSchemaName()));

        return tables.get(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        LocalFileTableHandle tableHandle = checkType(table, LocalFileTableHandle.class, "tableHandle");
        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), getColumns(tableHandle));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return ImmutableList.copyOf(tables.keySet());
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        LocalFileTableHandle tableHandle = checkType(table, LocalFileTableHandle.class, "tableHandle");
        ConnectorTableLayout layout = new ConnectorTableLayout(
                new LocalFileTableLayoutHandle(tableHandle),
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        LocalFileTableLayoutHandle layout = checkType(handle, LocalFileTableLayoutHandle.class, "layout");
        return new ConnectorTableLayout(layout, Optional.empty(), TupleDomain.<ColumnHandle>all(), Optional.empty(), Optional.empty(), ImmutableList.of());
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return false;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table)
    {
        LocalFileTableHandle tableHandle = checkType(table, LocalFileTableHandle.class, "tableHandle");
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : getColumns(tableHandle)) {
            int fieldIndex;
            if (column.equals(SERVER_ADDRESS_COLUMN)) {
                fieldIndex = -1;
            }
            else {
                fieldIndex = index;
                index++;
            }
            columnHandles.put(column.getName(), new LocalFileColumnHandle(connectorId, column.getName(), column.getType(), fieldIndex));
        }
        return columnHandles.build();
    }

    private List<ColumnMetadata> getColumns(LocalFileTableHandle tableHandle)
    {
        checkArgument(tableColumns.containsKey(tableHandle.getSchemaTableName()), "Table %s not registered", tableHandle.getSchemaTableName());
        return tableColumns.get(tableHandle.getSchemaTableName());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, LocalFileTableHandle.class, "tableHandle");
        return checkType(columnHandle, LocalFileColumnHandle.class, "columnHandle").getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            LocalFileTableHandle tableHandle = tables.get(tableName);
            if (tableHandle != null) {
                columns.put(tableName, getColumns(tableHandle));
            }
        }
        return columns.build();
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return emptyList();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyMap();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }
}
