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
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.localfile.LocalFileColumnHandle.SERVER_ADDRESS_COLUMN_NAME;
import static com.facebook.presto.localfile.LocalFileColumnHandle.SERVER_ADDRESS_ORDINAL_POSITION;
import static com.facebook.presto.localfile.Types.checkType;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class LocalFileMetadata
        implements ConnectorMetadata
{
    public static final String PRESTO_LOGS_SCHEMA = "logs";
    public static final ColumnMetadata SERVER_ADDRESS_COLUMN = new ColumnMetadata("server_address", VARCHAR);
    private static final List<String> SCHEMA_NAMES = ImmutableList.of(PRESTO_LOGS_SCHEMA);

    private final LocalFileTables localFileTables;

    @Inject
    public LocalFileMetadata(LocalFileTables localFileTables)
    {
        this.localFileTables = requireNonNull(localFileTables, "localFileTables is null");
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
        return localFileTables.getTable(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        LocalFileTableHandle tableHandle = checkType(table, LocalFileTableHandle.class, "tableHandle");
        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), localFileTables.getColumns(tableHandle));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return localFileTables.getTables();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        LocalFileTableHandle tableHandle = checkType(table, LocalFileTableHandle.class, "tableHandle");
        ConnectorTableLayout layout = new ConnectorTableLayout(new LocalFileTableLayoutHandle(tableHandle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        LocalFileTableLayoutHandle layout = checkType(handle, LocalFileTableLayoutHandle.class, "layout");
        return new ConnectorTableLayout(layout);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table)
    {
        LocalFileTableHandle tableHandle = checkType(table, LocalFileTableHandle.class, "tableHandle");
        return getColumnHandles(tableHandle);
    }

    private Map<String, ColumnHandle> getColumnHandles(LocalFileTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : localFileTables.getColumns(tableHandle)) {
            int ordinalPosition;
            if (column.getName().equals(SERVER_ADDRESS_COLUMN_NAME)) {
                ordinalPosition = SERVER_ADDRESS_ORDINAL_POSITION;
            }
            else {
                ordinalPosition = index;
                index++;
            }
            columnHandles.put(column.getName(), new LocalFileColumnHandle(column.getName(), column.getType(), ordinalPosition));
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, LocalFileTableHandle.class, "tableHandle");
        return checkType(columnHandle, LocalFileColumnHandle.class, "columnHandle").toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            LocalFileTableHandle tableHandle = localFileTables.getTable(tableName);
            if (tableHandle != null) {
                columns.put(tableName, localFileTables.getColumns(tableHandle));
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
