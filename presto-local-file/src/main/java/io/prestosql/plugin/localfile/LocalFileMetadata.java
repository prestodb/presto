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
package io.prestosql.plugin.localfile;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayout;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutResult;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.plugin.localfile.LocalFileColumnHandle.SERVER_ADDRESS_COLUMN_NAME;
import static io.prestosql.plugin.localfile.LocalFileColumnHandle.SERVER_ADDRESS_ORDINAL_POSITION;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class LocalFileMetadata
        implements ConnectorMetadata
{
    public static final String PRESTO_LOGS_SCHEMA = "logs";
    public static final ColumnMetadata SERVER_ADDRESS_COLUMN = new ColumnMetadata("server_address", createUnboundedVarcharType());
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
        LocalFileTableHandle tableHandle = (LocalFileTableHandle) table;
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
        LocalFileTableHandle tableHandle = (LocalFileTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new LocalFileTableLayoutHandle(tableHandle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        LocalFileTableLayoutHandle layout = (LocalFileTableLayoutHandle) handle;
        return new ConnectorTableLayout(layout);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table)
    {
        LocalFileTableHandle tableHandle = (LocalFileTableHandle) table;
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
        return ((LocalFileColumnHandle) columnHandle).toColumnMetadata();
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
