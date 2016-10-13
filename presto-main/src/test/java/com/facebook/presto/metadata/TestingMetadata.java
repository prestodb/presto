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
package com.facebook.presto.metadata;

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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TestingMetadata
        implements ConnectorMetadata
{
    private final ConcurrentMap<SchemaTableName, ConnectorTableMetadata> tables = new ConcurrentHashMap<>();
    private final ConcurrentMap<SchemaTableName, String> views = new ConcurrentHashMap<>();

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        Set<String> schemaNames = new HashSet<>();

        for (SchemaTableName schemaTableName : tables.keySet()) {
            schemaNames.add(schemaTableName.getSchemaName());
        }

        return ImmutableList.copyOf(schemaNames);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!tables.containsKey(tableName)) {
            return null;
        }
        return new InMemoryTableHandle(tableName);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        ConnectorTableMetadata tableMetadata = tables.get(tableName);
        checkArgument(tableMetadata != null, "Table %s does not exist", tableName);
        return tableMetadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata columnMetadata : getTableMetadata(session, tableHandle).getColumns()) {
            builder.put(columnMetadata.getName(), new InMemoryColumnHandle(columnMetadata.getName(), index, columnMetadata.getType()));
            index++;
        }
        return builder.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix.getSchemaName())) {
            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
            for (ColumnMetadata column : tables.get(tableName).getColumns()) {
                columns.add(new ColumnMetadata(column.getName(), column.getType()));
            }
            tableColumns.put(tableName, columns.build());
        }
        return tableColumns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        SchemaTableName tableName = getTableName(tableHandle);
        int columnIndex = checkType(columnHandle, InMemoryColumnHandle.class, "columnHandle").getOrdinalPosition();
        return tables.get(tableName).getColumns().get(columnIndex);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName tableName : tables.keySet()) {
            if (schemaNameOrNull == null || schemaNameOrNull.equals(tableName.getSchemaName())) {
                builder.add(tableName);
            }
        }
        return builder.build();
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        // TODO: use locking to do this properly
        ConnectorTableMetadata table = getTableMetadata(session, tableHandle);
        if (tables.putIfAbsent(newTableName, table) != null) {
            throw new IllegalArgumentException("Target table already exists: " + newTableName);
        }
        tables.remove(table.getTable(), table);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        ConnectorTableMetadata existingTable = tables.putIfAbsent(tableMetadata.getTable(), tableMetadata);
        checkArgument(existingTable == null, "Table %s already exists", tableMetadata.getTable());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        tables.remove(getTableName(tableHandle));
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        if (replace) {
            views.put(viewName, viewData);
        }
        else if (views.putIfAbsent(viewName, viewData) != null) {
            throw new PrestoException(ALREADY_EXISTS, "View already exists: " + viewName);
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        if (views.remove(viewName) == null) {
            throw new ViewNotFoundException(viewName);
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName viewName : views.keySet()) {
            if ((schemaNameOrNull == null) || schemaNameOrNull.equals(viewName.getSchemaName())) {
                builder.add(viewName);
            }
        }
        return builder.build();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> map = ImmutableMap.builder();
        for (Map.Entry<SchemaTableName, String> entry : views.entrySet()) {
            if (prefix.matches(entry.getKey())) {
                map.put(entry.getKey(), new ConnectorViewDefinition(entry.getKey(), Optional.empty(), entry.getValue()));
            }
        }
        return map.build();
    }

    private static SchemaTableName getTableName(ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof InMemoryTableHandle, "tableHandle is not an instance of InMemoryTableHandle");
        InMemoryTableHandle inMemoryTableHandle = (InMemoryTableHandle) tableHandle;
        return inMemoryTableHandle.getTableName();
    }

    public static class InMemoryTableHandle
            implements ConnectorTableHandle
    {
        private final SchemaTableName tableName;

        public InMemoryTableHandle(SchemaTableName schemaTableName)
        {
            this.tableName = schemaTableName;
        }

        public SchemaTableName getTableName()
        {
            return tableName;
        }
    }

    public static class InMemoryColumnHandle
            implements ColumnHandle
    {
        private final String name;
        private final int ordinalPosition;
        private final Type type;

        public InMemoryColumnHandle(String name, int ordinalPosition, Type type)
        {
            this.name = name;
            this.ordinalPosition = ordinalPosition;
            this.type = type;
        }

        public String getName()
        {
            return name;
        }

        public int getOrdinalPosition()
        {
            return ordinalPosition;
        }

        public Type getType()
        {
            return type;
        }
    }
}
