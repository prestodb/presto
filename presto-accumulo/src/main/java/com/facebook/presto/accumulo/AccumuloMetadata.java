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
package com.facebook.presto.accumulo;

import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.metadata.AccumuloView;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.AccumuloTableHandle;
import com.facebook.presto.accumulo.model.AccumuloTableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
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
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_EXISTS;
import static com.facebook.presto.accumulo.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Presto metadata provider for Accumulo.
 * Responsible for creating/dropping/listing tables, schemas, columns, all sorts of goodness. Heavily leverages {@link AccumuloClient}.
 */
public class AccumuloMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final AccumuloClient client;
    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    @Inject
    public AccumuloMetadata(
            AccumuloConnectorId connectorId,
            AccumuloClient client)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        checkNoRollback();

        SchemaTableName tableName = tableMetadata.getTable();
        AccumuloTable table = client.createTable(tableMetadata);

        AccumuloTableHandle handle = new AccumuloTableHandle(
                connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.getRowId(),
                table.isExternal(),
                table.getSerializerClassName(),
                table.getScanAuthorizations());

        setRollback(() -> rollbackCreateTable(table));

        return handle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        checkType(tableHandle, AccumuloTableHandle.class, "tableHandle");
        clearRollback();
        return Optional.empty();
    }

    private void rollbackCreateTable(AccumuloTable table)
    {
        client.dropTable(table);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        client.createTable(tableMetadata);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        AccumuloTableHandle handle = checkType(tableHandle, AccumuloTableHandle.class, "table");
        AccumuloTable table = client.getTable(handle.toSchemaTableName());
        if (table != null) {
            client.dropTable(table);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle,
            SchemaTableName newTableName)
    {
        if (client.getTable(newTableName) != null) {
            throw new PrestoException(ACCUMULO_TABLE_EXISTS, "Table " + newTableName + " already exists");
        }

        AccumuloTableHandle handle = checkType(tableHandle, AccumuloTableHandle.class, "table");
        client.renameTable(handle.toSchemaTableName(), newTableName);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        if (replace) {
            client.createOrReplaceView(viewName, viewData);
        }
        else {
            client.createView(viewName, viewData);
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        client.dropView(viewName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> builder = ImmutableMap.builder();
        for (SchemaTableName stName : listViews(session, prefix.getSchemaName())) {
            AccumuloView view = client.getView(stName);
            if (view != null) {
                builder.put(stName, new ConnectorViewDefinition(stName, Optional.empty(), view.getData()));
            }
        }
        return builder.build();
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return listViews(schemaNameOrNull);
    }

    /**
     * Gets all views in the given schema, or all schemas if null.
     *
     * @param schemaNameOrNull Schema to list for the views, or null to list all schemas
     * @return List of views
     */
    private List<SchemaTableName> listViews(String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        if (schemaNameOrNull == null) {
            for (String schema : client.getSchemaNames()) {
                for (String view : client.getViewNames(schema)) {
                    builder.add(new SchemaTableName(schema, view));
                }
            }
        }
        else {
            for (String view : client.getViewNames(schemaNameOrNull)) {
                builder.add(new SchemaTableName(schemaNameOrNull, view));
            }
        }

        return builder.build();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkNoRollback();
        AccumuloTableHandle handle = checkType(tableHandle, AccumuloTableHandle.class, "table");
        setRollback(() -> rollbackInsert(handle));
        return handle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        clearRollback();
        return Optional.empty();
    }

    private static void rollbackInsert(ConnectorInsertTableHandle insertHandle)
    {
        // Rollbacks for inserts are off the table when it comes to data in Accumulo.
        // When a batch of Mutations fails to be inserted, the general strategy
        // is to run the insert operation again until it is successful
        // Any mutations that were successfully written will be overwritten
        // with the same values, so that isn't a problem.
        AccumuloTableHandle handle = checkType(insertHandle, AccumuloTableHandle.class, "table");
        throw new PrestoException(NOT_SUPPORTED, format("Unable to rollback insert for table %s.%s. Some rows may have been written. Please run your insert again.", handle.getSchema(), handle.getTable()));
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (!listSchemaNames(session).contains(tableName.getSchemaName().toLowerCase(Locale.ENGLISH))) {
            return null;
        }

        // Need to validate that SchemaTableName is a table
        if (!this.listViews(session, tableName.getSchemaName()).contains(tableName)) {
            AccumuloTable table = client.getTable(tableName);
            if (table == null) {
                return null;
            }

            return new AccumuloTableHandle(
                    connectorId,
                    table.getSchema(),
                    table.getTable(),
                    table.getRowId(),
                    table.isExternal(),
                    table.getSerializerClassName(),
                    table.getScanAuthorizations());
        }

        return null;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        AccumuloTableHandle tableHandle = checkType(table, AccumuloTableHandle.class, "table");
        ConnectorTableLayout layout = new ConnectorTableLayout(new AccumuloTableLayoutHandle(tableHandle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(checkType(handle, AccumuloTableLayoutHandle.class, "layout"));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        AccumuloTableHandle handle = checkType(table, AccumuloTableHandle.class, "table");
        checkArgument(handle.getConnectorId().equals(connectorId), "table is not for this connector");
        SchemaTableName tableName = new SchemaTableName(handle.getSchema(), handle.getTable());
        ConnectorTableMetadata metadata = getTableMetadata(tableName);
        if (metadata == null) {
            throw new TableNotFoundException(tableName);
        }
        return metadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        AccumuloTableHandle handle = checkType(tableHandle, AccumuloTableHandle.class, "tableHandle");
        checkArgument(handle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        AccumuloTable table = client.getTable(handle.toSchemaTableName());
        if (table == null) {
            throw new TableNotFoundException(handle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (AccumuloColumnHandle column : table.getColumns()) {
            columnHandles.put(column.getName(), column);
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, AccumuloTableHandle.class, "tableHandle");
        return checkType(columnHandle, AccumuloColumnHandle.class, "columnHandle").getColumnMetadata();
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        AccumuloTableHandle handle = checkType(tableHandle, AccumuloTableHandle.class, "handle");
        AccumuloColumnHandle columnHandle = checkType(source, AccumuloColumnHandle.class, "columnHandle");
        AccumuloTable table = client.getTable(handle.toSchemaTableName());
        if (table == null) {
            throw new TableNotFoundException(new SchemaTableName(handle.getSchema(), handle.getTable()));
        }

        client.renameColumn(table, columnHandle.getName(), target);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(client.getSchemaNames());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        Set<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        }
        else {
            schemaNames = client.getSchemaNames();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : client.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private void checkNoRollback()
    {
        checkState(rollbackAction.get() == null, "Cannot begin a new write while in an existing one");
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "Should not have to override existing rollback action");
    }

    private void clearRollback()
    {
        rollbackAction.set(null);
    }

    public void rollback()
    {
        Runnable rollbackAction = this.rollbackAction.getAndSet(null);
        if (rollbackAction != null) {
            rollbackAction.run();
        }
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!client.getSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

        // Need to validate that SchemaTableName is a table
        if (!this.listViews(tableName.getSchemaName()).contains(tableName)) {
            AccumuloTable table = client.getTable(tableName);
            if (table == null) {
                return null;
            }

            return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
        }

        return null;
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        // List all tables if schema or table is null
        if (prefix.getSchemaName() == null || prefix.getTableName() == null) {
            return listTables(session, prefix.getSchemaName());
        }

        // Make sure requested table exists, returning the single table of it does
        SchemaTableName table = new SchemaTableName(prefix.getSchemaName(), prefix.getTableName());
        if (getTableHandle(session, table) != null) {
            return ImmutableList.of(table);
        }

        // Else, return empty list
        return ImmutableList.of();
    }
}
