/*
 * Copyright 2016 Bloomberg L.P.
 *
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
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.AccumuloTableHandle;
import com.facebook.presto.accumulo.model.AccumuloTableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_EXISTS;
import static com.facebook.presto.accumulo.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Presto metadata provider for Accumulo. Responsible for creating/dropping/listing tables, schemas,
 * columns, all sorts of goodness. Heavily leverages {@link AccumuloClient}
 */
public class AccumuloMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final AccumuloClient client;

    @Inject
    public AccumuloMetadata(AccumuloConnectorId connectorId, AccumuloClient client)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.client = requireNonNull(client, "client is null");
    }

    /**
     * Begins the process of creating a table with the given metadata. This process of
     * begin/commit/rollback is used for CTAS statements.
     *
     * @param session Current client session
     * @param tableMetadata Table metadata to create
     * @return Output table handle
     */
    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session,
            ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName stName = tableMetadata.getTable();
        AccumuloTable table = client.createTable(tableMetadata);
        return new AccumuloTableHandle(connectorId, stName.getSchemaName(), stName.getTableName(),
                table.getRowId(), table.isExternal(), table.getSerializerClassName(),
                table.getScanAuthorizations());
    }

    /**
     * Commit the table creation.
     *
     * @param session Current client session
     * @param tableHandle Output table handle
     * @param fragments Collection of fragment metadata
     */
    @Override
    public void commitCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments)
    {
        // TODO no-op?
        // The table and metadata is actually created in beginCreateTable
        // There may be some additional useful information regarding the fragments, but I don't
        // think that really matters for this connector
    }

    /**
     * Rollback the table creation
     *
     * @param session Current client session
     * @param tableHandle Table handle to delete
     */
    @Override
    public void rollbackCreateTable(ConnectorSession session,
            ConnectorOutputTableHandle tableHandle)
    {
        // Here, we just call drop table to rollback our create
        // Note that this will not delete the Accumulo tables if the table is external
        AccumuloTableHandle th = checkType(tableHandle, AccumuloTableHandle.class, "table");
        client.dropTable(client.getTable(th.toSchemaTableName()));
    }

    /**
     * Create the table metadata
     *
     * @param session Current client session
     * @param tableMetadata Table metadata to create
     */
    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        client.createTable(tableMetadata);
    }

    /**
     * Create the table metadata
     *
     * @param session Current client session
     * @param tableHandle Table metadata to drop
     */
    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        AccumuloTableHandle th = checkType(tableHandle, AccumuloTableHandle.class, "table");
        client.dropTable(client.getTable(th.toSchemaTableName()));
    }

    /**
     * Renames the given table to the new table name
     *
     * @param session Current client session
     * @param tableHandle Table metadata to rename
     * @param newTableName New table name!
     */
    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle,
            SchemaTableName newTableName)
    {
        if (client.getTable(newTableName) != null) {
            throw new PrestoException(ACCUMULO_TABLE_EXISTS,
                    "Table " + newTableName + " already exists");
        }

        AccumuloTableHandle th = checkType(tableHandle, AccumuloTableHandle.class, "table");
        client.renameTable(th.toSchemaTableName(), newTableName);
    }

    /**
     * Create the view metadata
     *
     * @param session Current client session
     * @param viewName Name of the view to create
     * @param viewData Data of the view
     * @param replace True to replace any existing view, false otherwise
     */
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

    /**
     * Drop the view metadata
     *
     * @param session Current client session
     * @param viewName Name of the view to create
     */
    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        client.dropView(viewName);
    }

    /**
     * Gets all views that match the given prefix
     *
     * @param session Current client session
     * @param prefix View prefix
     * @return Map of view names to the definition
     */
    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> bldr = ImmutableMap.builder();
        for (SchemaTableName stName : listViews(session, prefix.getSchemaName())) {
            bldr.put(stName, new ConnectorViewDefinition(stName, Optional.empty(), client.getView(stName).getData()));
        }
        return bldr.build();
    }

    /**
     * Gets all views in the given schema, or all schemas if null
     *
     * @param session Current client session
     * @param schemaNameOrNull Schema to list for the views, or null to list all schemas
     * @return List of views
     */
    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return listViews(schemaNameOrNull);
    }

    /**
     * Gets all views in the given schema, or all schemas if null
     *
     * @param schemaNameOrNull Schema to list for the views, or null to list all schemas
     * @return List of views
     */
    private List<SchemaTableName> listViews(String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> bldr = ImmutableList.builder();

        if (schemaNameOrNull == null) {
            for (String schema : client.getSchemaNames()) {
                for (String view : client.getViewNames(schema)) {
                    bldr.add(new SchemaTableName(schema, view));
                }
            }
        }
        else {
            for (String view : client.getViewNames(schemaNameOrNull)) {
                bldr.add(new SchemaTableName(schemaNameOrNull, view));
            }
        }

        return bldr.build();
    }

    /**
     * Begin an insert of data into an Accumulo table. This is for new inserts, not for a CTAS.
     *
     * @param session Current client session
     * @param tableHandle Table handle for the insert
     * @return Insert handle for the table
     */
    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        // This is all metadata management for the insert op
        // Not much to do here but validate the type and return the new table handle (which is the
        // same)
        return checkType(tableHandle, AccumuloTableHandle.class, "table");
    }

    /**
     * Commit the insert
     *
     * @param session Current client session
     * @param insertHandle Table handle
     * @param fragments Collection of fragment metadata
     */
    @Override
    public void commitInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments)
    {
        // Seems like most connectors just return metadata about the fragments
        // Unsure if we can use this for an optimization?
        // Priority is on reading though, so we can investigate this later
        checkType(insertHandle, AccumuloTableHandle.class, "table");
    }

    /**
     * Gets an instance of a TableHandle based on the given name
     *
     * @param session Current client session
     * @param stName Table name
     * @return Table handle or null if the table does not exist
     */
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName stName)
    {
        if (!listSchemaNames(session).contains(stName.getSchemaName().toLowerCase())) {
            return null;
        }

        // Need to validate that SchemaTableName is a table
        if (!this.listViews(session, stName.getSchemaName()).contains(stName)) {
            AccumuloTable table = client.getTable(stName);
            if (table == null) {
                return null;
            }

            return new AccumuloTableHandle(connectorId, table.getSchema(), table.getTable(),
                    table.getRowId(), table.isExternal(), table.getSerializerClassName(),
                    table.getScanAuthorizations());
        }

        return null;
    }

    /**
     * Gets all table layouts of the given table with the given constraints
     *
     * @param session Current client session
     * @param table Table handle
     * @param constraint Column constraints of the query
     * @param desiredColumns Column handles requested in the query
     * @return List of table layouts
     */
    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
            ConnectorTableHandle table, Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        AccumuloTableHandle tableHandle = checkType(table, AccumuloTableHandle.class, "table");
        ConnectorTableLayout layout = new ConnectorTableLayout(
                new AccumuloTableLayoutHandle(tableHandle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    /**
     * Gets a table layout from the given handle
     *
     * @param session Current client session
     * @param handle Table handle
     * @return Table layout
     */
    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session,
            ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(
                checkType(handle, AccumuloTableLayoutHandle.class, "layout"));
    }

    /**
     * Gets table metadata for the given handle
     *
     * @param session Current client session
     * @param table Table handle
     * @return Metadata for the given table handle
     */
    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
            ConnectorTableHandle table)
    {
        AccumuloTableHandle tHandle = checkType(table, AccumuloTableHandle.class, "table");
        checkArgument(tHandle.getConnectorId().equals(connectorId),
                "table is not for this connector");
        return getTableMetadata(new SchemaTableName(tHandle.getSchema(), tHandle.getTable()));
    }

    /**
     * Gets all available column handles for the requested table
     *
     * @param session client session
     * @param tableHandle Table handle
     * @return Mapping of Presto column name to column handle
     */
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        AccumuloTableHandle tHandle =
                checkType(tableHandle, AccumuloTableHandle.class, "tableHandle");
        checkArgument(tHandle.getConnectorId().equals(connectorId),
                "tableHandle is not for this connector");

        AccumuloTable table = client.getTable(tHandle.toSchemaTableName());
        if (table == null) {
            throw new TableNotFoundException(tHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (AccumuloColumnHandle column : table.getColumns()) {
            columnHandles.put(column.getName(), column);
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
            ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, AccumuloTableHandle.class, "tableHandle");
        return checkType(columnHandle, AccumuloColumnHandle.class, "columnHandle")
                .getColumnMetadata();
    }

    /**
     * Renames the given column of the given table to a new column name.
     * <p>
     * Note that this operation is an alias-only change. The Presto column mapping is unchanged,
     * i.e. the Accumulo column family and qualifier are not updated, so the queried data will
     * remain the same.
     *
     * @param session client session
     * @param tableHandle Table handle
     * @param source Column handle to rename
     * @param target New column name
     */
    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle,
            ColumnHandle source, String target)
    {
        AccumuloTableHandle tHandle =
                checkType(tableHandle, AccumuloTableHandle.class, "tableHandle");
        AccumuloColumnHandle cHandle =
                checkType(source, AccumuloColumnHandle.class, "columnHandle");
        AccumuloTable table = client.getTable(tHandle.toSchemaTableName());
        client.renameColumn(table, cHandle.getName(), target);
    }

    /**
     * List all existing schemas
     *
     * @param session Current client session
     * @see AccumuloClient#getSchemaNames
     */
    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(client.getSchemaNames());
    }

    /**
     * List all tables for a given schema, or null for all schemas
     *
     * @param session Current client session
     * @param schemaNameOrNull Schema name to list tables for, or null if all tables
     */
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

    /**
     * Lists all of the columns for each provided schema table
     *
     * @param session Current client session
     * @param prefix Table prefix
     * @return Mapping of table names with the given prefix to columns
     */
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns =
                ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    /**
     * Gets metadata for the given table
     *
     * @param stn Schema and table name
     * @return Table metadata, or null if schema/table does not exist
     */
    private ConnectorTableMetadata getTableMetadata(SchemaTableName stn)
    {
        if (!client.getSchemaNames().contains(stn.getSchemaName())) {
            return null;
        }

        // Need to validate that SchemaTableName is a table
        if (!this.listViews(stn.getSchemaName()).contains(stn)) {
            AccumuloTable table = client.getTable(stn);
            if (table == null) {
                return null;
            }

            return new ConnectorTableMetadata(stn, table.getColumnsMetadata());
        }

        return null;
    }

    /**
     * List all tables with the given prefix
     *
     * @param session Current client session
     * @param prefix Table prefix
     * @return List of table names with the given schema and/or table name, which could be one table
     */
    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        // List all tables if schema is null
        if (prefix.getSchemaName() == null) {
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
