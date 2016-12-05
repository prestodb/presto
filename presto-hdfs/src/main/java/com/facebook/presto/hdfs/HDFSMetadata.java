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
package com.facebook.presto.hdfs;

import com.facebook.presto.hdfs.metaserver.MetaServer;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
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
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.connector.ConnectorMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSMetadata
implements ConnectorMetadata
{
    private final String connectorId;
    private final MetaServer metaServer;

    public HDFSMetadata(String connectorId, MetaServer metaServer)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.metaServer = requireNonNull(metaServer, "metaServer is null");
    }

    /**
     * Returns the schemas provided by this connector.
     *
     * @param session
     */
    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metaServer.getAllDatabases();
    }

    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     *
     * @param session
     * @param tableName
     */
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<HDFSTableHandle> table = metaServer.getTable(tableName.getSchemaName(), tableName.getTableName());
        return table.get();
    }

    /**
     * Return a list of table layouts that satisfy the given constraint.
     * <p>
     * For each layout, connectors must return an "unenforced constraint" representing the part of the constraint summary that isn't guaranteed by the layout.
     *
     * @param session
     * @param table
     * @param constraint
     * @param desiredColumns
     */
    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        // get table name from ConnectorTableHandle
        // query tbl_params, and get fiber_col, timestamp_col and fiber_func
        // create HDFSTableLayoutHandle
        // ConnectorTableLayout layout = new ConnectorTableLayout(HDFSTableLayoutHandle)
        // return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()))
        return null;
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        // get table name from ConnectorTableHandle
        // query tbl_params, and get fiber_col, timestamp_col and fiber_func
        // create HDFSTableLayoutHandle
        // ConnectorTableLayout layout = new ConnectorTableLayout(HDFSTableLayoutHandle)
        return null;
    }

    /**
     * Return the metadata for the specified table handle.
     *
     * @param session
     * @param table
     * @throws RuntimeException if table handle is no longer valid
     */
    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        HDFSTableHandle tableHandle = (HDFSTableHandle) table;
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
        // new ConnectorTableMetadata(SchemaTableName tableName, List<ColumnMetadata> metadata)
        return null;
    }

    /**
     * List table names, possibly filtered by schema. An empty list is returned if none match.
     *
     * @param session
     * @param schemaNameOrNull
     */
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return null;
    }

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     *
     * @param session
     * @param tableHandle
     * @throws RuntimeException if table handle is no longer valid
     */
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return null;
    }

    /**
     * Gets the metadata for the specified table column.
     *
     * @param session
     * @param tableHandle
     * @param columnHandle
     * @throws RuntimeException if table or column handles are no longer valid
     */
    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        HDFSColumnHandle column = (HDFSColumnHandle) columnHandle;
        return new ColumnMetadata(column.getName(), column.getType(), column.getComment(), false);
    }

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     *
     * @param session
     * @param prefix
     */
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        Map<SchemaTableName, List<ColumnMetadata>> tableColumns = new HashMap<>();
        List<SchemaTableName> tableNames = metaServer.listTables(prefix);
        for (SchemaTableName table : tableNames) {
            List<ColumnMetadata> columnMetadatas = new ArrayList<>();
            HDFSTableHandle t = metaServer.getTable(table.getSchemaName(), table.getTableName()).get();
            List<HDFSColumnHandle> columns = t.getColumns();
            for (HDFSColumnHandle col : columns) {
                ColumnMetadata metadata = new ColumnMetadata(col.getName(), col.getType(), col.getComment(), false);
                columnMetadatas.add(metadata);
            }
            tableColumns.putIfAbsent(table, columnMetadatas);
        }
        return tableColumns;
    }

    /**
     * Creates a schema.
     *
     * @param session
     * @param schemaName
     * @param properties: contains comment, location and owner
     */
    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        HDFSDatabase database = new HDFSDatabase(schemaName,
                (String) properties.get("comment"),
                (String) properties.get("location"),
                (String) properties.get("owner"));
        metaServer.createDatabase(session, database);
    }

    /**
     * Drops the specified schema.
     *
     * @param session
     * @param schemaName
     * @throws PrestoException with {@code SCHEMA_NOT_EMPTY} if the schema is not empty
     */
    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        if (!metaServer.isDatabaseEmpty(session, schemaName)) {
            throw new PrestoException(StandardErrorCode.SCHEMA_NOT_EMPTY, "schema is not empty");
        }

        metaServer.dropDatabase(session, schemaName);
    }

    /**
     * Renames the specified schema.
     *
     * @param session
     * @param source
     * @param target
     */
    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        metaServer.renameDatabase(session, source, target);
    }

    /**
     * Creates a table using the specified table metadata.
     *
     * @param session
     * @param tableMetadata
     */
    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        String tableName = tableMetadata.getTable().getTableName();
        String schemaName = tableMetadata.getTable().getSchemaName();
        String comment = (String) tableMetadata.getProperties().get("comment");
        String location = (String) tableMetadata.getProperties().get("location");
        String owner = (String) tableMetadata.getProperties().get("owner");
        StorageFormat storageFormat = (StorageFormat) tableMetadata.getProperties().get("storageFormat");
        HDFSColumnHandle fiberCol = (HDFSColumnHandle) tableMetadata.getProperties().get("fiberCol");
        HDFSColumnHandle timeCol = (HDFSColumnHandle) tableMetadata.getProperties().get("timeCol");
        String fiberFunc = (String) tableMetadata.getProperties().get("fiberFunc");

        List<HDFSColumnHandle> columns = new ArrayList<>();
        for (ColumnMetadata colMetadata : tableMetadata.getColumns()) {
            HDFSColumnHandle column = new HDFSColumnHandle(colMetadata.getName(),
                    colMetadata.getType(),
                    colMetadata.getComment());
            columns.add(column);
        }

        HDFSTableHandle table = new HDFSTableHandle(connectorId,
                tableName,
                schemaName,
                comment,
                location,
                owner,
                storageFormat,
                columns,
                fiberCol,
                timeCol,
                fiberFunc);

        metaServer.createTable(session, table);
    }

    /**
     * Drops the specified table
     *
     * @param session
     * @param tableHandle
     * @throws RuntimeException if the table can not be dropped or table handle is no longer valid
     */
    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (tableHandle == null) {
            throw new RuntimeException("tableHandle is null");
        }
        HDFSTableHandle table = (HDFSTableHandle) tableHandle;
        metaServer.dropTable(session, table.getSchemaName(), table.getTableName());
    }

    /**
     * Rename the specified table
     *
     * @param session
     * @param tableHandle
     * @param newTableName
     * @throws RuntimeException if the table can not be renamed or table handle is no longer valid
     */
    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        if (tableHandle == null) {
            throw new RuntimeException("tableHandle is null");
        }
        HDFSTableHandle table = (HDFSTableHandle) tableHandle;
        metaServer.renameTable(session, table.getSchemaName(),
                table.getTableName(),
                newTableName.getSchemaName(),
                newTableName.getTableName());
    }

    /**
     * Drop the specified view.
     */
//    @Override
//    public void dropView(ConnectorSession session, SchemaTableName viewName)
//    {
//    }

    /**
     * List view names, possibly filtered by schema. An empty list is returned if none match.
     */
//    @Override
//    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
//    {
//        return emptyList();
//    }

    /**
     * Gets the view data for views that match the specified table prefix.
     */
//    @Override
//    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
//    {
//        return emptyMap();
//    }
}
