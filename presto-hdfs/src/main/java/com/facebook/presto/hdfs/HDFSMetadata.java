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
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hdfs.Types.checkType;
import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSMetadata
implements ConnectorMetadata
{
//    private final String connectorId;
    private final MetaServer metaServer;

    public HDFSMetadata(MetaServer metaServer)
    {
//        this.connectorId = requireNonNull(connectorId, "connectorId is null");
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
        Optional<HDFSTableHandle> table = metaServer.getTableHandle(tableName.getSchemaName(), tableName.getTableName());
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
        // TODO how to deal with desired columns?
        // get table name from ConnectorTableHandle
        HDFSTableHandle hdfsTable = checkType(table, HDFSTableHandle.class, "table");
        SchemaTableName tableName = hdfsTable.getSchemaTableName();
        // create HDFSTableLayoutHandle
        HDFSTableLayoutHandle tableLayout = metaServer.getTableLayout(tableName.getSchemaName(), tableName.getTableName()).get();
        // ConnectorTableLayout layout = new ConnectorTableLayout(HDFSTableLayoutHandle)
        ConnectorTableLayout layout = getTableLayout(session, tableLayout);

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        // TODO add fiber and timestamp as new LocalProperty into ConnectorTableLayout ?
        return new ConnectorTableLayout(handle);
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
        HDFSTableHandle hdfsTable = checkType(table, HDFSTableHandle.class, "table");
        SchemaTableName tableName = hdfsTable.getSchemaTableName();
        return getTableMetadata(tableName);
    }

    public ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        List<ColumnMetadata> columns = metaServer.getTableColMetadata(tableName.getSchemaName(),
                tableName.getTableName())
                .get();
        return new ConnectorTableMetadata(tableName, columns);
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
        if (schemaNameOrNull == null) {
            return new ArrayList<>();
        }
        return metaServer.listTables(new SchemaTablePrefix(schemaNameOrNull));
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
        HDFSTableHandle table = checkType(tableHandle, HDFSTableHandle.class, "table");
        List<HDFSColumnHandle> cols = metaServer.getTableColumnHandle(table.getSchemaName(), table.getTableName())
                .orElse(new ArrayList<>());
        Map<String, ColumnHandle> columnMap = new HashMap<>();
        for (HDFSColumnHandle col : cols) {
            columnMap.putIfAbsent(col.getName(), col);
        }
        return columnMap;
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
            List<ColumnMetadata> columnMetadatas = metaServer.getTableColMetadata(table.getSchemaName(),
                    table.getTableName())
                    .get();
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
//    @Override
//    public void dropSchema(ConnectorSession session, String schemaName)
//    {
//        if (!metaServer.isDatabaseEmpty(session, schemaName)) {
//            throw new PrestoException(StandardErrorCode.SCHEMA_NOT_EMPTY, "schema is not empty");
//        }
//
//        metaServer.dropDatabase(session, schemaName);
//    }

    /**
     * Renames the specified schema.
     *
     * @param session
     * @param source
     * @param target
     */
//    @Override
//    public void renameSchema(ConnectorSession session, String source, String target)
//    {
//        metaServer.renameDatabase(session, source, target);
//    }

    /**
     * Creates a table using the specified table metadata.
     *
     * @param session
     * @param tableMetadata
     */
    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
//        String tableName = tableMetadata.getTable().getTableName();
//        String schemaName = tableMetadata.getTable().getSchemaName();
//        String location = (String) tableMetadata.getProperties().get("location");
//        HDFSColumnHandle fiberCol = (HDFSColumnHandle) tableMetadata.getProperties().get("fiberCol");
//        HDFSColumnHandle timeCol = (HDFSColumnHandle) tableMetadata.getProperties().get("timeCol");
//        String fiberFunc = (String) tableMetadata.getProperties().get("fiberFunc");
//
//        List<HDFSColumnHandle> columns = new ArrayList<>();
//        for (ColumnMetadata colMetadata : tableMetadata.getColumns()) {
//            HDFSColumnHandle column = new HDFSColumnHandle(colMetadata.getName(),
//                    colMetadata.getType(),
//                    colMetadata.getComment());
//            columns.add(column);
//        }
//
//        HDFSTableHandle table = new HDFSTableHandle(connectorId,
//                tableName,
//                schemaName,
//                comment,
//                location,
//                owner,
//                storageFormat,
//                columns,
//                fiberCol,
//                timeCol,
//                fiberFunc);

        metaServer.createTable(session, tableMetadata);
    }

    /**
     * Drops the specified table
     *
     * @param session
     * @param tableHandle
     * @throws RuntimeException if the table can not be dropped or table handle is no longer valid
     */
//    @Override
//    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
//    {
//        if (tableHandle == null) {
//            throw new RuntimeException("tableHandle is null");
//        }
//        HDFSTableHandle table = (HDFSTableHandle) tableHandle;
//        metaServer.dropTable(session, table.getSchemaName(), table.getTableName());
//    }

    /**
     * Rename the specified table
     *
     * @param session
     * @param tableHandle
     * @param newTableName
     * @throws RuntimeException if the table can not be renamed or table handle is no longer valid
     */
//    @Override
//    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
//    {
//        if (tableHandle == null) {
//            throw new RuntimeException("tableHandle is null");
//        }
//        HDFSTableHandle table = (HDFSTableHandle) tableHandle;
//        metaServer.renameTable(session, table.getSchemaName(),
//                table.getTableName(),
//                newTableName.getSchemaName(),
//                newTableName.getTableName());
//    }

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
