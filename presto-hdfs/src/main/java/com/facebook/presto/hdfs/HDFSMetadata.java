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
import com.google.inject.Inject;
import io.airlift.log.Logger;

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
    private final String connectorId;
    private final MetaServer metaServer;
    private final Logger logger = Logger.get(HDFSMetadata.class);

    @Inject
    public HDFSMetadata(MetaServer metaServer, HDFSConnectorId connectorId)
    {
        this.connectorId = requireNonNull(connectorId.toString(), "connectorId is null");
        this.metaServer = requireNonNull(metaServer, "metaServer is null");
    }

    /**
     * Returns the schemas provided by this connector.
     *
     * @param session session
     */
    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metaServer.getAllDatabases();
    }

    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     *
     * @param session session
     * @param tableName table name
     */
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<HDFSTableHandle> table = metaServer.getTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
        return table.orElse(null);
    }

    /**
     * Return a list of table layouts that satisfy the given constraint.
     * <p>
     * For each layout, connectors must return an "unenforced constraint" representing the part of the constraint summary that isn't guaranteed by the layout.
     *
     * @param session session
     * @param table table
     * @param constraint constraint
     * @param desiredColumns desired columns
     */
    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        // get table name from ConnectorTableHandle
        HDFSTableHandle hdfsTable = checkType(table, HDFSTableHandle.class, "table");
        SchemaTableName tableName = hdfsTable.getSchemaTableName();
        // create HDFSTableLayoutHandle
        HDFSTableLayoutHandle tableLayout = metaServer.getTableLayout(connectorId, tableName.getSchemaName(), tableName.getTableName()).orElse(null);
        tableLayout.setPredicates(constraint.getSummary() != null ? Optional.of(constraint.getSummary()) : Optional.empty());
        // ConnectorTableLayout layout = new ConnectorTableLayout(HDFSTableLayoutHandle)
        ConnectorTableLayout layout = getTableLayout(session, tableLayout);

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        // TODO add fiber and timestamp as new LocalProperty into ConnectorTableLayout ?
        HDFSTableLayoutHandle layoutHandle = checkType(handle, HDFSTableLayoutHandle.class, "tableLayoutHandle");
        return new ConnectorTableLayout(layoutHandle);
    }

    /**
     * Return the metadata for the specified table handle.
     *
     * @param session session
     * @param table table
     * @throws RuntimeException if table handle is no longer valid
     */
    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        HDFSTableHandle hdfsTable = checkType(table, HDFSTableHandle.class, "table");
        SchemaTableName tableName = hdfsTable.getSchemaTableName();
        return getTableMetadata(tableName);
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        List<ColumnMetadata> columns = metaServer.getTableColMetadata(connectorId, tableName.getSchemaName(),
                tableName.getTableName()).orElse(new ArrayList<>());
        return new ConnectorTableMetadata(tableName, columns);
    }

    /**
     * List table names, possibly filtered by schema. An empty list is returned if none match.
     *
     * @param session session
     * @param schemaNameOrNull schema name
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
     * @param session session
     * @param tableHandle table handle
     * @throws RuntimeException if table handle is no longer valid
     */
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HDFSTableHandle table = checkType(tableHandle, HDFSTableHandle.class, "table");
        List<HDFSColumnHandle> cols = metaServer.getTableColumnHandle(connectorId, table.getSchemaName(), table.getTableName())
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
     * @param session session
     * @param tableHandle table handle
     * @param columnHandle column handle
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
     * @param session session
     * @param prefix prefix
     */
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        Map<SchemaTableName, List<ColumnMetadata>> tableColumns = new HashMap<>();
        List<SchemaTableName> tableNames = metaServer.listTables(prefix);
        for (SchemaTableName table : tableNames) {
            List<ColumnMetadata> columnMetadatas = metaServer.getTableColMetadata(connectorId, table.getSchemaName(),
                    table.getTableName()).orElse(new ArrayList<>());
            tableColumns.putIfAbsent(table, columnMetadatas);
        }
        return tableColumns;
    }

    /**
     * Creates a schema.
     *
     * @param session session
     * @param schemaName schema name
     * @param properties: contains comment, location and owner
     */
    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        HDFSDatabase database = new HDFSDatabase(schemaName);
        metaServer.createDatabase(session, database);
    }

    /**
     * Creates a table using the specified table metadata.
     *
     * @param session sesion
     * @param tableMetadata table metadata
     */
    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        metaServer.createTable(session, tableMetadata);
    }

    /**
     * Creates a table with fiber
     * */
    @Override
    public void createTableWithFiber(ConnectorSession session, ConnectorTableMetadata tableMetadata, String fiberKey, String function, String timeKey)
    {
        metaServer.createTableWithFiber(session, tableMetadata, fiberKey, function, timeKey);
    }
}
