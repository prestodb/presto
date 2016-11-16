package com.facebook.presto.hdfs;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSMetadata
implements ConnectorMetadata {
    /**
     * Returns the schemas provided by this connector.
     *
     * @param session
     */
    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return null;
    }

    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     *
     * @param session
     * @param tableName
     */
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        return null;
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
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
        return null;
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
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
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        return null;
    }

    /**
     * List table names, possibly filtered by schema. An empty list is returned if none match.
     *
     * @param session
     * @param schemaNameOrNull
     */
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
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
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
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
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return null;
    }

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     *
     * @param session
     * @param prefix
     */
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        return null;
    }

    /**
     * Creates a schema.
     *
     * @param session
     * @param schemaName
     * @param properties
     */
    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties) {

    }

    /**
     * Drops the specified schema.
     *
     * @param session
     * @param schemaName
     * @throws PrestoException with {@code SCHEMA_NOT_EMPTY} if the schema is not empty
     */
    @Override
    public void dropSchema(ConnectorSession session, String schemaName) {

    }

    /**
     * Renames the specified schema.
     *
     * @param session
     * @param source
     * @param target
     */
    @Override
    public void renameSchema(ConnectorSession session, String source, String target) {

    }

    /**
     * Creates a table using the specified table metadata.
     *
     * @param session
     * @param tableMetadata
     */
    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata) {

    }

    /**
     * Drops the specified table
     *
     * @param session
     * @param tableHandle
     * @throws RuntimeException if the table can not be dropped or table handle is no longer valid
     */
    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {

    }

    /**
     * Rename the specified table
     *
     * @param session
     * @param tableHandle
     * @param newTableName
     */
    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName) {

    }

    /**
     * Drop the specified view.
     */
    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
    }

    /**
     * List view names, possibly filtered by schema. An empty list is returned if none match.
     */
    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return emptyList();
    }

    /**
     * Gets the view data for views that match the specified table prefix.
     */
    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyMap();
    }
}
