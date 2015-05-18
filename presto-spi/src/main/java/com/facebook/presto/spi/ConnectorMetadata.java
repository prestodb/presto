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
package com.facebook.presto.spi;

import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

public interface ConnectorMetadata
{
    /**
     * Returns the schemas provided by this connector.
     */
    List<String> listSchemaNames(ConnectorSession session);

    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     */
    ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName);

    /**
     * Return a list of table layouts that satisfy the given constraint.
     *
     * For each layout, connectors must return an "unenforced constraint" representing the part of the constraint summary that isn't guaranteed by the layout.
     */
    default List<ConnectorTableLayoutResult> getTableLayouts(ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    default ConnectorTableLayout getTableLayout(ConnectorTableLayoutHandle handle)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Return the metadata for the specified table handle.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    ConnectorTableMetadata getTableMetadata(ConnectorTableHandle table);

    /**
     * List table names, possibly filtered by schema. An empty list is returned if none match.
     */
    List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull);

    /**
     * Returns the handle for the sample weight column, or null if the table does not contain sampled data.
     *
     * @throws RuntimeException if the table handle is no longer valid
     */
    ColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle);

    /**
     * Returns true if this catalog supports creation of sampled tables
     */
    boolean canCreateSampledTables(ConnectorSession session);

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    Map<String, ColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle);

    /**
     * Gets the metadata for the specified table column.
     *
     * @throws RuntimeException if table or column handles are no longer valid
     */
    ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ColumnHandle columnHandle);

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     */
    Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix);

    /**
     * Creates a table using the specified table metadata.
     */
    void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    /**
     * Drops the specified table
     *
     * @throws RuntimeException if the table can not be dropped or table handle is no longer valid
     */
    void dropTable(ConnectorTableHandle tableHandle);

    /**
     * Rename the specified table
     */
    void renameTable(ConnectorTableHandle tableHandle, SchemaTableName newTableName);

    /**
     * Rename the specified column
     */
    default void renameColumn(ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    /**
     * Begin the atomic creation of a table with data.
     */
    ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    /**
     * Commit a table creation with data after the data is written.
     */
    void commitCreateTable(ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments);

    /**
     * Rollback a table creation
     */
    default void rollbackCreateTable(ConnectorOutputTableHandle tableHandle)
    {
    }

    /**
     * Begin insert query
     */
    ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle);

    /**
     * Commit insert query
     */
    void commitInsert(ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments);

    /**
     * Rollback insert query
     */
    default void rollbackInsert(ConnectorInsertTableHandle insertHandle)
    {
    }

    /**
     * Create the specified view. The data for the view is opaque to the connector.
     */
    void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace);

    /**
     * Drop the specified view.
     */
    void dropView(ConnectorSession session, SchemaTableName viewName);

    /**
     * List view names, possibly filtered by schema. An empty list is returned if none match.
     */
    List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull);

    /**
     * Gets the view data for views that match the specified table prefix.
     */
    Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix);
}
