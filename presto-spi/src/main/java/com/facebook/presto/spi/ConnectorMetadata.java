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

import com.facebook.presto.spi.security.Privilege;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

@Deprecated
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
     * <p>
     * For each layout, connectors must return an "unenforced constraint" representing the part of the constraint summary that isn't guaranteed by the layout.
     */
    default List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    default ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Return the metadata for the specified table handle.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table);

    /**
     * List table names, possibly filtered by schema. An empty list is returned if none match.
     */
    List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull);

    /**
     * Returns the handle for the sample weight column, or null if the table does not contain sampled data.
     *
     * @throws RuntimeException if the table handle is no longer valid
     */
    default ColumnHandle getSampleWeightColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return null;
    }

    /**
     * Returns true if this catalog supports creation of sampled tables
     */
    default boolean canCreateSampledTables(ConnectorSession session)
    {
        return false;
    }

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle);

    /**
     * Gets the metadata for the specified table column.
     *
     * @throws RuntimeException if table or column handles are no longer valid
     */
    ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle);

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     */
    Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix);

    /**
     * Creates a table using the specified table metadata.
     */
    default void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    /**
     * Drops the specified table
     *
     * @throws RuntimeException if the table can not be dropped or table handle is no longer valid
     */
    default void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping tables");
    }

    /**
     * Rename the specified table
     */
    default void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    /**
     * Add the specified column
     */
    default void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }

    /**
     * Rename the specified column
     */
    default void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    /**
     * Begin the atomic creation of a table with data.
     */
    default ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
    }

    /**
     * Commit a table creation with data after the data is written.
     */
    default void commitCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginCreateTable() is implemented without commitCreateTable()");
    }

    /**
     * Rollback a table creation
     */
    default void rollbackCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle) {}

    /**
     * Begin insert query
     */
    default ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support inserts");
    }

    /**
     * Commit insert query
     */
    default void commitInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginInsert() is implemented without commitInsert()");
    }

    /**
     * Rollback insert query
     */
    default void rollbackInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle) {}

    /**
     * Get the column handle that will generate row IDs for the delete operation.
     * These IDs will be passed to the {@code deleteRows()} method of the
     * {@link UpdatablePageSource} that created them.
     */
    default ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support updates or deletes");
    }

    /**
     * Begin delete query
     */
    default ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Commit delete query
     *
     * @param fragments all fragments returned by {@link com.facebook.presto.spi.UpdatablePageSource#finish()}
     */
    default void commitDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Rollback delete query
     */
    default void rollbackDelete(ConnectorSession session, ConnectorTableHandle tableHandle) {}

    /**
     * Create the specified view. The data for the view is opaque to the connector.
     */
    default void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating views");
    }

    /**
     * Drop the specified view.
     */
    default void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping views");
    }

    /**
     * List view names, possibly filtered by schema. An empty list is returned if none match.
     */
    default List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return emptyList();
    }

    /**
     * Gets the view data for views that match the specified table prefix.
     */
    default Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyMap();
    }

    /**
     * @return whether delete without table scan is supported
     */
    default boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Delete the provided table layout
     *
     * @return number of rows deleted, or null for unknown
     */
    default OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Grants the specified privilege to the specified user on the specified table
     */
    default void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, String grantee, boolean grantOption)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support grants");
    }

    /**
     * Revokes the specified privilege on the specified table from the specified user
     */
    default void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, String grantee, boolean grantOption)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support revokes");
    }
}
