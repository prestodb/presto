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
package com.facebook.presto.spi.connector;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnIdentity;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorResolvedIndex;
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
import com.facebook.presto.spi.TableIdentity;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.security.Privilege;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

public interface ConnectorMetadata
{
    /**
     * Checks if a schema exists. The connector may have schemas that exist
     * but are not enumerable via {@link #listSchemaNames}.
     */
    default boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return listSchemaNames(session).contains(schemaName);
    }

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
    List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns);

    ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle);

    /**
     * Return the metadata for the specified table handle.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table);

    /**
     * Return the connector-specific metadata for the specified table layout. This is the object that is passed to the event listener framework.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    default Optional<Object> getInfo(ConnectorTableLayoutHandle layoutHandle)
    {
        return Optional.empty();
    }

    /**
     * List table names, possibly filtered by schema. An empty list is returned if none match.
     */
    List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull);

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
     * Creates a schema.
     */
    default void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    /**
     * Drops the specified schema.
     *
     * @throws PrestoException with {@code SCHEMA_NOT_EMPTY} if the schema is not empty
     */
    default void dropSchema(ConnectorSession session, String schemaName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    /**
     * Renames the specified schema.
     */
    default void renameSchema(ConnectorSession session, String source, String target)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

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
     * Get the physical layout for a new table.
     */
    default Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return Optional.empty();
    }

    /**
     * Get the physical layout for a inserting into an existing table.
     */
    default Optional<ConnectorNewTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        List<ConnectorTableLayout> layouts = getTableLayouts(session, tableHandle, new Constraint<>(TupleDomain.all(), map -> true), Optional.empty())
                .stream()
                .map(ConnectorTableLayoutResult::getTableLayout)
                .filter(layout -> layout.getNodePartitioning().isPresent())
                .collect(toList());

        if (layouts.isEmpty()) {
            return Optional.empty();
        }

        if (layouts.size() > 1) {
            throw new PrestoException(NOT_SUPPORTED, "Tables with multiple layouts can not be written");
        }

        ConnectorTableLayout layout = layouts.get(0);
        ConnectorPartitioningHandle partitioningHandle = layout.getNodePartitioning().get().getPartitioningHandle();
        Map<ColumnHandle, String> columnNamesByHandle = getColumnHandles(session, tableHandle).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        List<String> partitionColumns = layout.getNodePartitioning().get().getPartitioningColumns().stream()
                .map(columnNamesByHandle::get)
                .collect(toList());

        return Optional.of(new ConnectorNewTableLayout(partitioningHandle, partitionColumns));
    }

    /**
     * Begin the atomic creation of a table with data.
     */
    default ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
    }

    /**
     * Finish a table creation with data after the data is written.
     */
    default void finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginCreateTable() is implemented without finishCreateTable()");
    }

    /**
     * Begin insert query
     */
    default ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support inserts");
    }

    /**
     * Finish insert query
     */
    default void finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginInsert() is implemented without finishInsert()");
    }

    /**
     * Get the column handle that will generate row IDs for the delete operation.
     * These IDs will be passed to the {@code deleteRows()} method of the
     * {@link com.facebook.presto.spi.UpdatablePageSource} that created them.
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
     * Finish delete query
     *
     * @param fragments all fragments returned by {@link com.facebook.presto.spi.UpdatablePageSource#finish()}
     */
    default void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

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
     * Try to locate a table index that can lookup results by indexableColumns and provide the requested outputColumns.
     */
    default Optional<ConnectorResolvedIndex> resolveIndex(ConnectorSession session, ConnectorTableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        return Optional.empty();
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

    /**
     * Gets the table identity on the specified table
     */
    default TableIdentity getTableIdentity(ConnectorTableHandle connectorTableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support table identity");
    }

    /**
     * Deserialize the specified bytes to TableIdentity
     */
    default TableIdentity deserializeTableIdentity(byte[] bytes)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support table identity");
    }

    /**
     * Gets the column identity on the specified column
     */
    default ColumnIdentity getColumnIdentity(ColumnHandle columnHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support column identity");
    }

    /**
     * Deserialize the specified bytes to ColumnIdentity
     */
    default ColumnIdentity deserializeColumnIdentity(byte[] bytes)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support column identity");
    }
}
