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

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnIdentity;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableIdentity;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.tree.QualifiedName;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

public interface Metadata
{
    void verifyComparableOrderableContract();

    Type getType(TypeSignature signature);

    boolean isAggregationFunction(QualifiedName name);

    List<SqlFunction> listFunctions();

    void addFunctions(List<? extends SqlFunction> functions);

    boolean schemaExists(Session session, CatalogSchemaName schema);

    List<String> listSchemaNames(Session session, String catalogName);

    /**
     * Returns a table handle for the specified table name.
     */
    Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName);

    List<TableLayoutResult> getLayouts(Session session, TableHandle tableHandle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns);

    TableLayout getLayout(Session session, TableLayoutHandle handle);

    Optional<Object> getInfo(Session session, TableLayoutHandle handle);

    /**
     * Return the metadata for the specified table handle.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    TableMetadata getTableMetadata(Session session, TableHandle tableHandle);

    /**
     * Get the names that match the specified table prefix (never null).
     */
    List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix);

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle);

    /**
     * Gets the metadata for the specified table column.
     *
     * @throws RuntimeException if table or column handles are no longer valid
     */
    ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle);

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     */
    Map<QualifiedObjectName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix);

    /**
     * Creates a schema.
     */
    void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties);

    /**
     * Drops the specified schema.
     */
    void dropSchema(Session session, CatalogSchemaName schema);

    /**
     * Renames the specified schema.
     */
    void renameSchema(Session session, CatalogSchemaName source, String target);

    /**
     * Creates a table using the specified table metadata.
     */
    void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata);

    /**
     * Rename the specified table.
     */
    void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName);

    /**
     * Rename the specified column.
     */
    void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target);

    /**
     * Add the specified column to the table.
     */
    void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column);

    /**
     * Drops the specified table
     *
     * @throws RuntimeException if the table can not be dropped or table handle is no longer valid
     */
    void dropTable(Session session, TableHandle tableHandle);

    /**
     * Gets the TableIdentity for the specified table.
     */
    TableIdentity getTableIdentity(Session session, TableHandle tableHandle);

    /**
     * Deserialize the bytes to TableIdentity
     */
    TableIdentity deserializeTableIdentity(Session session, String catalogName, byte[] bytes);

    /**
     * Gets the ColumnIdentity for the specified column.
     */
    ColumnIdentity getColumnIdentity(Session session, TableHandle tableHandle, ColumnHandle columnHandle);

    /**
     * Deserialize the bytes to ColumnIdentity
     */
    ColumnIdentity deserializeColumnIdentity(Session session, String catalogName, byte[] bytes);

    Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata);

    /**
     * Begin the atomic creation of a table with data.
     */
    OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout);

    /**
     * Finish a table creation with data after the data is written.
     */
    Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments);

    Optional<NewTableLayout> getInsertLayout(Session session, TableHandle target);

    /**
     * Begin insert query
     */
    InsertTableHandle beginInsert(Session session, TableHandle tableHandle);

    /**
     * Finish insert query
     */
    Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments);

    /**
     * Get the row ID column handle used with UpdatablePageSource.
     */
    ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle);

    /**
     * @return whether delete without table scan is supported
     */
    boolean supportsMetadataDelete(Session session, TableHandle tableHandle, TableLayoutHandle tableLayoutHandle);

    /**
     * Delete the provide table layout
     *
     * @return number of rows deleted, or empty for unknown
     */
    OptionalLong metadataDelete(Session session, TableHandle tableHandle, TableLayoutHandle tableLayoutHandle);

    /**
     * Begin delete query
     */
    TableHandle beginDelete(Session session, TableHandle tableHandle);

    /**
     * Finish delete query
     */
    void finishDelete(Session session, TableHandle tableHandle, Collection<Slice> fragments);

    /**
     * Returns a connector id for the specified catalog name.
     */
    Optional<ConnectorId> getCatalogHandle(Session session, String catalogName);

    /**
     * Gets all the loaded catalogs
     *
     * @return Map of catalog name to connector id
     */
    Map<String, ConnectorId> getCatalogNames(Session session);

    /**
     * Get the names that match the specified table prefix (never null).
     */
    List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix);

    /**
     * Get the view definitions that match the specified table prefix (never null).
     */
    Map<QualifiedObjectName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix);

    /**
     * Returns the view definition for the specified view name.
     */
    Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName);

    /**
     * Creates the specified view with the specified view definition.
     */
    void createView(Session session, QualifiedObjectName viewName, String viewData, boolean replace);

    /**
     * Drops the specified view.
     */
    void dropView(Session session, QualifiedObjectName viewName);

    /**
     * Try to locate a table index that can lookup results by indexableColumns and provide the requested outputColumns.
     */
    Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain);

    /**
     * Grants the specified privilege to the specified user on the specified table
     */
    void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, String grantee, boolean grantOption);

    /**
     * Revokes the specified privilege on the specified table from the specified user
     */
    void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, String grantee, boolean grantOption);

    FunctionRegistry getFunctionRegistry();

    ProcedureRegistry getProcedureRegistry();

    TypeManager getTypeManager();

    BlockEncodingSerde getBlockEncodingSerde();

    SessionPropertyManager getSessionPropertyManager();

    SchemaPropertyManager getSchemaPropertyManager();

    TablePropertyManager getTablePropertyManager();
}
