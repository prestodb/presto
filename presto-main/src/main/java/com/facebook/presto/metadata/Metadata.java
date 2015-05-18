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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.tree.QualifiedName;
import io.airlift.slice.Slice;

import javax.validation.constraints.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface Metadata
{
    Type getType(TypeSignature signature);

    FunctionInfo resolveFunction(QualifiedName name, List<TypeSignature> parameterTypes, boolean approximate);

    @NotNull
    FunctionInfo getExactFunction(Signature handle);

    boolean isAggregationFunction(QualifiedName name);

    @NotNull
    List<ParametricFunction> listFunctions();

    void addFunctions(List<? extends ParametricFunction> functions);

    FunctionInfo resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException;

    @NotNull
    List<String> listSchemaNames(Session session, String catalogName);

    /**
     * Returns a table handle for the specified table name.
     */
    @NotNull
    Optional<TableHandle> getTableHandle(Session session, QualifiedTableName tableName);

    @NotNull
    List<TableLayoutResult> getLayouts(TableHandle tableHandle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns);

    @NotNull
    TableLayout getLayout(TableLayoutHandle handle);

    /**
     * Return the metadata for the specified table handle.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    @NotNull
    TableMetadata getTableMetadata(TableHandle tableHandle);

    /**
     * Get the names that match the specified table prefix (never null).
     */
    @NotNull
    List<QualifiedTableName> listTables(Session session, QualifiedTablePrefix prefix);

    /**
     * Returns the handle for the sample weight column.
     *
     * @throws RuntimeException if the table handle is no longer valid
     */
    @NotNull
    Optional<ColumnHandle> getSampleWeightColumnHandle(TableHandle tableHandle);

    /**
     * Returns true iff this catalog supports creation of sampled tables
     *
     */
    boolean canCreateSampledTables(Session session, String catalogName);

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    @NotNull
    Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle);

    /**
     * Gets the metadata for the specified table column.
     *
     * @throws RuntimeException if table or column handles are no longer valid
     */
    @NotNull
    ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle);

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     */
    @NotNull
    Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix);

    /**
     * Creates a table using the specified table metadata.
     */
    @NotNull
    void createTable(Session session, String catalogName, TableMetadata tableMetadata);

    /**
     * Rename the specified table.
     */
    void renameTable(TableHandle tableHandle, QualifiedTableName newTableName);

    /**
     * Rename the specified column.
     */
    void renameColumn(TableHandle tableHandle, ColumnHandle source, String target);

    /**
     * Drops the specified table
     *
     * @throws RuntimeException if the table can not be dropped or table handle is no longer valid
     */
    void dropTable(TableHandle tableHandle);

    /**
     * Begin the atomic creation of a table with data.
     */
    OutputTableHandle beginCreateTable(Session session, String catalogName, TableMetadata tableMetadata);

    /**
     * Commit a table creation with data after the data is written.
     */
    void commitCreateTable(OutputTableHandle tableHandle, Collection<Slice> fragments);

    /**
     * Rollback a table creation
     */
    void rollbackCreateTable(OutputTableHandle tableHandle);

    /**
     * Begin insert query
     */
    InsertTableHandle beginInsert(Session session, TableHandle tableHandle);

    /**
     * Commit insert query
     */
    void commitInsert(InsertTableHandle tableHandle, Collection<Slice> fragments);

    /**
     * Rollback insert query
     */
    void rollbackInsert(InsertTableHandle tableHandle);

    /**
     * Gets all the loaded catalogs
     *
     * @return Map of catalog name to connector id
     */
    @NotNull
    Map<String, String> getCatalogNames();

    /**
     * Get the names that match the specified table prefix (never null).
     */
    @NotNull
    List<QualifiedTableName> listViews(Session session, QualifiedTablePrefix prefix);

    /**
     * Get the view definitions that match the specified table prefix (never null).
     */
    @NotNull
    Map<QualifiedTableName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix);

    /**
     * Returns the view definition for the specified view name.
     */
    @NotNull
    Optional<ViewDefinition> getView(Session session, QualifiedTableName viewName);

    /**
     * Creates the specified view with the specified view definition.
     */
    void createView(Session session, QualifiedTableName viewName, String viewData, boolean replace);

    /**
     * Drops the specified view.
     */
    void dropView(Session session, QualifiedTableName viewName);

    FunctionRegistry getFunctionRegistry();

    TypeManager getTypeManager();

    BlockEncodingSerde getBlockEncodingSerde();
}
