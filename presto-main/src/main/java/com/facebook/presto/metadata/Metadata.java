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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.OutputTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Optional;

import javax.validation.constraints.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface Metadata
{
    FunctionInfo getFunction(QualifiedName name, List<Type> parameterTypes);

    @NotNull
    FunctionInfo getFunction(FunctionHandle handle);

    boolean isAggregationFunction(QualifiedName name);

    @NotNull
    List<FunctionInfo> listFunctions();

    @NotNull
    List<String> listSchemaNames(String catalogName);

    /**
     * Returns a table handle for the specified table name.
     */
    @NotNull
    Optional<TableHandle> getTableHandle(QualifiedTableName tableName);

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
    List<QualifiedTableName> listTables(QualifiedTablePrefix prefix);

    /**
     * Returns a handle for the specified table column.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    @NotNull
    Optional<ColumnHandle> getColumnHandle(TableHandle tableHandle, String columnName);

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
    Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(QualifiedTablePrefix prefix);

    /**
     * Creates a table using the specified table metadata.
     */
    @NotNull
    TableHandle createTable(String catalogName, TableMetadata tableMetadata);

    /**
     * Drops the specified table
     *
     * @throws RuntimeException if the table can not be dropped or table handle is no longer valid
     */
    void dropTable(TableHandle tableHandle);

    /**
     * Begin the atomic creation of a table with data.
     */
    OutputTableHandle beginCreateTable(String catalogName, TableMetadata tableMetadata);

    /**
     * Commit a table creation with data after the data is written.
     */
    void commitCreateTable(OutputTableHandle tableHandle, Collection<String> fragments);

    /**
     * HACK: This is here only for table alias support and should be remove when aliases are based on serialized table handles.
     */
    @NotNull
    @Deprecated
    String getConnectorId(TableHandle tableHandle);

    /**
     * HACK: This is here only for table alias support and should be remove when aliases are based on serialized table handles.
     */
    @NotNull
    @Deprecated
    Optional<TableHandle> getTableHandle(String connectorId, SchemaTableName tableName);

    /**
     * Gets all the loaded catalogs
     * @return Map of catalog name to connector id
     */
    @NotNull
    Map<String, String> getCatalogNames();
}
