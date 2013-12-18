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

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface ConnectorMetadata
{
    /**
     * Can this connector handler operations for the specified table handle.
     */
    boolean canHandle(TableHandle tableHandle);

    /**
     * Returns the schemas provided by this connector.
     */
    List<String> listSchemaNames();

    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     */
    TableHandle getTableHandle(SchemaTableName tableName);

    /**
     * Return the metadata for the specified table handle.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    ConnectorTableMetadata getTableMetadata(TableHandle table);

    /**
     * Get the names that match the specified table prefix (never null).
     */
    List<SchemaTableName> listTables(String schemaNameOrNull);

    /**
     * Returns a handle for the specified table column, or null if the table does not contain the specified column.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName);

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle);

    /**
     * Gets the metadata for the specified table column.
     *
     * @throws RuntimeException if table or column handles are no longer valid
     */
    ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle);

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     */
    Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix);

    /**
     * Creates a table using the specified table metadata.
     */
    TableHandle createTable(ConnectorTableMetadata tableMetadata);

    /**
     * Drops the specified table
     *
     * @throws RuntimeException if the table can not be dropped or table handle is no longer valid
     */
    void dropTable(TableHandle tableHandle);

    /**
     * Can this connector handler operations for the specified output table handle.
     */
    boolean canHandle(OutputTableHandle tableHandle);

    /**
     * Begin the atomic creation of a table with data.
     */
    OutputTableHandle beginCreateTable(ConnectorTableMetadata tableMetadata);

    /**
     * Commit a table creation with data after the data is written.
     */
    void commitCreateTable(OutputTableHandle tableHandle, Collection<String> fragments);
}
