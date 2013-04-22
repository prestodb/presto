package com.facebook.presto.metadata;

import java.util.List;
import java.util.Map;

public interface ConnectorMetadata
{
    int priority();

    boolean canHandle(TableHandle tableHandle);

    boolean canHandle(QualifiedTablePrefix prefix);

    List<String> listSchemaNames(String catalogName);

    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     */
    TableHandle getTableHandle(QualifiedTableName tableName);

    /**
     * Return the metadata for the specified table handle.
     * @throws RuntimeException if table handle is no longer valid
     */
    TableMetadata getTableMetadata(TableHandle table);

    /**
     * Get the names that match the specified table prefix (never null).
     */
    List<QualifiedTableName> listTables(QualifiedTablePrefix prefix);

    /**
     * Returns a handle for the specified table column, or null if the table does not contain the specified column.
     * @throws RuntimeException if table handle is no longer valid
     */
    ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName);

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     * @throws RuntimeException if table handle is no longer valid
     */
    Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle);

    /**
     * Gets the metadata for the specified table column.
     * @throws RuntimeException if table or column handles are no longer valid
     */
    ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle);

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     */
    Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(QualifiedTablePrefix prefix);

    List<Map<String, String>> listTablePartitionValues(QualifiedTablePrefix prefix);

    /**
     * Creates a table using the specified table metadata.
     */
    TableHandle createTable(TableMetadata tableMetadata);

    /**
     * Drops the specified table
     * @throws RuntimeException if the table can not be dropped or table handle is no longer valid
     */
    void dropTable(TableHandle tableHandle);
}
