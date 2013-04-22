package com.facebook.presto.metadata;

import com.google.common.base.Optional;

import java.util.List;
import java.util.Map;

public interface InternalSchemaMetadata
{
    /**
     * Returns a table handle for the specified table name.
     */
    Optional<TableHandle> getTableHandle(QualifiedTableName table);

    /**
     * Return the metadata for the specified table handle, or absent value if table is not part of this schema.
     * @throws RuntimeException if table handle is no longer valid
     */
    Optional<TableMetadata> getTableMetadata(TableHandle tableHandle);

    /**
     * Get the names that match the specified table prefix (never null).
     */
    List<QualifiedTableName> listTables(QualifiedTablePrefix prefix);

    /**
     * Gets all of the columns on the specified table, or absent value if table is not part of this schema.
     * @throws RuntimeException if table handle is no longer valid
     */
    Optional<Map<String,ColumnHandle>> getColumnHandles(TableHandle tableHandle);

    /**
     * Gets the metadata for the specified table column, or absent value if table is not part of this schema.
     * @throws RuntimeException if table or column handles are no longer valid
     */
    Optional<ColumnMetadata> getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle);

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     */
   Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(QualifiedTablePrefix prefix);
}
