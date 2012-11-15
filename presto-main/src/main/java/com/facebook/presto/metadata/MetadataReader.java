package com.facebook.presto.metadata;

public interface MetadataReader
{
    TableMetadata getTable(String catalogName, String schemaName, String tableName);

    TableMetadata getTable(TableHandle tableHandle);

    ColumnMetadata getColumn(ColumnHandle columnHandle);
}
