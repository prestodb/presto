package com.facebook.presto.metadata;

public interface Metadata
{
    FunctionInfo getFunction(String name);

    TableMetadata getTable(String catalogName, String schemaName, String tableName);

    void createTable(TableMetadata table);
}
