package com.facebook.presto.metadata;

import static com.google.common.base.Preconditions.checkNotNull;

public class TableNamespace
{
    private final String catalogName;
    private final String databaseName;
    private final String tableName;

    public TableNamespace(String catalogName, String databaseName, String tableName)
    {
        checkNotNull(catalogName, "catalogName is null");
        checkNotNull(databaseName, "databaseName is null");
        checkNotNull(tableName, "tableName is null");
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public String getTableName()
    {
        return tableName;
    }
}
