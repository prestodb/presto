package com.facebook.presto.hive;

import com.facebook.presto.spi.SchemaTableName;

import static com.google.common.base.Preconditions.checkNotNull;

public class TableOfflineException
    extends RuntimeException
{
    private final SchemaTableName tableName;

    public TableOfflineException(SchemaTableName tableName)
    {
        this(tableName, String.format("Table '%s' is offline", tableName));
    }

    public TableOfflineException(SchemaTableName tableName, String message)
    {
        super(message);
        this.tableName = checkNotNull(tableName, "tableName is null");
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }
}
