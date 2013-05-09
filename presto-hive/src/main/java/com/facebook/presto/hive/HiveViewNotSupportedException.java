package com.facebook.presto.hive;

import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.SchemaTableName;

import static java.lang.String.format;

public class HiveViewNotSupportedException
        extends NotFoundException
{
    private final SchemaTableName tableName;

    public HiveViewNotSupportedException(SchemaTableName tableName)
    {
        this(tableName, format("Hive views are not supported: '%s'", tableName));
    }

    public HiveViewNotSupportedException(SchemaTableName tableName, String message)
    {
        super(message);
        this.tableName = tableName;
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }
}
