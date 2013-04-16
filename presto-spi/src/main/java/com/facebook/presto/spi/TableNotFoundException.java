package com.facebook.presto.spi;

public class TableNotFoundException
        extends NotFoundException
{
    private final SchemaTableName tableName;

    public TableNotFoundException(SchemaTableName tableName)
    {
        this(tableName, "Table " + tableName + " not found");
    }

    public TableNotFoundException(SchemaTableName tableName, String message)
    {
        super(message);
        if (tableName == null) {
            throw new NullPointerException("tableName is null");
        }
        this.tableName = tableName;
    }

    public TableNotFoundException(SchemaTableName tableName, Throwable cause)
    {
        this(tableName, "Table " + tableName + " not found", cause);
    }

    public TableNotFoundException(SchemaTableName tableName, String message, Throwable cause)
    {
        super(message, cause);
        if (tableName == null) {
            throw new NullPointerException("tableName is null");
        }
        this.tableName = tableName;
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }
}
