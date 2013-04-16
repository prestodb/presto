package com.facebook.presto.spi;

public class ColumnNotFoundException
        extends NotFoundException
{
    private final SchemaTableName tableName;
    private final String columnName;

    public ColumnNotFoundException(SchemaTableName tableName, String columnName)
    {
        this(tableName, columnName, "Table " + tableName + " not found");
    }

    public ColumnNotFoundException(SchemaTableName tableName, String columnName, String message)
    {
        super(message);
        if (tableName == null) {
            throw new NullPointerException("tableName is null");
        }
        if (columnName == null) {
            throw new NullPointerException("columnName is null");
        }
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public ColumnNotFoundException(SchemaTableName tableName, String columnName, Throwable cause)
    {
        this(tableName, columnName, "Table " + tableName + " not found", cause);
    }

    public ColumnNotFoundException(SchemaTableName tableName, String columnName, String message, Throwable cause)
    {
        super(message, cause);
        if (tableName == null) {
            throw new NullPointerException("tableName is null");
        }
        if (columnName == null) {
            throw new NullPointerException("columnName is null");
        }
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    public String getColumnName()
    {
        return columnName;
    }
}
