package com.facebook.presto.spi;

public class SchemaNotFoundException
        extends NotFoundException
{
    private final String schemaName;

    public SchemaNotFoundException(String schemaName)
    {
        this(schemaName, "Schema " + schemaName + " not found");
    }

    public SchemaNotFoundException(String schemaName, String message)
    {
        super(message);
        if (schemaName == null) {
            throw new NullPointerException("schemaName is null");
        }
        this.schemaName = schemaName;
    }

    public SchemaNotFoundException(String schemaName, Throwable cause)
    {
        this(schemaName, "Schema " + schemaName + " not found", cause);
    }

    public SchemaNotFoundException(String schemaName, String message, Throwable cause)
    {
        super(message, cause);
        if (schemaName == null) {
            throw new NullPointerException("schemaName is null");
        }
        this.schemaName = schemaName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }
}
