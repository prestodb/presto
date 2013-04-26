package com.facebook.presto.spi;

import java.util.Objects;

import static com.facebook.presto.spi.SchemaUtil.checkNotEmpty;

public class SchemaTablePrefix
{
    /* nullable */
    private final String schemaName;
    /* nullable */
    private final String tableName;

    public SchemaTablePrefix()
    {
        this.schemaName = null;
        this.tableName = null;
    }

    public SchemaTablePrefix(String schemaName)
    {
        this.schemaName = checkNotEmpty(schemaName, "schemaName");
        this.tableName = null;
    }

    public SchemaTablePrefix(String schemaName, String tableName)
    {
        this.schemaName = checkNotEmpty(schemaName, "schemaName");
        this.tableName = checkNotEmpty(tableName, "tableName");
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SchemaTablePrefix other = (SchemaTablePrefix) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        return (schemaName == null ? "*" : schemaName) +
                '.' +
                (tableName == null ? "*" : tableName);
    }
}
