package com.facebook.presto.spi;

import java.util.Objects;

import static com.facebook.presto.spi.SchemaUtil.checkLowerCase;

public class SchemaTableName
{
    private final String schemaName;
    private final String tableName;

    public SchemaTableName(String schemaName, String tableName)
    {
        this.schemaName = checkLowerCase(schemaName, "schemaName");
        this.tableName = checkLowerCase(tableName, "tableName");
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
        final SchemaTableName other = (SchemaTableName) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        return schemaName + '.' + tableName;
    }
}
