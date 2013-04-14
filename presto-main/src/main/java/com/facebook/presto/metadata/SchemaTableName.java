package com.facebook.presto.metadata;

import com.google.common.base.Objects;

import javax.annotation.concurrent.Immutable;

import static com.facebook.presto.metadata.MetadataUtil.checkLowerCase;

@Immutable
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
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        SchemaTableName o = (SchemaTableName) obj;
        return Objects.equal(schemaName, o.schemaName) &&
                Objects.equal(tableName, o.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return schemaName + '.' + tableName;
    }
}
