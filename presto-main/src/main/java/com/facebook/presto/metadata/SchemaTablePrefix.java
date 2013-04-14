package com.facebook.presto.metadata;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import javax.annotation.concurrent.Immutable;

import static com.facebook.presto.metadata.MetadataUtil.checkLowerCase;
import static com.google.common.base.Preconditions.checkArgument;

@Immutable
public class SchemaTablePrefix
{
    private final Optional<String> schemaName;
    private final Optional<String> tableName;

    public SchemaTablePrefix()
    {
        this(Optional.<String>absent(), Optional.<String>absent());
    }

    public SchemaTablePrefix(String schemaName)
    {
        this(Optional.of(schemaName), Optional.<String>absent());
    }

    public SchemaTablePrefix(String schemaName, String tableName)
    {
        this(Optional.of(schemaName), Optional.of(tableName));
    }

    public SchemaTablePrefix(Optional<String> schemaName, Optional<String> tableName)
    {
        checkArgument(!tableName.isPresent() || schemaName.isPresent(), "A schemaName is required when tableName is present");
        this.schemaName = checkLowerCase(schemaName, "schemaName");
        this.tableName = checkLowerCase(tableName, "tableName");
    }

    public Optional<String> getSchemaName()
    {
        return schemaName;
    }

    public Optional<String> getTableName()
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
        SchemaTablePrefix o = (SchemaTablePrefix) obj;
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
        return schemaName.or("*") + '.' + tableName.or("*");
    }
}
