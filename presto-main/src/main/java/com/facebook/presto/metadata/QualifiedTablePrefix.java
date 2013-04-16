package com.facebook.presto.metadata;

import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

import javax.annotation.concurrent.Immutable;

import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.checkSchemaName;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;

@Immutable
public class QualifiedTablePrefix
{
    private final String catalogName;
    private final Optional<String> schemaName;
    private final Optional<String> tableName;

    public QualifiedTablePrefix(String catalogName)
    {
        this.catalogName = checkCatalogName(catalogName);
        this.schemaName = Optional.absent();
        this.tableName = Optional.absent();
    }

    public QualifiedTablePrefix(String catalogName, String schemaName)
    {
        this.catalogName = checkCatalogName(catalogName);
        this.schemaName = Optional.of(checkSchemaName(schemaName));
        this.tableName = Optional.absent();
    }

    public QualifiedTablePrefix(String catalogName, String schemaName, String tableName)
    {
        this.catalogName = checkCatalogName(catalogName);
        this.schemaName = Optional.of(checkSchemaName(schemaName));
        this.tableName = Optional.of(checkTableName(tableName));
    }

    public QualifiedTablePrefix(String catalogName, Optional<String> schemaName, Optional<String> tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public Optional<String> getSchemaName()
    {
        return schemaName;
    }

    public Optional<String> getTableName()
    {
        return tableName;
    }

    public boolean hasSchemaName()
    {
        return schemaName.isPresent();
    }

    public boolean hasTableName()
    {
        return tableName.isPresent();
    }

    public SchemaTablePrefix asSchemaTablePrefix()
    {
        return new SchemaTablePrefix(schemaName.orNull(), tableName.orNull());
    }

    public SchemaTablePrefix toSchemaTablePrefix() {
        if (!schemaName.isPresent()) {
            return new SchemaTablePrefix();
        } else if (!tableName.isPresent()) {
            return new SchemaTablePrefix(schemaName.get());
        } else {
            return new SchemaTablePrefix(schemaName.get(), tableName.get());
        }
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
        QualifiedTablePrefix o = (QualifiedTablePrefix) obj;
        return Objects.equal(catalogName, o.catalogName) &&
                Objects.equal(schemaName, o.schemaName) &&
                Objects.equal(tableName, o.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(catalogName, schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return catalogName + '.' + schemaName.or("*") + '.' + tableName.or("*");
    }
}
