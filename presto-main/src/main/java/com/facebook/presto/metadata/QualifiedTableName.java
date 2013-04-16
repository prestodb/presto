package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class QualifiedTableName
{
    @JsonCreator
    public static QualifiedTableName valueOf(String tableName)
    {
        checkNotNull(tableName, "tableName is null");

        ImmutableList<String> ids = ImmutableList.copyOf(Splitter.on('.').split(tableName));
        checkArgument(ids.size() == 3, "Invalid tableName %s", tableName);

        return new QualifiedTableName(ids.get(0), ids.get(1), ids.get(2));
    }

    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    public QualifiedTableName(String catalogName, String schemaName, String tableName)
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

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public QualifiedName asQualifiedName()
    {
        return new QualifiedName(ImmutableList.of(catalogName, schemaName, tableName));
    }

    public SchemaTableName asSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
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
        QualifiedTableName o = (QualifiedTableName) obj;
        return Objects.equal(catalogName, o.catalogName) &&
                Objects.equal(schemaName, o.schemaName) &&
                Objects.equal(tableName, o.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(catalogName, schemaName, tableName);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return catalogName + '.' + schemaName + '.' + tableName;
    }

    public static Function<SchemaTableName, QualifiedTableName> convertFromSchemaTableName(final String catalogName)
    {
        return new Function<SchemaTableName, QualifiedTableName>() {
            @Override
            public QualifiedTableName apply(SchemaTableName input)
            {
                return new QualifiedTableName(catalogName, input.getSchemaName(), input.getTableName());
            }
        };
    }
}
