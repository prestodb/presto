package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static java.lang.String.format;

public class InternalTableHandle
        implements TableHandle
{
    private final QualifiedTableName table;

    public static InternalTableHandle forQualifiedTableName(QualifiedTableName table)
    {
        return new InternalTableHandle(checkTable(table));
    }

    @JsonCreator
    public InternalTableHandle(
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        this(new QualifiedTableName(catalogName, schemaName, tableName));
    }

    private InternalTableHandle(QualifiedTableName table)
    {
        this.table = checkTable(table);
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.INTERNAL;
    }

    @JsonProperty
    public String getCatalogName()
    {
        return table.getCatalogName();
    }

    @JsonProperty
    public String getSchemaName()
    {
        return table.getSchemaName();
    }

    @JsonProperty
    public String getTableName()
    {
        return table.getTableName();
    }

    public QualifiedTableName getTable()
    {
        return table;
    }

    @Override
    public String toString()
    {
        return format("internal:%s", table);
    }
}
