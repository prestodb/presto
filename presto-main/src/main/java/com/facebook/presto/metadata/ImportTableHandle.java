package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.facebook.presto.metadata.MetadataUtil.checkTable;

public class ImportTableHandle
    implements TableHandle
{
    private final QualifiedTableName table;

    public static ImportTableHandle forQualifiedTableName(QualifiedTableName table)
    {
        return new ImportTableHandle(checkTable(table));
    }

    @JsonCreator
    public ImportTableHandle(
            @JsonProperty("sourceName") String sourceName,
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("tableName") String tableName)
    {
        this(new QualifiedTableName(sourceName, databaseName, tableName));
    }

    private ImportTableHandle(QualifiedTableName table)
    {
        this.table = checkTable(table);
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.IMPORT;
    }

    @JsonProperty
    public String getSourceName()
    {
        return table.getCatalogName();
    }

    @JsonProperty
    public String getDatabaseName()
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
}
