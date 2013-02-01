package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class ImportTableHandle
    implements TableHandle
{
    private final String sourceName;
    private final String databaseName;
    private final String tableName;

    @JsonCreator
    public ImportTableHandle(
            @JsonProperty("sourceName") String sourceName,
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("tableName") String tableName)
    {
        this.sourceName = Preconditions.checkNotNull(sourceName, "sourceName is null");
        this.databaseName = Preconditions.checkNotNull(databaseName, "databaseName is null");
        this.tableName = Preconditions.checkNotNull(tableName, "tableName is null");
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.IMPORT;
    }

    @JsonProperty
    public String getSourceName()
    {
        return sourceName;
    }

    @JsonProperty
    public String getDatabaseName()
    {
        return databaseName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }
}
