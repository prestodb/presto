package com.facebook.presto.metadata;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class ImportColumnHandle
    implements ColumnHandle
{
    private final String sourceName;
    private final String columnName;
    private final ImportTableHandle importTableHandle;

    @JsonCreator
    public ImportColumnHandle(
            @JsonProperty("sourceName") String sourceName,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("importTableHandle") ImportTableHandle importTableHandle)
    {
        checkNotNull(sourceName, "sourceName is null");
        checkNotNull(columnName, "columnName is null");
        checkNotNull(importTableHandle, "importTableHandle is null");

        this.sourceName = sourceName;
        this.columnName = columnName;
        this.importTableHandle = importTableHandle;
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
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public ImportTableHandle getImportTableHandle()
    {
        return importTableHandle;
    }
}
