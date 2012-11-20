package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class ImportColumnHandle
        implements ColumnHandle
{
    private final String sourceName;
    private final String columnName;
    private final TupleInfo.Type columnType;

    @JsonCreator
    public ImportColumnHandle(
            @JsonProperty("sourceName") String sourceName,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") TupleInfo.Type columnType)
    {
        this.sourceName = checkNotNull(sourceName, "sourceName is null");
        this.columnName = checkNotNull(columnName, "columnName is null");
        this.columnType = checkNotNull(columnType, "columnType is null");
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
    public TupleInfo.Type getColumnType()
    {
        return columnType;
    }
}
