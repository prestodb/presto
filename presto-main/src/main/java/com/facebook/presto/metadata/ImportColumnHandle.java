package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class ImportColumnHandle
        implements ColumnHandle
{
    private final String sourceName;
    private final String columnName;
    private final int columnId;
    private final TupleInfo.Type columnType;

    @JsonCreator
    public ImportColumnHandle(
            @JsonProperty("sourceName") String sourceName,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnId") int columnId,
            @JsonProperty("columnType") TupleInfo.Type columnType)
    {
        Preconditions.checkArgument(columnId >= 0, "columnId must be >= 0");

        this.sourceName = checkNotNull(sourceName, "sourceName is null");
        this.columnName = checkNotNull(columnName, "columnName is null");
        this.columnType = checkNotNull(columnType, "columnType is null");
        this.columnId = columnId;
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
    public int getColumnId()
    {
        return columnId;
    }

    @JsonProperty
    public TupleInfo.Type getColumnType()
    {
        return columnType;
    }

    public static Function<ImportColumnHandle, String> columnNameGetter()
    {
        return new Function<ImportColumnHandle, String>()
        {
            @Override
            public String apply(ImportColumnHandle input)
            {
                return input.getColumnName();
            }
        };
    }
}
