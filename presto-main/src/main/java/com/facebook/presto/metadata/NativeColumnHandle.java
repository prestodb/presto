package com.facebook.presto.metadata;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import static com.google.common.base.Preconditions.checkArgument;

public class NativeColumnHandle
        implements ColumnHandle
{
    private final long columnId;

    @JsonCreator
    public NativeColumnHandle(@JsonProperty("columnId") long columnId)
    {
        checkArgument(columnId > 0, "columnId must be greater than zero");
        this.columnId = columnId;
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.NATIVE;
    }

    @JsonProperty
    public long getColumnId()
    {
        return columnId;
    }
}
