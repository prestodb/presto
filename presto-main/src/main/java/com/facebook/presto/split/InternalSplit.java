package com.facebook.presto.split;

import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.metadata.InternalTableHandle;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class InternalSplit
        implements Split
{
    private final InternalTableHandle tableHandle;

    @JsonCreator
    public InternalSplit(@JsonProperty("tableHandle") InternalTableHandle tableHandle)
    {
        this.tableHandle = checkNotNull(tableHandle, "tableHandle is null");
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.INTERNAL;
    }

    @JsonProperty
    public InternalTableHandle getTableHandle()
    {
        return tableHandle;
    }
}
