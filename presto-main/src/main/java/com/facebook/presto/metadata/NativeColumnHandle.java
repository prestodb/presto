package com.facebook.presto.metadata;

import static com.google.common.base.Preconditions.checkArgument;

public class NativeColumnHandle
        implements ColumnHandle
{
    private final long columnId;

    public NativeColumnHandle(long columnId)
    {
        checkArgument(columnId > 0, "columnId must be greater than zero");
        this.columnId = columnId;
    }

    public long getColumnId()
    {
        return columnId;
    }
}
