package com.facebook.presto.metadata;

public class NativeColumnHandle
        implements ColumnHandle
{
    private final long columnId;

    public NativeColumnHandle(long columnId)
    {
        this.columnId = columnId;
    }

    public long getColumnId()
    {
        return columnId;
    }
}
