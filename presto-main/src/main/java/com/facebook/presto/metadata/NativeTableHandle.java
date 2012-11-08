package com.facebook.presto.metadata;

public class NativeTableHandle
        implements TableHandle
{
    private final long tableId;

    public NativeTableHandle(long tableId)
    {
        this.tableId = tableId;
    }

    public long getTableId()
    {
        return tableId;
    }
}
