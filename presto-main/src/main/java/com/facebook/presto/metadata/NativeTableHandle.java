package com.facebook.presto.metadata;

import static com.google.common.base.Preconditions.checkArgument;

public class NativeTableHandle
        implements TableHandle
{
    private final long tableId;

    public NativeTableHandle(long tableId)
    {
        checkArgument(tableId > 0, "tableId must be greater than zero");
        this.tableId = tableId;
    }

    public long getTableId()
    {
        return tableId;
    }
}
