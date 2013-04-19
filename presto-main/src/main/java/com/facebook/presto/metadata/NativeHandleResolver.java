package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.split.NativeSplit;
import com.facebook.presto.spi.Split;

public class NativeHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof NativeTableHandle;
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle instanceof NativeColumnHandle;
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof NativeSplit;
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        return NativeTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return NativeColumnHandle.class;
    }

    @Override
    public Class<? extends Split> getSplitClass()
    {
        return NativeSplit.class;
    }
}
