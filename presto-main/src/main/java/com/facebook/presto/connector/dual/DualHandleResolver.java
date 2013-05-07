package com.facebook.presto.connector.dual;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;

public class DualHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof DualTableHandle;
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle instanceof DualColumnHandle;
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof DualSplit;
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        return DualTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return DualColumnHandle.class;
    }

    @Override
    public Class<? extends Split> getSplitClass()
    {
        return DualSplit.class;
    }
}
