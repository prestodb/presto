package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.split.InternalSplit;
import com.facebook.presto.spi.Split;

public class InternalHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof InternalTableHandle;
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle instanceof InternalColumnHandle;
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof InternalSplit;
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        return InternalTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return InternalColumnHandle.class;
    }

    @Override
    public Class<? extends Split> getSplitClass()
    {
        return InternalSplit.class;
    }
}
