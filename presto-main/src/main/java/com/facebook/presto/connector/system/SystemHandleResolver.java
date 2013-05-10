package com.facebook.presto.connector.system;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;

public class SystemHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof SystemTableHandle;
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle instanceof SystemColumnHandle;
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof SystemSplit;
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        return SystemTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return SystemColumnHandle.class;
    }

    @Override
    public Class<? extends Split> getSplitClass()
    {
        return SystemSplit.class;
    }
}
