package com.facebook.presto.connector.jmx;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;

public class JmxHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof JmxTableHandle;
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle instanceof JmxColumnHandle;
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof JmxSplit;
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        return JmxTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return JmxColumnHandle.class;
    }

    @Override
    public Class<? extends Split> getSplitClass()
    {
        return JmxSplit.class;
    }
}
