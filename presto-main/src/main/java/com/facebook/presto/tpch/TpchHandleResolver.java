package com.facebook.presto.tpch;

import com.facebook.presto.metadata.ConnectorHandleResolver;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;

public class TpchHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof TpchTableHandle;
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle instanceof TpchColumnHandle;
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        return TpchTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return TpchColumnHandle.class;
    }
}
