package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;

public class ImportHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof ImportTableHandle;
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return false;
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        return ImportTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        throw new UnsupportedOperationException();
    }
}
