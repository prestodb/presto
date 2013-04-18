package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;

public interface ConnectorHandleResolver
{
    boolean canHandle(TableHandle tableHandle);

    boolean canHandle(ColumnHandle columnHandle);

    Class<? extends TableHandle> getTableHandleClass();

    Class<? extends ColumnHandle> getColumnHandleClass();
}
