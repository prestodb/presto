package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.Split;

public interface ConnectorHandleResolver
{
    boolean canHandle(TableHandle tableHandle);

    boolean canHandle(ColumnHandle columnHandle);

    boolean canHandle(Split split);

    Class<? extends TableHandle> getTableHandleClass();

    Class<? extends ColumnHandle> getColumnHandleClass();

    Class<? extends Split> getSplitClass();
}
