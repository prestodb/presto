package com.facebook.presto.spi;

public interface ConnectorHandleResolver
{
    boolean canHandle(TableHandle tableHandle);

    boolean canHandle(ColumnHandle columnHandle);

    boolean canHandle(Split split);

    Class<? extends TableHandle> getTableHandleClass();

    Class<? extends ColumnHandle> getColumnHandleClass();

    Class<? extends Split> getSplitClass();
}
