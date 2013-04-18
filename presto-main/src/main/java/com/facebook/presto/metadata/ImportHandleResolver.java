package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.Split;

import static com.google.common.base.Preconditions.checkNotNull;

public class ImportHandleResolver
        implements ConnectorHandleResolver
{
    private final ImportClient importClient;

    public ImportHandleResolver(ImportClient importClient)
    {
        this.importClient = checkNotNull(importClient, "importClient is null");
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return importClient.canHandle(tableHandle);
    }

    @Override
    public boolean canHandle(ColumnHandle tableHandle)
    {
        return importClient.canHandle(tableHandle);
    }

    @Override
    public boolean canHandle(Split split)
    {
        return importClient.canHandle(split);
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        return importClient.getTableHandleClass();
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return importClient.getColumnHandleClass();
    }

    @Override
    public Class<? extends Split> getSplitClass()
    {
        return importClient.getSplitClass();
    }
}
