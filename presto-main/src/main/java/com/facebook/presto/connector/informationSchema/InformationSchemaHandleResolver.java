package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;

public class InformationSchemaHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof InformationSchemaTableHandle;
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle instanceof InformationSchemaColumnHandle;
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof InformationSchemaSplit;
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        return InformationSchemaTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return InformationSchemaColumnHandle.class;
    }

    @Override
    public Class<? extends Split> getSplitClass()
    {
        return InformationSchemaSplit.class;
    }
}
