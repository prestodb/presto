package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;

import javax.inject.Inject;

import java.util.Map;

public class ColumnHandleJacksonModule
        extends AbstractTypedJacksonModule<ColumnHandle>
{
    @Inject
    public ColumnHandleJacksonModule(Map<String, Class<? extends ColumnHandle>> columnHandleTypes)
    {
        super(ColumnHandle.class, "columnHandleType", columnHandleTypes);
    }
}
