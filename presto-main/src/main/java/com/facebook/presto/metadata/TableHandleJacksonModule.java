package com.facebook.presto.metadata;

import javax.inject.Inject;

import java.util.Map;

public class TableHandleJacksonModule
        extends AbstractTypedJacksonModule<TableHandle>
{
    @Inject
    public TableHandleJacksonModule(Map<String, Class<? extends TableHandle>> tableHandleTypes)
    {
        super(TableHandle.class, "type", tableHandleTypes);
    }
}
