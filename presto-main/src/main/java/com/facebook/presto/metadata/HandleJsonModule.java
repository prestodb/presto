package com.facebook.presto.metadata;

import com.google.inject.Binder;
import com.google.inject.Module;

import static io.airlift.json.JsonBinder.jsonBinder;

public class HandleJsonModule
        implements Module
{
    public void configure(Binder binder)
    {
        jsonBinder(binder).addModuleBinding().to(TableHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(ColumnHandleJacksonModule.class);
    }
}
