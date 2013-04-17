package com.facebook.presto.metadata;

import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.inject.Binder;
import com.google.inject.Module;

import static com.facebook.presto.metadata.HandleJsonBinder.bindColumnHandle;
import static com.facebook.presto.metadata.HandleJsonBinder.bindTableHandle;
import static io.airlift.json.JsonBinder.jsonBinder;

public class HandleJsonModule
        implements Module
{
    public void configure(Binder binder)
    {
        jsonBinder(binder).addModuleBinding().to(TableHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(ColumnHandleJacksonModule.class);

        bindTableHandle(binder, "native").toInstance(NativeTableHandle.class);
        bindTableHandle(binder, "internal").toInstance(InternalTableHandle.class);
        bindTableHandle(binder, "import").toInstance(ImportTableHandle.class);
        bindTableHandle(binder, "tpch").toInstance(TpchTableHandle.class);

        bindColumnHandle(binder, "native").toInstance(NativeColumnHandle.class);
        bindColumnHandle(binder, "internal").toInstance(InternalColumnHandle.class);
        bindColumnHandle(binder, "tpch").toInstance(TpchColumnHandle.class);
    }
}
