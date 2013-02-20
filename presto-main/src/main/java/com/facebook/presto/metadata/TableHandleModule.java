package com.facebook.presto.metadata;

import com.facebook.presto.tpch.TpchTableHandle;
import com.google.inject.Binder;
import com.google.inject.Module;

import static com.facebook.presto.metadata.TableHandleBinder.bindTableHandle;
import static io.airlift.json.JsonBinder.jsonBinder;

public class TableHandleModule
        implements Module
{
    public void configure(Binder binder)
    {
        jsonBinder(binder).addModuleBinding().to(TableHandleJacksonModule.class);

        bindTableHandle(binder, "native").toInstance(NativeTableHandle.class);
        bindTableHandle(binder, "internal").toInstance(InternalTableHandle.class);
        bindTableHandle(binder, "import").toInstance(ImportTableHandle.class);
        bindTableHandle(binder, "tpch").toInstance(TpchTableHandle.class);
    }
}
