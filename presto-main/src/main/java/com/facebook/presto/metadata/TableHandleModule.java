package com.facebook.presto.metadata;

import com.facebook.presto.tpch.TpchTableHandle;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.google.inject.Binder;
import com.google.inject.Module;

import static com.facebook.presto.metadata.TableHandleJacksonModule.bindTableHandle;
import static io.airlift.json.JsonBinder.jsonBinder;

public class TableHandleModule
        implements Module
{
    public void configure(Binder binder)
    {
        // replace next three lines with jsonBinder(binder).addModuleBinding().to(TableHandleJacksonModule.class)
        // when airlift pull request #40 is applied.
        jsonBinder(binder).addSerializerBinding(TableHandle.class).to(TableHandleJacksonModule.TableHandleSerializer.class);
        jsonBinder(binder).addDeserializerBinding(TableHandle.class).to(TableHandleJacksonModule.TableHandleDeserializer.class);
        binder.bind(TypeIdResolver.class).annotatedWith(ForTableHandle.class).to(TableHandleJacksonModule.class);

        bindTableHandle(binder, "native").toInstance(NativeTableHandle.class);
        bindTableHandle(binder, "internal").toInstance(InternalTableHandle.class);
        bindTableHandle(binder, "import").toInstance(ImportTableHandle.class);
        bindTableHandle(binder, "tpch").toInstance(TpchTableHandle.class);
    }
}
