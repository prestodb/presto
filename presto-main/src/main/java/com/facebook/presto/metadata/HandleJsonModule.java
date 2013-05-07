package com.facebook.presto.metadata;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;

import static io.airlift.json.JsonBinder.jsonBinder;

public class HandleJsonModule
        implements Module
{
    public void configure(Binder binder)
    {
        jsonBinder(binder).addModuleBinding().to(TableHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(ColumnHandleJacksonModule.class);
        jsonBinder(binder).addModuleBinding().to(SplitJacksonModule.class);

        binder.bind(HandleResolver.class).in(Scopes.SINGLETON);
        MapBinder<String, ConnectorHandleResolver> connectorHandleResolverBinder = MapBinder.newMapBinder(binder, String.class, ConnectorHandleResolver.class);
        connectorHandleResolverBinder.addBinding("remote").to(RemoteSplitHandleResolver.class).in(Scopes.SINGLETON);
        connectorHandleResolverBinder.addBinding("collocated").to(CollocatedSplitHandleResolver.class).in(Scopes.SINGLETON);
    }
}
