package com.facebook.presto.tpch;

import com.facebook.presto.spi.ConnectorFactory;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;

public class TpchModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(TpchMetadata.class).in(Scopes.SINGLETON);
        binder.bind(TpchSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(TpchDataStreamProvider.class).in(Scopes.SINGLETON);
        MapBinder.newMapBinder(binder, String.class, ConnectorFactory.class).addBinding("tpch").to(TpchConnectorFactory.class);
    }
}
