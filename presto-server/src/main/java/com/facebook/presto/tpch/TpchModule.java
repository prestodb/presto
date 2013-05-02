package com.facebook.presto.tpch;

import com.facebook.presto.spi.ConnectorFactory;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;

public class TpchModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        MapBinder.newMapBinder(binder, String.class, ConnectorFactory.class).addBinding("tpch").to(TpchConnectorFactory.class);
    }
}
