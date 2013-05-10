package com.facebook.presto.connector.dual;

import com.facebook.presto.spi.Connector;
import com.google.inject.Binder;
import com.google.inject.Module;

import static com.google.inject.multibindings.MapBinder.newMapBinder;

public class DualModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        newMapBinder(binder, String.class, Connector.class).addBinding("dual").to(DualConnector.class);
    }
}
