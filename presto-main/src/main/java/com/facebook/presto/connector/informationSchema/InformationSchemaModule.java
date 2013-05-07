package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class InformationSchemaModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        newSetBinder(binder, ConnectorSplitManager.class).addBinding().to(InformationSchemaSplitManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorDataStreamProvider.class).addBinding().to(InformationSchemaDataStreamProvider.class).in(Scopes.SINGLETON);
        newMapBinder(binder, String.class, ConnectorHandleResolver.class).addBinding("information_schema").to(InformationSchemaHandleResolver.class).in(Scopes.SINGLETON);
    }
}
