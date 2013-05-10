package com.facebook.presto.connector.system;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.SystemTable;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import static com.google.inject.multibindings.MapBinder.newMapBinder;

public class SystemTablesModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        newMapBinder(binder, String.class, Connector.class).addBinding("system").to(SystemConnector.class);
        binder.bind(SystemTablesManager.class).in(Scopes.SINGLETON);
        binder.bind(SystemTablesMetadata.class).in(Scopes.SINGLETON);
        binder.bind(SystemSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(SystemDataStreamProvider.class).in(Scopes.SINGLETON);
        Multibinder<SystemTable> globalTableBinder = Multibinder.newSetBinder(binder, SystemTable.class);
        globalTableBinder.addBinding().to(NodesSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(QuerySystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(TaskSystemTable.class).in(Scopes.SINGLETON);
    }
}
