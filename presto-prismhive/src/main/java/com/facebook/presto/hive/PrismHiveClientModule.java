package com.facebook.presto.hive;

import com.facebook.presto.spi.ImportClientFactory;
import com.facebook.prism.namespaceservice.PrismClient;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.swift.service.guice.ThriftClientBinder.thriftClientBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigurationModule.bindConfig;

public class PrismHiveClientModule
    implements Module
{
    @Override
    public void configure(Binder binder)
    {
        bindConfig(binder).to(PrismHiveConfig.class);
        thriftClientBinder(binder).bindThriftClient(PrismClient.class);
        binder.bind(PrismClientProvider.class).in(Scopes.SINGLETON);
        binder.bind(SmcLookup.class).in(Scopes.SINGLETON);
        binder.bind(PrismHiveImportClientFactory.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ImportClientFactory.class).addBinding().to(PrismHiveImportClientFactory.class);
    }
}
