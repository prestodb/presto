package com.facebook.presto.hive;

import com.facebook.presto.spi.ImportClientFactory;
import com.facebook.prism.namespaceservice.PrismServiceClient;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.swift.service.guice.ThriftClientBinder.thriftClientBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigurationModule.bindConfig;

public class PrismClientModule
    implements Module
{
    @Override
    public void configure(Binder binder)
    {
        bindConfig(binder).to(PrismConfig.class);
        thriftClientBinder(binder).bindThriftClient(PrismServiceClient.class);
        binder.bind(PrismServiceClientProvider.class).in(Scopes.SINGLETON);
        binder.bind(SmcLookup.class).in(Scopes.SINGLETON);
        binder.bind(PrismImportClientFactory.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ImportClientFactory.class).addBinding().to(PrismImportClientFactory.class);
    }
}
