package com.facebook.presto.event.scribe.client;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.swift.service.guice.ThriftClientBinder.thriftClientBinder;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;

public class ScribeClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        thriftClientBinder(binder).bindThriftClient(ScribeClient.class);
        discoveryBinder(binder).bindSelector("scribe");
        bindConfig(binder).to(ScribeClientConfiguration.class);
        binder.bind(ScribeClientProvider.class).in(Scopes.SINGLETON);
        binder.bind(AsyncScribeLogger.class).in(Scopes.SINGLETON);

        binder.bind(ScribeClient.class).toProvider(ScribeClientProvider.class);
    }
}
