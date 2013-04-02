package com.facebook.presto.server;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.guice.ExportBinder;
import org.weakref.jmx.guice.MBeanModule;

import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class FailureDetectorModule
    implements Module
{
    @Override
    public void configure(Binder binder)
    {
        httpClientBinder(binder)
                .bindAsyncHttpClient("failure-detector", ForFailureDetector.class)
                .withTracing();

        bindConfig(binder).to(FailureDetectorConfiguration.class);

        binder.bind(HeartbeatFailureDetector.class).in(Scopes.SINGLETON);

        binder.bind(FailureDetector.class)
                .to(HeartbeatFailureDetector.class)
                .in(Scopes.SINGLETON);

        ExportBinder.newExporter(binder)
                .export(HeartbeatFailureDetector.class)
                .withGeneratedName();
    }
}
