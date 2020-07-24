package com.facebook.presto.execution;

import com.facebook.presto.server.remotetask.ContinuousBatchTaskStatusFetcher;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class GuiceFetcherModule
        extends AbstractModule
{
    @Override
    protected void configure() {
        bind(ContinuousBatchTaskStatusFetcher.class)
            .annotatedWith(Names.named("DefaultCBTSF"))
            .to(ContinuousBatchTaskStatusFetcher.class);
    }
}