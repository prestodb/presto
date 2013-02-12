/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.thrift;

import com.facebook.presto.spi.ImportClientFactory;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigurationModule.bindConfig;

public class PrestoThriftClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(Integer.class)
          .annotatedWith(Names.named("thrift_client_max_frame_size"))
          .toInstance(10485760);
        newSetBinder(binder, ImportClientFactory.class).addBinding().to(PrestoThriftImportClientFactory.class);
        bindConfig(binder).to(PrestoThriftClientConfig.class);
    }
}
