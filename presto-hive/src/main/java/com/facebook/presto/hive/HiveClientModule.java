/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.weakref.jmx.guice.ExportBinder;

import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;

public class HiveClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(HiveImportClientFactory.class).in(Scopes.SINGLETON);

        binder.bind(HiveClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(FileSystemCache.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(SlowDatanodeSwitcher.class).in(Scopes.SINGLETON);
        binder.bind(FileSystemWrapper.class).toProvider(FileSystemWrapperProvider.class).in(Scopes.SINGLETON);
        binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
        binder.bind(DiscoveryLocatedHiveCluster.class).in(Scopes.SINGLETON);
        binder.bind(HiveCluster.class).to(DiscoveryLocatedHiveCluster.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(HiveClientConfig.class);
        binder.bind(HiveMetastoreClientFactory.class).in(Scopes.SINGLETON);
        discoveryBinder(binder).bindSelector("hive-metastore");

        ExportBinder.newExporter(binder)
                .export(SlowDatanodeSwitcher.class)
                .withGeneratedName();
    }
}
