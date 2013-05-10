/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import org.weakref.jmx.guice.ExportBinder;

import javax.inject.Singleton;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;

public class HiveClientModule
        implements Module
{
    private final String connectorId;

    public HiveClientModule(String connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(HiveConnectorId.class).toInstance(new HiveConnectorId(connectorId));
        binder.bind(HiveClient.class).in(Scopes.SINGLETON);

        binder.bind(FileSystemCache.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(SlowDatanodeSwitcher.class).in(Scopes.SINGLETON);
        binder.bind(FileSystemWrapper.class).toProvider(FileSystemWrapperProvider.class).in(Scopes.SINGLETON);
        binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(HiveClientConfig.class);

        binder.bind(CachingHiveMetastore.class).in(Scopes.SINGLETON);
        ExportBinder.newExporter(binder)
                .export(CachingHiveMetastore.class)
                .as("com.facebook.presto.hive:type=CachingHiveMetastore,name=" + connectorId);

        binder.bind(HiveCluster.class).to(DiscoveryLocatedHiveCluster.class).in(Scopes.SINGLETON);
        binder.bind(HiveMetastoreClientFactory.class).in(Scopes.SINGLETON);
        discoveryBinder(binder).bindSelector("hive-metastore");

        ExportBinder.newExporter(binder)
                .export(SlowDatanodeSwitcher.class)
                .as("com.facebook.presto.hive:type=SlowDatanodeSwitcher,name=" + connectorId);
    }

    @ForHiveClient
    @Singleton
    @Provides
    public ExecutorService createCachingHiveMetastore(HiveConnectorId hiveClientId, HiveClientConfig hiveClientConfig)
    {
        return Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("hive-" + hiveClientId + "-%d").build());
    }
}
