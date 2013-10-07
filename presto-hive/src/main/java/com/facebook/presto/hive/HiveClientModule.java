/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

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
        binder.bind(FileSystemWrapper.class).toProvider(FileSystemWrapperProvider.class).in(Scopes.SINGLETON);
        binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(HiveClientConfig.class);
        bindConfig(binder).to(HivePluginConfig.class);

        binder.bind(CachingHiveMetastore.class).in(Scopes.SINGLETON);
        newExporter(binder).export(CachingHiveMetastore.class)
                .as(generatedNameOf(CachingHiveMetastore.class, connectorId));

        binder.bind(DiscoveryLocatedHiveCluster.class).in(Scopes.SINGLETON);
        binder.bind(HiveMetastoreClientFactory.class).in(Scopes.SINGLETON);
        discoveryBinder(binder).bindSelector("hive-metastore");
    }

    @ForHiveClient
    @Singleton
    @Provides
    public ExecutorService createHiveClientExecutor(HiveConnectorId hiveClientId)
    {
        return Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("hive-" + hiveClientId + "-%d").build());
    }

    @ForHiveMetastore
    @Singleton
    @Provides
    public ExecutorService createCachingHiveMetastoreExecutor(HiveConnectorId hiveClientId, HiveClientConfig hiveClientConfig)
    {
        return Executors.newFixedThreadPool(
                hiveClientConfig.getMaxMetastoreRefreshThreads(),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("hive-metastore-" + hiveClientId + "-%d").build());
    }

    @Singleton
    @Provides
    public HiveCluster createHiveCluster(Injector injector, HivePluginConfig config)
    {
        URI uri = checkNotNull(config.getMetastoreUri(), "metastoreUri is null");
        String scheme = uri.getScheme();
        checkArgument(!isNullOrEmpty(scheme), "metastoreUri scheme is missing: %s", uri);
        switch (scheme) {
            case "discovery":
                return injector.getInstance(DiscoveryLocatedHiveCluster.class);
            case "thrift":
                checkArgument(uri.getHost() != null, "metastoreUri host is missing: %s", uri);
                checkArgument(uri.getPort() != -1, "metastoreUri port is missing: %s", uri);
                HostAndPort address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
                return new StaticHiveCluster(address, injector.getInstance(HiveMetastoreClientFactory.class));
        }
        throw new IllegalArgumentException("unsupported metastoreUri: " + uri);
    }
}
