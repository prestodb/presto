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

import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.google.common.net.HostAndPort;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import javax.inject.Singleton;

import java.net.URI;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HiveClientModule
        implements Module
{
    private final String connectorId;
    private final HiveMetastore metastore;

    public HiveClientModule(String connectorId, HiveMetastore metastore)
    {
        this.connectorId = connectorId;
        this.metastore = metastore;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(HiveConnectorId.class).toInstance(new HiveConnectorId(connectorId));
        binder.bind(HiveClient.class).in(Scopes.SINGLETON);

        binder.bind(HdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
        binder.bind(DirectoryLister.class).to(HadoopDirectoryLister.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(HiveClientConfig.class);
        bindConfig(binder).to(HivePluginConfig.class);

        if (metastore != null) {
            binder.bind(HiveMetastore.class).toInstance(metastore);
        }
        else {
            binder.bind(HiveMetastore.class).to(CachingHiveMetastore.class).in(Scopes.SINGLETON);
            newExporter(binder).export(HiveMetastore.class)
                    .as(generatedNameOf(CachingHiveMetastore.class, connectorId));
        }

        binder.bind(NamenodeStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(NamenodeStats.class).as(generatedNameOf(NamenodeStats.class));

        binder.bind(DiscoveryLocatedHiveCluster.class).in(Scopes.SINGLETON);
        binder.bind(HiveMetastoreClientFactory.class).in(Scopes.SINGLETON);
        discoveryBinder(binder).bindSelector("hive-metastore");

        binder.bind(ConnectorDataStreamProvider.class).to(HiveDataStreamProvider.class).in(Scopes.SINGLETON);

        Multibinder<HiveRecordCursorProvider> recordCursorProviderBinder = Multibinder.newSetBinder(binder, HiveRecordCursorProvider.class);
        recordCursorProviderBinder.addBinding().to(OrcRecordCursorProvider.class).in(Scopes.SINGLETON);
        recordCursorProviderBinder.addBinding().to(ParquetRecordCursorProvider.class).in(Scopes.SINGLETON);
        recordCursorProviderBinder.addBinding().to(DwrfRecordCursorProvider.class).in(Scopes.SINGLETON);
        recordCursorProviderBinder.addBinding().to(ColumnarTextHiveRecordCursorProvider.class).in(Scopes.SINGLETON);
        recordCursorProviderBinder.addBinding().to(ColumnarBinaryHiveRecordCursorProvider.class).in(Scopes.SINGLETON);
        recordCursorProviderBinder.addBinding().to(GenericHiveRecordCursorProvider.class).in(Scopes.SINGLETON);
    }

    @ForHiveClient
    @Singleton
    @Provides
    public ExecutorService createHiveClientExecutor(HiveConnectorId hiveClientId)
    {
        return newCachedThreadPool(daemonThreadsNamed("hive-" + hiveClientId + "-%s"));
    }

    @ForHiveMetastore
    @Singleton
    @Provides
    public ExecutorService createCachingHiveMetastoreExecutor(HiveConnectorId hiveClientId, HiveClientConfig hiveClientConfig)
    {
        return newFixedThreadPool(
                hiveClientConfig.getMaxMetastoreRefreshThreads(),
                daemonThreadsNamed("hive-metastore-" + hiveClientId + "-%s"));
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
