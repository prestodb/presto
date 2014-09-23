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

import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorRecordSinkProvider;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorSplitManager;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import java.lang.management.ManagementFactory;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

public class HiveConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Map<String, String> optionalConfig;
    private final ClassLoader classLoader;
    private final HiveMetastore metastore;

    public HiveConnectorFactory(String name, Map<String, String> optionalConfig, ClassLoader classLoader)
    {
        this(name, optionalConfig, classLoader, null);
    }

    public HiveConnectorFactory(String name, Map<String, String> optionalConfig, ClassLoader classLoader, HiveMetastore metastore)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.optionalConfig = checkNotNull(optionalConfig, "optionalConfig is null");
        this.classLoader = checkNotNull(classLoader, "classLoader is null");
        this.metastore = metastore;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config)
    {
        checkNotNull(config, "config is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new NodeModule(),
                    new DiscoveryModule(),
                    new MBeanModule(),
                    new JsonModule(),
                    new HiveClientModule(connectorId, metastore),
                    new Module()
                    {
                        @Override
                        public void configure(Binder binder)
                        {
                            MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
                            binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(platformMBeanServer));
                        }
                    }
            );

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            HiveClient hiveClient = injector.getInstance(HiveClient.class);
            ConnectorDataStreamProvider dataStreamProvider = injector.getInstance(ConnectorDataStreamProvider.class);

            return new HiveConnector(
                    new ClassLoaderSafeConnectorMetadata(hiveClient, classLoader),
                    new ClassLoaderSafeConnectorSplitManager(hiveClient, classLoader),
                    new ClassLoaderSafeConnectorDataStreamProvider(dataStreamProvider, classLoader),
                    new ClassLoaderSafeConnectorRecordSinkProvider(hiveClient, classLoader),
                    new ClassLoaderSafeConnectorHandleResolver(hiveClient, classLoader));
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
