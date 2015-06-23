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
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorRecordSinkProvider;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorSplitManager;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
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
    private final TypeManager typeManager;

    public HiveConnectorFactory(String name, Map<String, String> optionalConfig, ClassLoader classLoader, HiveMetastore metastore, TypeManager typeManager)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.optionalConfig = checkNotNull(optionalConfig, "optionalConfig is null");
        this.classLoader = checkNotNull(classLoader, "classLoader is null");
        this.metastore = metastore;
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
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
                    new MBeanModule(),
                    new JsonModule(),
                    new HiveClientModule(connectorId, metastore, typeManager),
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

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            ConnectorMetadata metadata = injector.getInstance(ConnectorMetadata.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorRecordSinkProvider recordSinkProvider = injector.getInstance(ConnectorRecordSinkProvider.class);
            ConnectorHandleResolver handleResolver = injector.getInstance(ConnectorHandleResolver.class);
            HiveSessionProperties hiveSessionProperties = injector.getInstance(HiveSessionProperties.class);

            return new HiveConnector(
                    lifeCycleManager,
                    new ClassLoaderSafeConnectorMetadata(metadata, classLoader),
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                    new ClassLoaderSafeConnectorRecordSinkProvider(recordSinkProvider, classLoader),
                    new ClassLoaderSafeConnectorHandleResolver(handleResolver, classLoader),
                    ImmutableSet.of(),
                    hiveSessionProperties.getSessionProperties());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
