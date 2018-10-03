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
package com.facebook.presto.iceberg;

import com.facebook.presto.hive.HiveSchemaProperties;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.HiveTableProperties;
import com.facebook.presto.hive.HiveTransactionManager;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.RebindSafeMBeanServer;
import com.facebook.presto.hive.authentication.HiveAuthenticationModule;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.HiveMetastore;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastore;
import com.facebook.presto.hive.s3.HiveS3Module;
import com.facebook.presto.hive.security.HiveSecurityModule;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeNodePartitioningProvider;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import java.lang.management.ManagementFactory;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class IcebergConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final ClassLoader classLoader;

    public IcebergConnectorFactory(String name, ClassLoader classLoader)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new IcebergHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new EventModule(),
                    new MBeanModule(),
                    new JsonModule(),
                    new IcebergModule(connectorId, context.getTypeManager(), context.getPageIndexerFactory(), context.getNodeManager()),
                    new HiveS3Module(connectorId),
                    new HiveSecurityModule(),
                    new HiveAuthenticationModule(),
                    binder -> {
                        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
                        binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(platformMBeanServer));
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(HiveMetastore.class).to(ThriftHiveMetastore.class).in(Scopes.SINGLETON);
                        binder.bind(ExtendedHiveMetastore.class).to(BridgingHiveMetastore.class).in(Scopes.SINGLETON);
                    });

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            IcebergMetadataFactory metadataFactory = injector.getInstance(IcebergMetadataFactory.class);
            HiveTransactionManager transactionManager = injector.getInstance(HiveTransactionManager.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorPageSinkProvider pageSinkProvider = injector.getInstance(ConnectorPageSinkProvider.class);
            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(ConnectorNodePartitioningProvider.class);
            HiveSessionProperties hiveSessionProperties = injector.getInstance(HiveSessionProperties.class);
            HiveTableProperties hiveTableProperties = injector.getInstance(HiveTableProperties.class);
            ConnectorAccessControl accessControl = injector.getInstance(ConnectorAccessControl.class);

            return new IcebergConnector(
                    lifeCycleManager,
                    metadataFactory,
                    transactionManager,
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
                    new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                    ImmutableSet.of(),
                    hiveSessionProperties.getSessionProperties(),
                    HiveSchemaProperties.SCHEMA_PROPERTIES,
                    hiveTableProperties.getTableProperties(),
                    accessControl,
                    classLoader);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
