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
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
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
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import java.lang.management.ManagementFactory;
import java.util.Map;

import static com.facebook.presto.hive.ConditionalModule.installModuleIf;
import static com.facebook.presto.hive.SecurityConfig.ALLOW_ALL_ACCESS_CONTROL;
import static com.facebook.presto.hive.authentication.AuthenticationModules.kerberosHdfsAuthenticationModule;
import static com.facebook.presto.hive.authentication.AuthenticationModules.kerberosHiveMetastoreAuthenticationModule;
import static com.facebook.presto.hive.authentication.AuthenticationModules.kerberosImpersonatingHdfsAuthenticationModule;
import static com.facebook.presto.hive.authentication.AuthenticationModules.noHdfsAuthenticationModule;
import static com.facebook.presto.hive.authentication.AuthenticationModules.noHiveMetastoreAuthenticationModule;
import static com.facebook.presto.hive.authentication.AuthenticationModules.simpleImpersonatingHdfsAuthenticationModule;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class HiveConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Map<String, String> optionalConfig;
    private final ClassLoader classLoader;
    private final HiveMetastore metastore;
    private final TypeManager typeManager;
    private final PageIndexerFactory pageIndexerFactory;
    private final NodeManager nodeManager;

    public HiveConnectorFactory(
            String name,
            Map<String, String> optionalConfig,
            ClassLoader classLoader,
            HiveMetastore metastore,
            TypeManager typeManager,
            PageIndexerFactory pageIndexerFactory,
            NodeManager nodeManager)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.optionalConfig = requireNonNull(optionalConfig, "optionalConfig is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.metastore = metastore;
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexer is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new HiveHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config)
    {
        requireNonNull(config, "config is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new NodeModule(),
                    new MBeanModule(),
                    new JsonModule(),
                    new HiveClientModule(connectorId, metastore, typeManager, pageIndexerFactory, nodeManager),
                    installModuleIf(
                            SecurityConfig.class,
                            security -> ALLOW_ALL_ACCESS_CONTROL.equalsIgnoreCase(security.getSecuritySystem()),
                            new NoSecurityModule()),
                    installModuleIf(
                            SecurityConfig.class,
                            security -> "read-only".equalsIgnoreCase(security.getSecuritySystem()),
                            new ReadOnlySecurityModule()),
                    installModuleIf(
                            SecurityConfig.class,
                            security -> "sql-standard".equalsIgnoreCase(security.getSecuritySystem()),
                            new SqlStandardSecurityModule()),
                    installModuleIf(
                            HiveClientConfig.class,
                            hiveClientConfig -> hiveClientConfig.getHiveMetastoreAuthenticationType() == HiveClientConfig.HiveMetastoreAuthenticationType.NONE,
                            noHiveMetastoreAuthenticationModule()),
                    installModuleIf(
                            HiveClientConfig.class,
                            hiveClientConfig -> hiveClientConfig.getHiveMetastoreAuthenticationType() == HiveClientConfig.HiveMetastoreAuthenticationType.KERBEROS,
                            kerberosHiveMetastoreAuthenticationModule()),
                    installModuleIf(
                            HiveClientConfig.class,
                            configuration -> configuration.getHdfsAuthenticationType() == HiveClientConfig.HdfsAuthenticationType.NONE &&
                                    !configuration.isHdfsImpersonationEnabled(),
                            noHdfsAuthenticationModule()),
                    installModuleIf(
                            HiveClientConfig.class,
                            configuration -> configuration.getHdfsAuthenticationType() == HiveClientConfig.HdfsAuthenticationType.NONE &&
                                    configuration.isHdfsImpersonationEnabled(),
                            simpleImpersonatingHdfsAuthenticationModule()),
                    installModuleIf(
                            HiveClientConfig.class,
                            configuration -> configuration.getHdfsAuthenticationType() == HiveClientConfig.HdfsAuthenticationType.KERBEROS &&
                                    !configuration.isHdfsImpersonationEnabled(),
                            kerberosHdfsAuthenticationModule()),
                    installModuleIf(
                            HiveClientConfig.class,
                            configuration -> configuration.getHdfsAuthenticationType() == HiveClientConfig.HdfsAuthenticationType.KERBEROS &&
                                    configuration.isHdfsImpersonationEnabled(),
                            kerberosImpersonatingHdfsAuthenticationModule()),
                    binder -> {
                        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
                        binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(platformMBeanServer));
                    }
            );

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            HiveMetadataFactory metadataFactory = injector.getInstance(HiveMetadataFactory.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorPageSinkProvider pageSinkProvider = injector.getInstance(ConnectorPageSinkProvider.class);
            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(ConnectorNodePartitioningProvider.class);
            HiveSessionProperties hiveSessionProperties = injector.getInstance(HiveSessionProperties.class);
            HiveTableProperties hiveTableProperties = injector.getInstance(HiveTableProperties.class);
            ConnectorAccessControl accessControl = injector.getInstance(ConnectorAccessControl.class);

            return new HiveConnector(
                    lifeCycleManager,
                    metadataFactory,
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
                    new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                    ImmutableSet.of(),
                    hiveSessionProperties.getSessionProperties(),
                    hiveTableProperties.getTableProperties(),
                    accessControl,
                    classLoader);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
