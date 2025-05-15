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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.event.client.EventModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.json.smile.SmileModule;
import com.facebook.presto.cache.CachingModule;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.authentication.HiveAuthenticationModule;
import com.facebook.presto.hive.azure.HiveAzureModule;
import com.facebook.presto.hive.gcs.HiveGcsModule;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HiveMetastoreModule;
import com.facebook.presto.hive.s3.HiveS3Module;
import com.facebook.presto.hive.security.HiveSecurityModule;
import com.facebook.presto.hive.security.SystemTableAwareAccessControl;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSystemConfig;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadataUpdaterProvider;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTypeSerdeProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeNodePartitioningProvider;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class HiveConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final ClassLoader classLoader;
    private final Optional<ExtendedHiveMetastore> metastore;

    public HiveConnectorFactory(String name, ClassLoader classLoader, Optional<ExtendedHiveMetastore> metastore)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
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
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new EventModule(),
                    new MBeanModule(),
                    new JsonModule(),
                    new SmileModule(),
                    new HiveClientModule(catalogName),
                    new HiveS3Module(catalogName),
                    new HiveGcsModule(),
                    new HiveAzureModule(),
                    new HiveMetastoreModule(catalogName, metastore),
                    new HiveSecurityModule(),
                    new HiveAuthenticationModule(),
                    new HiveProcedureModule(),
                    new CachingModule(),
                    new HiveCommonModule(),
                    binder -> {
                        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
                        binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(platformMBeanServer));
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                        binder.bind(PageSorter.class).toInstance(context.getPageSorter());
                        binder.bind(StandardFunctionResolution.class).toInstance(context.getStandardFunctionResolution());
                        binder.bind(FunctionMetadataManager.class).toInstance(context.getFunctionMetadataManager());
                        binder.bind(RowExpressionService.class).toInstance(context.getRowExpressionService());
                        binder.bind(FilterStatsCalculatorService.class).toInstance(context.getFilterStatsCalculatorService());
                        binder.bind(BlockEncodingSerde.class).toInstance(context.getBlockEncodingSerde());
                        binder.bind(ConnectorSystemConfig.class).toInstance(context.getConnectorSystemConfig());
                    });

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .quiet()
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            HiveMetadataFactory metadataFactory = injector.getInstance(HiveMetadataFactory.class);
            HiveTransactionManager transactionManager = injector.getInstance(HiveTransactionManager.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorPageSinkProvider pageSinkProvider = injector.getInstance(ConnectorPageSinkProvider.class);
            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(ConnectorNodePartitioningProvider.class);
            HiveSessionProperties hiveSessionProperties = injector.getInstance(HiveSessionProperties.class);
            HiveCommonSessionProperties hiveCommonSessionProperties = injector.getInstance(HiveCommonSessionProperties.class);
            HiveTableProperties hiveTableProperties = injector.getInstance(HiveTableProperties.class);
            HiveAnalyzeProperties hiveAnalyzeProperties = injector.getInstance(HiveAnalyzeProperties.class);
            ConnectorAccessControl accessControl = new SystemTableAwareAccessControl(injector.getInstance(ConnectorAccessControl.class));
            Set<Procedure> procedures = injector.getInstance(Key.get(new TypeLiteral<Set<Procedure>>() {}));
            ConnectorPlanOptimizerProvider planOptimizerProvider = injector.getInstance(ConnectorPlanOptimizerProvider.class);
            ConnectorMetadataUpdaterProvider metadataUpdaterProvider = injector.getInstance(ConnectorMetadataUpdaterProvider.class);
            ConnectorTypeSerdeProvider connectorTypeSerdeProvider = injector.getInstance(ConnectorTypeSerdeProvider.class);

            List<PropertyMetadata<?>> allSessionProperties = new ArrayList<>(hiveSessionProperties.getSessionProperties());
            allSessionProperties.addAll(hiveCommonSessionProperties.getSessionProperties());

            return new HiveConnector(
                    lifeCycleManager,
                    metadataFactory,
                    transactionManager,
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
                    new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                    ImmutableSet.of(),
                    procedures,
                    allSessionProperties,
                    SchemaProperties.SCHEMA_PROPERTIES,
                    hiveTableProperties.getTableProperties(),
                    hiveAnalyzeProperties.getAnalyzeProperties(),
                    accessControl,
                    planOptimizerProvider,
                    metadataUpdaterProvider,
                    connectorTypeSerdeProvider,
                    classLoader);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
