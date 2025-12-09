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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.event.client.EventModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.cache.CachingModule;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HiveCommonModule;
import com.facebook.presto.hive.HiveCommonSessionProperties;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.RebindSafeMBeanServer;
import com.facebook.presto.hive.SchemaProperties;
import com.facebook.presto.hive.authentication.HiveAuthenticationModule;
import com.facebook.presto.hive.gcs.HiveGcsModule;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.s3.HiveS3Module;
import com.facebook.presto.plugin.base.security.AllowAllAccessControl;
import com.facebook.presto.spi.ConnectorSystemConfig;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeNodePartitioningProvider;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.procedure.BaseProcedure;
import com.facebook.presto.spi.procedure.ProcedureRegistry;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class InternalIcebergConnectorFactory
{
    private InternalIcebergConnectorFactory() {}

    public static Connector createConnector(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Optional<ExtendedHiveMetastore> metastore,
            MBeanServer mBeanServer)
    {
        ClassLoader classLoader = InternalIcebergConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new EventModule(),
                    new MBeanModule(),
                    new JsonModule(),
                    new IcebergCommonModule(catalogName),
                    new IcebergCatalogModule(catalogName, metastore),
                    new HiveS3Module(catalogName),
                    new HiveGcsModule(),
                    new HiveAuthenticationModule(),
                    new CachingModule(),
                    new HiveCommonModule(),
                    binder -> {
                        binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(mBeanServer));
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(ProcedureRegistry.class).toInstance(context.getProcedureRegistry());
                        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                        binder.bind(PageSorter.class).toInstance(context.getPageSorter());
                        binder.bind(StandardFunctionResolution.class).toInstance(context.getStandardFunctionResolution());
                        binder.bind(FunctionMetadataManager.class).toInstance(context.getFunctionMetadataManager());
                        binder.bind(RowExpressionService.class).toInstance(context.getRowExpressionService());
                        binder.bind(FilterStatsCalculatorService.class).toInstance(context.getFilterStatsCalculatorService());
                        binder.bind(ConnectorSystemConfig.class).toInstance(context.getConnectorSystemConfig());
                    });

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            IcebergTransactionManager transactionManager = injector.getInstance(IcebergTransactionManager.class);
            IcebergMetadataFactory metadataFactory = injector.getInstance(IcebergMetadataFactory.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorPageSinkProvider pageSinkProvider = injector.getInstance(ConnectorPageSinkProvider.class);
            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(ConnectorNodePartitioningProvider.class);
            IcebergSessionProperties icebergSessionProperties = injector.getInstance(IcebergSessionProperties.class);
            HiveCommonSessionProperties hiveCommonSessionProperties = injector.getInstance(HiveCommonSessionProperties.class);
            IcebergTableProperties icebergTableProperties = injector.getInstance(IcebergTableProperties.class);
            Set<BaseProcedure<?>> procedures =
                    injector.getInstance(Key.get(new TypeLiteral<Set<BaseProcedure<?>>>() {}));
            ConnectorPlanOptimizerProvider planOptimizerProvider = injector.getInstance(ConnectorPlanOptimizerProvider.class);

            List<PropertyMetadata<?>> allSessionProperties = new ArrayList<>(icebergSessionProperties.getSessionProperties());
            allSessionProperties.addAll(hiveCommonSessionProperties.getSessionProperties());

            return new IcebergConnector(
                    lifeCycleManager,
                    transactionManager,
                    metadataFactory,
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
                    new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                    ImmutableSet.of(),
                    allSessionProperties,
                    SchemaProperties.SCHEMA_PROPERTIES,
                    icebergTableProperties.getTableProperties(),
                    icebergTableProperties.getColumnProperties(),
                    new AllowAllAccessControl(),
                    procedures,
                    planOptimizerProvider);
        }
    }
}
