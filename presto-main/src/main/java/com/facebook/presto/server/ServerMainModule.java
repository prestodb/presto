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
package com.facebook.presto.server;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.discovery.client.ServiceAnnouncement;
import com.facebook.airlift.http.server.TheServlet;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.stats.GcMonitor;
import com.facebook.airlift.stats.JmxGcMonitor;
import com.facebook.airlift.stats.PauseMeter;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.drift.client.ExceptionClassification;
import com.facebook.drift.client.address.AddressSelector;
import com.facebook.drift.codec.utils.DefaultThriftCodecsModule;
import com.facebook.drift.transport.netty.client.DriftNettyClientModule;
import com.facebook.drift.transport.netty.server.DriftNettyServerModule;
import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.catalogserver.CatalogServerClient;
import com.facebook.presto.catalogserver.RandomCatalogServerAddressSelector;
import com.facebook.presto.catalogserver.RemoteMetadataManager;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.ConnectorCodecManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.connector.system.SystemConnectorModule;
import com.facebook.presto.cost.FilterStatsCalculator;
import com.facebook.presto.cost.HistoryBasedOptimizationConfig;
import com.facebook.presto.cost.HistoryBasedPlanStatisticsManager;
import com.facebook.presto.cost.ScalarStatsCalculator;
import com.facebook.presto.cost.StatsNormalizer;
import com.facebook.presto.event.SplitMonitor;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.ExplainAnalyzeContext;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.MemoryRevokingScheduler;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.SqlTaskManager;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagementExecutor;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.TaskThresholdMemoryRevokingScheduler;
import com.facebook.presto.execution.buffer.SpoolingOutputBufferFactory;
import com.facebook.presto.execution.executor.MultilevelSplitQueue;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.execution.scheduler.FlatNetworkTopology;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.NodeSchedulerExporter;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.execution.scheduler.nodeSelection.SimpleTtlNodeSelectorConfig;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.memory.LocalMemoryManagerExporter;
import com.facebook.presto.memory.MemoryInfo;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.MemoryPoolAssignmentsRequest;
import com.facebook.presto.memory.MemoryResource;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.memory.ReservedSystemMemoryConfig;
import com.facebook.presto.metadata.AnalyzePropertyManager;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.ColumnPropertyManager;
import com.facebook.presto.metadata.DiscoveryNodeManager;
import com.facebook.presto.metadata.ForNodeManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.SessionPropertyProviderConfig;
import com.facebook.presto.metadata.StaticCatalogStore;
import com.facebook.presto.metadata.StaticCatalogStoreConfig;
import com.facebook.presto.metadata.StaticFunctionNamespaceStore;
import com.facebook.presto.metadata.StaticFunctionNamespaceStoreConfig;
import com.facebook.presto.metadata.StaticTypeManagerStore;
import com.facebook.presto.metadata.StaticTypeManagerStoreConfig;
import com.facebook.presto.metadata.TableFunctionRegistry;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.nodeManager.PluginNodeManager;
import com.facebook.presto.operator.ExchangeClientConfig;
import com.facebook.presto.operator.ExchangeClientFactory;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.operator.FileFragmentResultCacheConfig;
import com.facebook.presto.operator.FileFragmentResultCacheManager;
import com.facebook.presto.operator.ForExchange;
import com.facebook.presto.operator.FragmentCacheStats;
import com.facebook.presto.operator.FragmentResultCacheManager;
import com.facebook.presto.operator.HttpAndThriftRpcShuffleClientProvider;
import com.facebook.presto.operator.HttpShuffleClientProvider;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.NoOpFragmentResultCacheManager;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.RpcShuffleClientProvider;
import com.facebook.presto.operator.TableCommitContext;
import com.facebook.presto.operator.TaskMemoryReservationSummary;
import com.facebook.presto.operator.ThriftShuffleClientProvider;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.resourcemanager.ClusterMemoryManagerService;
import com.facebook.presto.resourcemanager.ClusterQueryTrackerService;
import com.facebook.presto.resourcemanager.ClusterStatusSender;
import com.facebook.presto.resourcemanager.ForResourceManager;
import com.facebook.presto.resourcemanager.NoopResourceGroupService;
import com.facebook.presto.resourcemanager.RaftConfig;
import com.facebook.presto.resourcemanager.RandomResourceManagerAddressSelector;
import com.facebook.presto.resourcemanager.ResourceGroupService;
import com.facebook.presto.resourcemanager.ResourceManagerClient;
import com.facebook.presto.resourcemanager.ResourceManagerClusterStatusSender;
import com.facebook.presto.resourcemanager.ResourceManagerConfig;
import com.facebook.presto.resourcemanager.ResourceManagerInconsistentException;
import com.facebook.presto.resourcemanager.ResourceManagerResourceGroupService;
import com.facebook.presto.server.remotetask.HttpLocationFactory;
import com.facebook.presto.server.remotetask.ReactorNettyHttpClientConfig;
import com.facebook.presto.server.thrift.FixedAddressSelector;
import com.facebook.presto.server.thrift.HandleThriftModule;
import com.facebook.presto.server.thrift.ThriftServerInfoClient;
import com.facebook.presto.server.thrift.ThriftServerInfoService;
import com.facebook.presto.server.thrift.ThriftTaskClient;
import com.facebook.presto.server.thrift.ThriftTaskService;
import com.facebook.presto.server.thrift.ThriftTaskUpdateRequestBodyReader;
import com.facebook.presto.sessionpropertyproviders.JavaWorkerSessionPropertyProvider;
import com.facebook.presto.sessionpropertyproviders.NativeWorkerSessionPropertyProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.plan.SimplePlanFragment;
import com.facebook.presto.spi.plan.SimplePlanFragmentSerde;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.session.WorkerSessionPropertyProvider;
import com.facebook.presto.spiller.FileSingleStreamSpillerFactory;
import com.facebook.presto.spiller.GenericPartitioningSpillerFactory;
import com.facebook.presto.spiller.GenericSpillerFactory;
import com.facebook.presto.spiller.LocalSpillManager;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.spiller.SpillerStats;
import com.facebook.presto.spiller.StandaloneSpillerFactory;
import com.facebook.presto.spiller.TempStorageSingleStreamSpillerFactory;
import com.facebook.presto.spiller.TempStorageStandaloneSpillerFactory;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSinkProvider;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.Serialization.ExpressionDeserializer;
import com.facebook.presto.sql.Serialization.ExpressionSerializer;
import com.facebook.presto.sql.Serialization.FunctionCallDeserializer;
import com.facebook.presto.sql.Serialization.VariableReferenceExpressionDeserializer;
import com.facebook.presto.sql.Serialization.VariableReferenceExpressionSerializer;
import com.facebook.presto.sql.SqlEnvironmentConfig;
import com.facebook.presto.sql.analyzer.AnalyzerProviderManager;
import com.facebook.presto.sql.analyzer.BuiltInAnalyzerProvider;
import com.facebook.presto.sql.analyzer.BuiltInQueryAnalyzer;
import com.facebook.presto.sql.analyzer.BuiltInQueryPreparer;
import com.facebook.presto.sql.analyzer.BuiltInQueryPreparerProvider;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig.SingleStreamSpillerChoice;
import com.facebook.presto.sql.analyzer.ForMetadataExtractor;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.analyzer.JavaFeaturesConfig;
import com.facebook.presto.sql.analyzer.MetadataExtractor;
import com.facebook.presto.sql.analyzer.MetadataExtractorMBean;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.QueryPreparerProviderManager;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.sql.gen.RowExpressionPredicateCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.JsonCodecSimplePlanFragmentSerde;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.facebook.presto.sql.planner.sanity.PlanCheckerProviderManager;
import com.facebook.presto.sql.planner.sanity.PlanCheckerProviderManagerConfig;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.statusservice.NodeStatusService;
import com.facebook.presto.tracing.TracerProviderManager;
import com.facebook.presto.tracing.TracingConfig;
import com.facebook.presto.transaction.TransactionManagerConfig;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.util.FinalizerService;
import com.facebook.presto.util.GcStatusMonitor;
import com.facebook.presto.version.EmbedVersion;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.airlift.slice.Slice;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import jakarta.servlet.Filter;
import jakarta.servlet.Servlet;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.facebook.airlift.concurrent.ConcurrentScheduledExecutor.createConcurrentScheduledExecutor;
import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.airlift.json.smile.SmileCodecBinder.smileCodecBinder;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.drift.client.ExceptionClassification.HostStatus.DOWN;
import static com.facebook.drift.client.ExceptionClassification.HostStatus.NORMAL;
import static com.facebook.drift.client.guice.DriftClientBinder.driftClientBinder;
import static com.facebook.drift.codec.guice.ThriftCodecBinder.thriftCodecBinder;
import static com.facebook.drift.server.guice.DriftServerBinder.driftServerBinder;
import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.NetworkTopologyType.FLAT;
import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.NetworkTopologyType.LEGACY;
import static com.facebook.presto.server.ServerConfig.POOL_TYPE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ServerMainModule
        extends AbstractConfigurationAwareModule
{
    private final SqlParserOptions sqlParserOptions;

    public ServerMainModule(SqlParserOptions sqlParserOptions)
    {
        requireNonNull(sqlParserOptions, "sqlParserOptions is null");
        this.sqlParserOptions = SqlParserOptions.copyOf(sqlParserOptions);
    }

    @Override
    protected void setup(Binder binder)
    {
        ServerConfig serverConfig = buildConfigObject(ServerConfig.class);

        if (serverConfig.isResourceManager()) {
            install(new ResourceManagerModule());
        }
        else if (serverConfig.isCatalogServer()) {
            install(new CatalogServerModule());
        }
        else if (serverConfig.isCoordinator()) {
            install(new CoordinatorModule());
        }
        else {
            install(new WorkerModule());
        }

        install(new InternalCommunicationModule());

        configBinder(binder).bindConfig(FeaturesConfig.class);
        configBinder(binder).bindConfig(FunctionsConfig.class);
        configBinder(binder).bindConfig(JavaFeaturesConfig.class);

        binder.bind(PlanChecker.class).in(Scopes.SINGLETON);

        binder.bind(SqlParser.class).in(Scopes.SINGLETON);
        binder.bind(SqlParserOptions.class).toInstance(sqlParserOptions);
        sqlParserOptions.useEnhancedErrorHandler(serverConfig.isEnhancedErrorReporting());

        // Metadata Extractor
        binder.bind(ExecutorService.class).annotatedWith(ForMetadataExtractor.class)
                .toInstance(newCachedThreadPool(threadsNamed("metadata-extractor-%s")));
        binder.bind(MetadataExtractorMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(MetadataExtractorMBean.class).as(generatedNameOf(MetadataExtractor.class));

        // analyzer
        binder.bind(BuiltInQueryPreparer.class).in(Scopes.SINGLETON);
        binder.bind(BuiltInQueryPreparerProvider.class).in(Scopes.SINGLETON);
        binder.bind(QueryPreparerProviderManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, QueryExplainer.class);
        binder.bind(BuiltInQueryAnalyzer.class).in(Scopes.SINGLETON);
        binder.bind(BuiltInAnalyzerProvider.class).in(Scopes.SINGLETON);
        binder.bind(AnalyzerProviderManager.class).in(Scopes.SINGLETON);

        jaxrsBinder(binder).bind(ThrowableMapper.class);

        configBinder(binder).bindConfig(QueryManagerConfig.class);

        configBinder(binder).bindConfig(SqlEnvironmentConfig.class);

        jsonCodecBinder(binder).bindJsonCodec(ViewDefinition.class);

        newOptionalBinder(binder, ExplainAnalyzeContext.class);

        // GC Monitor
        binder.bind(GcMonitor.class).to(JmxGcMonitor.class).in(Scopes.SINGLETON);

        // session properties
        binder.bind(SessionPropertyManager.class).in(Scopes.SINGLETON);
        binder.bind(SystemSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(SessionPropertyDefaults.class).in(Scopes.SINGLETON);

        // expression manager
        binder.bind(ExpressionOptimizerManager.class).in(Scopes.SINGLETON);

        // schema properties
        binder.bind(SchemaPropertyManager.class).in(Scopes.SINGLETON);

        // table properties
        binder.bind(TablePropertyManager.class).in(Scopes.SINGLETON);

        // column properties
        binder.bind(ColumnPropertyManager.class).in(Scopes.SINGLETON);

        // analyze properties
        binder.bind(AnalyzePropertyManager.class).in(Scopes.SINGLETON);

        // node manager
        discoveryBinder(binder).bindSelector("presto");
        binder.bind(DiscoveryNodeManager.class).in(Scopes.SINGLETON);
        binder.bind(InternalNodeManager.class).to(DiscoveryNodeManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(DiscoveryNodeManager.class).withGeneratedName();
        httpClientBinder(binder).bindHttpClient("node-manager", ForNodeManager.class)
                .withTracing()
                .withConfigDefaults(config -> {
                    config.setRequestTimeout(new Duration(10, SECONDS));
                });
        driftClientBinder(binder).bindDriftClient(ThriftServerInfoClient.class, ForNodeManager.class)
                .withAddressSelector(((addressSelectorBinder, annotation, prefix) ->
                        addressSelectorBinder.bind(AddressSelector.class).annotatedWith(annotation).to(FixedAddressSelector.class)));

        // node scheduler
        // TODO: remove from NodePartitioningManager and move to CoordinatorModule
        configBinder(binder).bindConfig(NodeSchedulerConfig.class);
        configBinder(binder).bindConfig(SimpleTtlNodeSelectorConfig.class);
        binder.bind(NodeScheduler.class).in(Scopes.SINGLETON);
        binder.bind(NodeSelectionStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(NodeSelectionStats.class).withGeneratedName();
        binder.bind(NodeSchedulerExporter.class).in(Scopes.SINGLETON);
        binder.bind(NodeTaskMap.class).in(Scopes.SINGLETON);
        newExporter(binder).export(NodeScheduler.class).withGeneratedName();

        // network topology
        // TODO: move to CoordinatorModule when NodeScheduler is moved
        install(installModuleIf(
                NodeSchedulerConfig.class,
                config -> LEGACY.equalsIgnoreCase(config.getNetworkTopology()),
                moduleBinder -> moduleBinder.bind(NetworkTopology.class).to(LegacyNetworkTopology.class).in(Scopes.SINGLETON)));
        install(installModuleIf(
                NodeSchedulerConfig.class,
                config -> FLAT.equalsIgnoreCase(config.getNetworkTopology()),
                moduleBinder -> moduleBinder.bind(NetworkTopology.class).to(FlatNetworkTopology.class).in(Scopes.SINGLETON)));

        // task execution
        jaxrsBinder(binder).bind(TaskResource.class);
        jaxrsBinder(binder).bind(ThriftTaskUpdateRequestBodyReader.class);

        newExporter(binder).export(TaskResource.class).withGeneratedName();
        jaxrsBinder(binder).bind(TaskExecutorResource.class);
        newExporter(binder).export(TaskExecutorResource.class).withGeneratedName();
        binder.bind(TaskManagementExecutor.class).in(Scopes.SINGLETON);

        install(new DefaultThriftCodecsModule());
        // handle resolve for thrift
        binder.install(new HandleThriftModule());

        thriftCodecBinder(binder).bindCustomThriftCodec(SqlInvokedFunctionCodec.class);
        thriftCodecBinder(binder).bindCustomThriftCodec(SqlFunctionIdCodec.class);

        binder.bind(ConnectorCodecManager.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindListJsonCodec(TaskMemoryReservationSummary.class);
        binder.bind(SqlTaskManager.class).in(Scopes.SINGLETON);
        binder.bind(TaskManager.class).to(Key.get(SqlTaskManager.class));
        binder.bind(SpoolingOutputBufferFactory.class).in(Scopes.SINGLETON);

        binder.bind(RandomResourceManagerAddressSelector.class).in(Scopes.SINGLETON);
        driftClientBinder(binder)
                .bindDriftClient(ResourceManagerClient.class, ForResourceManager.class)
                .withAddressSelector((addressSelectorBinder, annotation, prefix) ->
                        addressSelectorBinder.bind(AddressSelector.class).annotatedWith(annotation).to(RandomResourceManagerAddressSelector.class))
                .withExceptionClassifier(throwable -> {
                    if (throwable instanceof ResourceManagerInconsistentException) {
                        return new ExceptionClassification(Optional.of(true), DOWN);
                    }
                    return new ExceptionClassification(Optional.of(true), NORMAL);
                });

        binder.bind(RandomCatalogServerAddressSelector.class).in(Scopes.SINGLETON);
        driftClientBinder(binder)
                .bindDriftClient(CatalogServerClient.class)
                .withAddressSelector((addressSelectorBinder, annotation, prefix) ->
                        addressSelectorBinder.bind(AddressSelector.class).annotatedWith(annotation).to(RandomCatalogServerAddressSelector.class));

        newOptionalBinder(binder, ClusterMemoryManagerService.class);
        newOptionalBinder(binder, ClusterQueryTrackerService.class);
        install(installModuleIf(
                ServerConfig.class,
                ServerConfig::isResourceManagerEnabled,
                new Module()
                {
                    @Override
                    public void configure(Binder moduleBinder)
                    {
                        configBinder(moduleBinder).bindConfig(ResourceManagerConfig.class);
                        // HTTP endpoint for some of ResourceManagerServer methods.
                        ResourceManagerConfig resourceManagerConfig = buildConfigObject(ResourceManagerConfig.class);
                        if (resourceManagerConfig.getHeartbeatHttpEnabled()) {
                            jaxrsBinder(moduleBinder).bind(ResourceManagerHeartbeatResource.class);
                        }
                        moduleBinder.bind(ClusterStatusSender.class).to(ResourceManagerClusterStatusSender.class).in(Scopes.SINGLETON);
                        if (serverConfig.isCoordinator()) {
                            moduleBinder.bind(ClusterMemoryManagerService.class).in(Scopes.SINGLETON);
                            moduleBinder.bind(ClusterQueryTrackerService.class).in(Scopes.SINGLETON);
                            moduleBinder.bind(ResourceGroupService.class).to(ResourceManagerResourceGroupService.class).in(Scopes.SINGLETON);
                        }
                    }

                    @Provides
                    @Singleton
                    @ForResourceManager
                    public ScheduledExecutorService createResourceManagerScheduledExecutor(ResourceManagerConfig config)
                    {
                        return createConcurrentScheduledExecutor("resource-manager-heartbeats", config.getHeartbeatConcurrency(), config.getHeartbeatThreads());
                    }

                    @Provides
                    @Singleton
                    @ForResourceManager
                    public ListeningExecutorService createResourceManagerExecutor(ResourceManagerConfig config)
                    {
                        ExecutorService executor = new ThreadPoolExecutor(
                                0,
                                config.getResourceManagerExecutorThreads(),
                                60,
                                SECONDS,
                                new LinkedBlockingQueue<>(),
                                daemonThreadsNamed("resource-manager-executor-%s"));
                        return listeningDecorator(executor);
                    }
                },
                moduleBinder -> {
                    moduleBinder.bind(ClusterStatusSender.class).toInstance(execution -> {});
                    moduleBinder.bind(ResourceGroupService.class).to(NoopResourceGroupService.class).in(Scopes.SINGLETON);
                }));

        FeaturesConfig featuresConfig = buildConfigObject(FeaturesConfig.class);
        FeaturesConfig.TaskSpillingStrategy taskSpillingStrategy = featuresConfig.getTaskSpillingStrategy();
        switch (taskSpillingStrategy) {
            case PER_TASK_MEMORY_THRESHOLD:
                binder.bind(TaskThresholdMemoryRevokingScheduler.class).in(Scopes.SINGLETON);
                break;
            default:
                binder.bind(MemoryRevokingScheduler.class).in(Scopes.SINGLETON);
        }

        // Add monitoring for JVM pauses
        binder.bind(PauseMeter.class).in(Scopes.SINGLETON);
        newExporter(binder).export(PauseMeter.class).withGeneratedName();
        binder.bind(GcStatusMonitor.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(MemoryManagerConfig.class);
        configBinder(binder).bindConfig(NodeMemoryConfig.class);
        configBinder(binder).bindConfig(ReservedSystemMemoryConfig.class);
        binder.bind(LocalMemoryManager.class).in(Scopes.SINGLETON);
        binder.bind(LocalMemoryManagerExporter.class).in(Scopes.SINGLETON);
        binder.bind(EmbedVersion.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TaskManager.class).withGeneratedName();
        binder.bind(TaskExecutor.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TaskExecutor.class).withGeneratedName();
        binder.bind(MultilevelSplitQueue.class).in(Scopes.SINGLETON);
        newExporter(binder).export(MultilevelSplitQueue.class).withGeneratedName();
        binder.bind(LocalExecutionPlanner.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(FileFragmentResultCacheConfig.class);
        binder.bind(FragmentCacheStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FragmentCacheStats.class).withGeneratedName();
        configBinder(binder).bindConfig(CompilerConfig.class);
        binder.bind(ExpressionCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ExpressionCompiler.class).withGeneratedName();
        binder.bind(PageFunctionCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(PageFunctionCompiler.class).withGeneratedName();
        configBinder(binder).bindConfig(TaskManagerConfig.class);
        configBinder(binder).bindConfig(ReactorNettyHttpClientConfig.class);
        binder.bind(IndexJoinLookupStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IndexJoinLookupStats.class).withGeneratedName();
        binder.bind(AsyncHttpExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(AsyncHttpExecutionMBean.class).withGeneratedName();
        binder.bind(JoinFilterFunctionCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(JoinFilterFunctionCompiler.class).withGeneratedName();
        binder.bind(JoinCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(JoinCompiler.class).withGeneratedName();
        binder.bind(OrderingCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(OrderingCompiler.class).withGeneratedName();
        binder.bind(PagesIndex.Factory.class).to(PagesIndex.DefaultFactory.class);
        binder.bind(LookupJoinOperators.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(TaskStatus.class);
        jsonCodecBinder(binder).bindJsonCodec(StageInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(OperatorStats.class);
        jsonCodecBinder(binder).bindJsonCodec(ExecutionFailureInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(TableCommitContext.class);
        jsonCodecBinder(binder).bindJsonCodec(SqlInvokedFunction.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskSource.class);
        jsonCodecBinder(binder).bindJsonCodec(TableWriteInfo.class);
        smileCodecBinder(binder).bindSmileCodec(TaskStatus.class);
        smileCodecBinder(binder).bindSmileCodec(TaskInfo.class);
        thriftCodecBinder(binder).bindThriftCodec(TaskStatus.class);
        thriftCodecBinder(binder).bindThriftCodec(TaskInfo.class);

        // exchange client
        binder.bind(RpcShuffleClientProvider.class)
                .annotatedWith(ForExchange.class)
                .to(HttpAndThriftRpcShuffleClientProvider.class);
        binder.bind(HttpShuffleClientProvider.class)
                .annotatedWith(ForExchange.class)
                .to(HttpShuffleClientProvider.class);
        binder.bind(ThriftShuffleClientProvider.class)
                .annotatedWith(ForExchange.class)
                .to(ThriftShuffleClientProvider.class);
        binder.bind(ExchangeClientSupplier.class).to(ExchangeClientFactory.class).in(Scopes.SINGLETON);

        httpClientBinder(binder).bindHttpClient("exchange", ForExchange.class)
                .withTracing()
                .withFilter(GenerateTraceTokenRequestFilter.class)
                .withConfigDefaults(config -> {
                    config.setRequestTimeout(new Duration(10, SECONDS));
                    config.setMaxConnectionsPerServer(250);
                    config.setMaxContentLength(new DataSize(32, MEGABYTE));
                });

        binder.install(new DriftNettyClientModule());
        driftClientBinder(binder).bindDriftClient(ThriftTaskClient.class, ForExchange.class)
                .withAddressSelector(((addressSelectorBinder, annotation, prefix) ->
                        addressSelectorBinder.bind(AddressSelector.class).annotatedWith(annotation).to(FixedAddressSelector.class)));

        configBinder(binder).bindConfig(ExchangeClientConfig.class);
        binder.bind(ExchangeExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ExchangeExecutionMBean.class).withGeneratedName();

        // execution
        binder.bind(LocationFactory.class).to(HttpLocationFactory.class).in(Scopes.SINGLETON);

        // memory manager
        jaxrsBinder(binder).bind(MemoryResource.class);

        jsonCodecBinder(binder).bindJsonCodec(MemoryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(MemoryPoolAssignmentsRequest.class);
        smileCodecBinder(binder).bindSmileCodec(MemoryInfo.class);
        smileCodecBinder(binder).bindSmileCodec(MemoryPoolAssignmentsRequest.class);

        // transaction manager
        configBinder(binder).bindConfig(TransactionManagerConfig.class);

        // data stream provider
        binder.bind(PageSourceManager.class).in(Scopes.SINGLETON);
        binder.bind(PageSourceProvider.class).to(PageSourceManager.class).in(Scopes.SINGLETON);

        // page sink provider
        binder.bind(PageSinkManager.class).in(Scopes.SINGLETON);
        binder.bind(PageSinkProvider.class).to(PageSinkManager.class).in(Scopes.SINGLETON);

        // metadata
        binder.bind(StaticCatalogStore.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(StaticCatalogStoreConfig.class);
        binder.bind(StaticFunctionNamespaceStore.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(StaticFunctionNamespaceStoreConfig.class);
        binder.bind(StaticTypeManagerStore.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(StaticTypeManagerStoreConfig.class);
        configBinder(binder).bindConfig(SessionPropertyProviderConfig.class);
        binder.bind(FunctionAndTypeManager.class).in(Scopes.SINGLETON);
        binder.bind(TableFunctionRegistry.class).in(Scopes.SINGLETON);
        binder.bind(MetadataManager.class).in(Scopes.SINGLETON);

        if (serverConfig.isCatalogServerEnabled() && serverConfig.isCoordinator()) {
            binder.bind(RemoteMetadataManager.class).in(Scopes.SINGLETON);
            binder.bind(Metadata.class).to(RemoteMetadataManager.class).in(Scopes.SINGLETON);
        }
        else {
            binder.bind(Metadata.class).to(MetadataManager.class).in(Scopes.SINGLETON);
        }

        // row expression utils
        binder.bind(DomainTranslator.class).to(RowExpressionDomainTranslator.class).in(Scopes.SINGLETON);
        binder.bind(PredicateCompiler.class).to(RowExpressionPredicateCompiler.class).in(Scopes.SINGLETON);
        binder.bind(DeterminismEvaluator.class).to(RowExpressionDeterminismEvaluator.class).in(Scopes.SINGLETON);

        // type
        binder.bind(TypeManager.class).to(FunctionAndTypeManager.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        newSetBinder(binder, Type.class);

        // plan
        jsonBinder(binder).addKeySerializerBinding(VariableReferenceExpression.class).to(VariableReferenceExpressionSerializer.class);
        jsonBinder(binder).addKeyDeserializerBinding(VariableReferenceExpression.class).to(VariableReferenceExpressionDeserializer.class);
        jsonCodecBinder(binder).bindJsonCodec(SimplePlanFragment.class);
        binder.bind(SimplePlanFragmentSerde.class).to(JsonCodecSimplePlanFragmentSerde.class).in(Scopes.SINGLETON);

        // history statistics
        configBinder(binder).bindConfig(HistoryBasedOptimizationConfig.class);
        binder.bind(HistoryBasedPlanStatisticsManager.class).in(Scopes.SINGLETON);

        // split manager
        binder.bind(SplitManager.class).in(Scopes.SINGLETON);

        // partitioning provider manager
        binder.bind(PartitioningProviderManager.class).in(Scopes.SINGLETON);

        // node partitioning manager
        binder.bind(NodePartitioningManager.class).in(Scopes.SINGLETON);

        // connector plan optimizer manager
        binder.bind(ConnectorPlanOptimizerManager.class).in(Scopes.SINGLETON);

        // index manager
        binder.bind(IndexManager.class).in(Scopes.SINGLETON);

        // handle resolver
        binder.install(new HandleJsonModule());
        binder.bind(ObjectMapper.class).toProvider(JsonObjectMapperProvider.class);

        // connector
        binder.bind(ScalarStatsCalculator.class).in(Scopes.SINGLETON);
        binder.bind(StatsNormalizer.class).in(Scopes.SINGLETON);
        binder.bind(FilterStatsCalculator.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorManager.class).in(Scopes.SINGLETON);

        // system connector
        binder.install(new SystemConnectorModule());

        // splits
        jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(ConnectorSplit.class);
        jsonCodecBinder(binder).bindJsonCodec(PlanFragment.class);
        smileCodecBinder(binder).bindSmileCodec(TaskUpdateRequest.class);
        smileCodecBinder(binder).bindSmileCodec(ConnectorSplit.class);
        smileCodecBinder(binder).bindSmileCodec(PlanFragment.class);
        jsonBinder(binder).addSerializerBinding(Slice.class).to(SliceSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Slice.class).to(SliceDeserializer.class);
        jsonBinder(binder).addSerializerBinding(Expression.class).to(ExpressionSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Expression.class).to(ExpressionDeserializer.class);
        jsonBinder(binder).addDeserializerBinding(FunctionCall.class).to(FunctionCallDeserializer.class);
        thriftCodecBinder(binder).bindThriftCodec(TaskUpdateRequest.class);

        // split monitor
        binder.bind(SplitMonitor.class).in(Scopes.SINGLETON);

        // Determine the NodeVersion
        NodeVersion nodeVersion = new NodeVersion(serverConfig.getPrestoVersion());
        binder.bind(NodeVersion.class).toInstance(nodeVersion);

        // presto announcement
        checkArgument(!(serverConfig.isResourceManager() && serverConfig.isCoordinator()),
                "Server cannot be configured as both resource manager and coordinator");

        checkArgument(!(serverConfig.isCatalogServer() && serverConfig.isCoordinator()),
                "Server cannot be configured as both catalog server and coordinator");

        ServiceAnnouncement.ServiceAnnouncementBuilder serviceAnnouncementBuilder = discoveryBinder(binder).bindHttpAnnouncement("presto")
                .addProperty("node_version", nodeVersion.toString())
                .addProperty("coordinator", String.valueOf(serverConfig.isCoordinator()))
                .addProperty("resource_manager", String.valueOf(serverConfig.isResourceManager()))
                .addProperty("catalog_server", String.valueOf(serverConfig.isCatalogServer()))
                .addProperty("connectorIds", nullToEmpty(serverConfig.getDataSources()))
                .addProperty(POOL_TYPE, serverConfig.getPoolType().name());

        RaftConfig raftConfig = buildConfigObject(RaftConfig.class);
        if (serverConfig.isResourceManager() && raftConfig.isEnabled()) {
            serviceAnnouncementBuilder.addProperty("raftPort", String.valueOf(raftConfig.getPort()));
        }

        // server info resource
        jaxrsBinder(binder).bind(ServerInfoResource.class);
        jsonCodecBinder(binder).bindJsonCodec(ServerInfo.class);

        // node status resource
        jaxrsBinder(binder).bind(StatusResource.class);
        jsonCodecBinder(binder).bindJsonCodec(NodeStatus.class);

        // plugin manager
        binder.bind(PluginManager.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(PluginManagerConfig.class);

        binder.bind(CatalogManager.class).in(Scopes.SINGLETON);

        // block encodings
        binder.bind(BlockEncodingManager.class).in(Scopes.SINGLETON);
        binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, BlockEncoding.class);
        jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
        jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);

        // thread visualizer
        jaxrsBinder(binder).bind(ThreadResource.class);

        // PageSorter
        binder.bind(PageSorter.class).to(PagesIndexPageSorter.class).in(Scopes.SINGLETON);

        // PageIndexer
        binder.bind(PageIndexerFactory.class).to(GroupByHashPageIndexerFactory.class).in(Scopes.SINGLETON);

        // Finalizer
        binder.bind(FinalizerService.class).in(Scopes.SINGLETON);

        // Spiller
        binder.bind(SpillerFactory.class).to(GenericSpillerFactory.class).in(Scopes.SINGLETON);
        binder.bind(StandaloneSpillerFactory.class).to(TempStorageStandaloneSpillerFactory.class).in(Scopes.SINGLETON);
        binder.bind(PartitioningSpillerFactory.class).to(GenericPartitioningSpillerFactory.class).in(Scopes.SINGLETON);
        binder.bind(SpillerStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(SpillerStats.class).withGeneratedName();
        newExporter(binder).export(SpillerFactory.class).withGeneratedName();
        binder.bind(LocalSpillManager.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(NodeSpillConfig.class);

        install(installModuleIf(
                FeaturesConfig.class,
                config -> config.getSingleStreamSpillerChoice() == SingleStreamSpillerChoice.LOCAL_FILE,
                moduleBinder -> moduleBinder
                        .bind(SingleStreamSpillerFactory.class)
                        .to(FileSingleStreamSpillerFactory.class)
                        .in(Scopes.SINGLETON)));
        install(installModuleIf(
                FeaturesConfig.class,
                config -> config.getSingleStreamSpillerChoice() == SingleStreamSpillerChoice.TEMP_STORAGE,
                moduleBinder -> moduleBinder
                        .bind(SingleStreamSpillerFactory.class)
                        .to(TempStorageSingleStreamSpillerFactory.class)
                        .in(Scopes.SINGLETON)));

        // Thrift RPC
        binder.install(new DriftNettyServerModule());
        driftServerBinder(binder).bindService(ThriftTaskService.class);
        driftServerBinder(binder).bindService(ThriftServerInfoService.class);

        // Async page transport
        newSetBinder(binder, Filter.class, TheServlet.class).addBinding()
                .to(AsyncPageTransportForwardFilter.class).in(Scopes.SINGLETON);
        binder.bind(AsyncPageTransportServlet.class).in(Scopes.SINGLETON);
        newExporter(binder).export(AsyncPageTransportServlet.class).withGeneratedName();
        newMapBinder(binder, String.class, Servlet.class, TheServlet.class)
                .addBinding("/v1/task/async/*")
                .to(AsyncPageTransportServlet.class)
                .in(Scopes.SINGLETON);

        // cleanup
        binder.bind(ExecutorCleanup.class).in(Scopes.SINGLETON);

        // Distributed tracing
        configBinder(binder).bindConfig(TracingConfig.class);
        binder.bind(TracerProviderManager.class).in(Scopes.SINGLETON);

        //Optional Status Detector
        newOptionalBinder(binder, NodeStatusService.class);
        binder.bind(NodeStatusNotificationManager.class).in(Scopes.SINGLETON);

        binder.bind(PlanCheckerProviderManager.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(PlanCheckerProviderManagerConfig.class);

        // Worker session property providers
        MapBinder<String, WorkerSessionPropertyProvider> mapBinder =
                newMapBinder(binder, String.class, WorkerSessionPropertyProvider.class);
        if (featuresConfig.isNativeExecutionEnabled()) {
            if (!serverConfig.isCoordinatorSidecarEnabled()) {
                mapBinder.addBinding("native-worker").to(NativeWorkerSessionPropertyProvider.class).in(Scopes.SINGLETON);
            }
            if (!featuresConfig.isExcludeInvalidWorkerSessionProperties()) {
                mapBinder.addBinding("java-worker").to(JavaWorkerSessionPropertyProvider.class).in(Scopes.SINGLETON);
            }
        }
        else {
            mapBinder.addBinding("java-worker").to(JavaWorkerSessionPropertyProvider.class).in(Scopes.SINGLETON);
            if (!featuresConfig.isExcludeInvalidWorkerSessionProperties()) {
                mapBinder.addBinding("native-worker").to(NativeWorkerSessionPropertyProvider.class).in(Scopes.SINGLETON);
            }
        }

        // Node manager binding
        binder.bind(PluginNodeManager.class).in(Scopes.SINGLETON);
        binder.bind(NodeManager.class).to(PluginNodeManager.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForExchange
    public static ScheduledExecutorService createExchangeExecutor(ExchangeClientConfig config)
    {
        return newScheduledThreadPool(config.getClientThreads(), daemonThreadsNamed("exchange-client-%s"));
    }

    @Provides
    @Singleton
    @ForAsyncRpc
    public static ExecutorService createAsyncHttpResponseCoreExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("async-http-response-%s"));
    }

    @Provides
    @Singleton
    @ForAsyncRpc
    public static BoundedExecutor createAsyncHttpResponseExecutor(@ForAsyncRpc ExecutorService coreExecutor, TaskManagerConfig config)
    {
        return new BoundedExecutor(coreExecutor, config.getHttpResponseThreads());
    }

    @Provides
    @Singleton
    @ForAsyncRpc
    public static ScheduledExecutorService createAsyncHttpTimeoutExecutor(TaskManagerConfig config)
    {
        return createConcurrentScheduledExecutor("async-http-timeout", config.getHttpTimeoutConcurrency(), config.getHttpTimeoutThreads());
    }

    @Provides
    @Singleton
    public static FragmentResultCacheManager createFragmentResultCacheManager(FileFragmentResultCacheConfig config, BlockEncodingSerde blockEncodingSerde, FragmentCacheStats fragmentCacheStats)
    {
        if (config.isCachingEnabled()) {
            return new FileFragmentResultCacheManager(
                    config,
                    blockEncodingSerde,
                    fragmentCacheStats,
                    newFixedThreadPool(5, daemonThreadsNamed("fragment-result-cache-writer-%s")),
                    newFixedThreadPool(1, daemonThreadsNamed("fragment-result-cache-remover-%s")));
        }
        return new NoOpFragmentResultCacheManager();
    }

    public static class ExecutorCleanup
    {
        private final List<ExecutorService> executors;
        @Inject(optional = true)
        @ForResourceManager
        private ScheduledExecutorService resourceManagerScheduledExecutor;
        @Inject(optional = true)
        @ForResourceManager
        private ListeningExecutorService resourceManagerExecutor;

        @Inject
        public ExecutorCleanup(
                @ForExchange ScheduledExecutorService exchangeExecutor,
                @ForAsyncRpc ExecutorService httpResponseExecutor,
                @ForAsyncRpc ScheduledExecutorService httpTimeoutExecutor)
        {
            executors = ImmutableList.of(
                    exchangeExecutor,
                    httpResponseExecutor,
                    httpTimeoutExecutor);
        }

        @PreDestroy
        public void shutdown()
        {
            executors.forEach(ExecutorService::shutdownNow);
            if (resourceManagerScheduledExecutor != null) {
                resourceManagerScheduledExecutor.shutdownNow();
            }
            if (resourceManagerExecutor != null) {
                resourceManagerExecutor.shutdownNow();
            }
        }
    }
}
