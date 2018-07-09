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

import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.connector.system.SystemConnectorModule;
import com.facebook.presto.cost.AggregationStatsRule;
import com.facebook.presto.cost.AssignUniqueIdStatsRule;
import com.facebook.presto.cost.CoefficientBasedStatsCalculator;
import com.facebook.presto.cost.ComposableStatsCalculator;
import com.facebook.presto.cost.ComposableStatsCalculator.Rule;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostCalculator.EstimatedExchanges;
import com.facebook.presto.cost.CostCalculatorUsingExchanges;
import com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.EnforceSingleRowStatsRule;
import com.facebook.presto.cost.ExchangeStatsRule;
import com.facebook.presto.cost.FilterStatsCalculator;
import com.facebook.presto.cost.FilterStatsRule;
import com.facebook.presto.cost.JoinStatsRule;
import com.facebook.presto.cost.LimitStatsRule;
import com.facebook.presto.cost.OutputStatsRule;
import com.facebook.presto.cost.ProjectStatsRule;
import com.facebook.presto.cost.ScalarStatsCalculator;
import com.facebook.presto.cost.SelectingStatsCalculator;
import com.facebook.presto.cost.SelectingStatsCalculator.New;
import com.facebook.presto.cost.SemiJoinStatsRule;
import com.facebook.presto.cost.SimpleFilterProjectSemiJoinStatsRule;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsNormalizer;
import com.facebook.presto.cost.TableScanStatsRule;
import com.facebook.presto.cost.UnionStatsRule;
import com.facebook.presto.cost.ValuesStatsRule;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.event.query.QueryMonitorConfig;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.MemoryRevokingScheduler;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.QueryPerformanceFetcher;
import com.facebook.presto.execution.QueryPerformanceFetcherProvider;
import com.facebook.presto.execution.SqlTaskManager;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagementExecutor;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.executor.MultilevelSplitQueue;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.execution.resourceGroups.NoOpResourceGroupManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.execution.scheduler.FlatNetworkTopology;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.NodeSchedulerExporter;
import com.facebook.presto.failureDetector.FailureDetector;
import com.facebook.presto.failureDetector.FailureDetectorModule;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.memory.LocalMemoryManagerExporter;
import com.facebook.presto.memory.MemoryInfo;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.MemoryPoolAssignmentsRequest;
import com.facebook.presto.memory.MemoryResource;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.memory.ReservedSystemMemoryConfig;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.DiscoveryNodeManager;
import com.facebook.presto.metadata.ForNodeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.StaticCatalogStore;
import com.facebook.presto.metadata.StaticCatalogStoreConfig;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.operator.ExchangeClientConfig;
import com.facebook.presto.operator.ExchangeClientFactory;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.operator.ForExchange;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.server.remotetask.HttpLocationFactory;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spiller.FileSingleStreamSpillerFactory;
import com.facebook.presto.spiller.GenericPartitioningSpillerFactory;
import com.facebook.presto.spiller.GenericSpillerFactory;
import com.facebook.presto.spiller.LocalSpillManager;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.spiller.SpillerStats;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSinkProvider;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.Serialization.ExpressionDeserializer;
import com.facebook.presto.sql.Serialization.ExpressionSerializer;
import com.facebook.presto.sql.Serialization.FunctionCallDeserializer;
import com.facebook.presto.sql.SqlEnvironmentConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.PlanOptimizers;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.transaction.ForTransactionManager;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.transaction.TransactionManagerConfig;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.server.EmbeddedDiscoveryModule;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.slice.Slice;
import io.airlift.stats.GcMonitor;
import io.airlift.stats.JmxGcMonitor;
import io.airlift.stats.PauseMeter;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.NetworkTopologyType.FLAT;
import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.NetworkTopologyType.LEGACY;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.reflect.Reflection.newProxy;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
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

        if (serverConfig.isCoordinator()) {
            install(new CoordinatorModule());
            binder.bind(new TypeLiteral<Optional<QueryPerformanceFetcher>>() {}).toProvider(QueryPerformanceFetcherProvider.class).in(Scopes.SINGLETON);
        }
        else {
            binder.bind(new TypeLiteral<Optional<QueryPerformanceFetcher>>() {}).toInstance(Optional.empty());

            // Install no-op session supplier on workers, since only coordinators create sessions.
            binder.bind(SessionSupplier.class).to(NoOpSessionSupplier.class).in(Scopes.SINGLETON);

            // Install no-op resource group manager on workers, since only coordinators manage resource groups.
            binder.bind(ResourceGroupManager.class).to(NoOpResourceGroupManager.class).in(Scopes.SINGLETON);

            // HACK: this binding is needed by SystemConnectorModule, but will only be used on the coordinator
            binder.bind(QueryManager.class).toInstance(newProxy(QueryManager.class, (proxy, method, args) -> {
                throw new UnsupportedOperationException();
            }));
        }

        // discovery server
        // TODO: move to CoordinatorModule
        install(installModuleIf(EmbeddedDiscoveryConfig.class, EmbeddedDiscoveryConfig::isEnabled, new EmbeddedDiscoveryModule()));

        InternalCommunicationConfig internalCommunicationConfig = buildConfigObject(InternalCommunicationConfig.class);
        configBinder(binder).bindConfigGlobalDefaults(HttpClientConfig.class, config -> {
            config.setKeyStorePath(internalCommunicationConfig.getKeyStorePath());
            config.setKeyStorePassword(internalCommunicationConfig.getKeyStorePassword());
        });

        configBinder(binder).bindConfig(FeaturesConfig.class);

        binder.bind(SqlParser.class).in(Scopes.SINGLETON);
        binder.bind(SqlParserOptions.class).toInstance(sqlParserOptions);
        sqlParserOptions.useEnhancedErrorHandler(serverConfig.isEnhancedErrorReporting());

        bindFailureDetector(binder, serverConfig.isCoordinator());

        jaxrsBinder(binder).bind(ThrowableMapper.class);

        configBinder(binder).bindConfig(QueryManagerConfig.class);

        configBinder(binder).bindConfig(SqlEnvironmentConfig.class);

        jsonCodecBinder(binder).bindJsonCodec(ViewDefinition.class);

        // GC Monitor
        binder.bind(GcMonitor.class).to(JmxGcMonitor.class).in(Scopes.SINGLETON);

        // session properties
        binder.bind(SessionPropertyManager.class).in(Scopes.SINGLETON);
        binder.bind(SystemSessionProperties.class).in(Scopes.SINGLETON);

        // schema properties
        binder.bind(SchemaPropertyManager.class).in(Scopes.SINGLETON);

        // table properties
        binder.bind(TablePropertyManager.class).in(Scopes.SINGLETON);

        // node manager
        discoveryBinder(binder).bindSelector("presto");
        binder.bind(DiscoveryNodeManager.class).in(Scopes.SINGLETON);
        binder.bind(InternalNodeManager.class).to(DiscoveryNodeManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(DiscoveryNodeManager.class).withGeneratedName();
        httpClientBinder(binder).bindHttpClient("node-manager", ForNodeManager.class)
                .withTracing()
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                });

        // node scheduler
        // TODO: remove from NodePartitioningManager and move to CoordinatorModule
        configBinder(binder).bindConfig(NodeSchedulerConfig.class);
        binder.bind(NodeScheduler.class).in(Scopes.SINGLETON);
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
        newExporter(binder).export(TaskResource.class).withGeneratedName();
        binder.bind(TaskManagementExecutor.class).in(Scopes.SINGLETON);
        binder.bind(SqlTaskManager.class).in(Scopes.SINGLETON);
        binder.bind(TaskManager.class).to(Key.get(SqlTaskManager.class));

        // memory revoking scheduler
        binder.bind(MemoryRevokingScheduler.class).in(Scopes.SINGLETON);

        // Add monitoring for JVM pauses
        binder.bind(PauseMeter.class).in(Scopes.SINGLETON);
        newExporter(binder).export(PauseMeter.class).withGeneratedName();

        configBinder(binder).bindConfig(MemoryManagerConfig.class);
        configBinder(binder).bindConfig(NodeMemoryConfig.class);
        configBinder(binder).bindConfig(ReservedSystemMemoryConfig.class);
        binder.bind(LocalMemoryManager.class).in(Scopes.SINGLETON);
        binder.bind(LocalMemoryManagerExporter.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TaskManager.class).withGeneratedName();
        binder.bind(TaskExecutor.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TaskExecutor.class).withGeneratedName();
        binder.bind(MultilevelSplitQueue.class).in(Scopes.SINGLETON);
        newExporter(binder).export(MultilevelSplitQueue.class).withGeneratedName();
        binder.bind(LocalExecutionPlanner.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(CompilerConfig.class);
        binder.bind(ExpressionCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ExpressionCompiler.class).withGeneratedName();
        binder.bind(PageFunctionCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(PageFunctionCompiler.class).withGeneratedName();
        configBinder(binder).bindConfig(TaskManagerConfig.class);
        binder.bind(IndexJoinLookupStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IndexJoinLookupStats.class).withGeneratedName();
        binder.bind(StatementHttpExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(StatementHttpExecutionMBean.class).withGeneratedName();
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
        jaxrsBinder(binder).bind(PagesResponseWriter.class);

        // exchange client
        binder.bind(new TypeLiteral<ExchangeClientSupplier>() {}).to(ExchangeClientFactory.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("exchange", ForExchange.class)
                .withTracing()
                .withFilter(GenerateTraceTokenRequestFilter.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                    config.setMaxConnectionsPerServer(250);
                    config.setMaxContentLength(new DataSize(32, MEGABYTE));
                });

        configBinder(binder).bindConfig(ExchangeClientConfig.class);
        binder.bind(ExchangeExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ExchangeExecutionMBean.class).withGeneratedName();

        // execution
        binder.bind(LocationFactory.class).to(HttpLocationFactory.class).in(Scopes.SINGLETON);

        // memory manager
        jaxrsBinder(binder).bind(MemoryResource.class);

        jsonCodecBinder(binder).bindJsonCodec(MemoryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(MemoryPoolAssignmentsRequest.class);

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
        binder.bind(MetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(Metadata.class).to(MetadataManager.class).in(Scopes.SINGLETON);

        // statistics calculator
        binder.bind(StatsCalculator.class).annotatedWith(SelectingStatsCalculator.Old.class).to(CoefficientBasedStatsCalculator.class).in(Scopes.SINGLETON);
        binder.bind(StatsCalculator.class).to(SelectingStatsCalculator.class).in(Scopes.SINGLETON);

        // cost calculator
        binder.bind(CostCalculator.class).to(CostCalculatorUsingExchanges.class).in(Scopes.SINGLETON);
        binder.bind(CostCalculator.class).annotatedWith(EstimatedExchanges.class).to(CostCalculatorWithEstimatedExchanges.class).in(Scopes.SINGLETON);
        binder.bind(CostComparator.class).in(Scopes.SINGLETON);

        // type
        binder.bind(TypeRegistry.class).in(Scopes.SINGLETON);
        binder.bind(TypeManager.class).to(TypeRegistry.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        newSetBinder(binder, Type.class);

        // split manager
        binder.bind(SplitManager.class).in(Scopes.SINGLETON);

        // node partitioning manager
        binder.bind(NodePartitioningManager.class).in(Scopes.SINGLETON);

        // index manager
        binder.bind(IndexManager.class).in(Scopes.SINGLETON);

        // handle resolver
        binder.install(new HandleJsonModule());

        // connector
        binder.bind(ConnectorManager.class).in(Scopes.SINGLETON);

        // system connector
        binder.install(new SystemConnectorModule());

        // splits
        jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(ConnectorSplit.class);
        jsonBinder(binder).addSerializerBinding(Slice.class).to(SliceSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Slice.class).to(SliceDeserializer.class);
        jsonBinder(binder).addSerializerBinding(Expression.class).to(ExpressionSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Expression.class).to(ExpressionDeserializer.class);
        jsonBinder(binder).addDeserializerBinding(FunctionCall.class).to(FunctionCallDeserializer.class);

        // query monitor
        configBinder(binder).bindConfig(QueryMonitorConfig.class);
        binder.bind(QueryMonitor.class).in(Scopes.SINGLETON);

        // Determine the NodeVersion
        NodeVersion nodeVersion = new NodeVersion(serverConfig.getPrestoVersion());
        binder.bind(NodeVersion.class).toInstance(nodeVersion);

        // presto announcement
        discoveryBinder(binder).bindHttpAnnouncement("presto")
                .addProperty("node_version", nodeVersion.toString())
                .addProperty("coordinator", String.valueOf(serverConfig.isCoordinator()))
                .addProperty("connectorIds", nullToEmpty(serverConfig.getDataSources()));

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

        // optimizers
        binder.bind(PlanOptimizers.class).in(Scopes.SINGLETON);

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
        binder.bind(SingleStreamSpillerFactory.class).to(FileSingleStreamSpillerFactory.class).in(Scopes.SINGLETON);
        binder.bind(PartitioningSpillerFactory.class).to(GenericPartitioningSpillerFactory.class).in(Scopes.SINGLETON);
        binder.bind(SpillerStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(SpillerFactory.class).withGeneratedName();
        binder.bind(LocalSpillManager.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(NodeSpillConfig.class);

        // cleanup
        binder.bind(ExecutorCleanup.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @New
    public static StatsCalculator createNewStatsCalculator(Metadata metadata)
    {
        StatsNormalizer normalizer = new StatsNormalizer();
        ScalarStatsCalculator scalarStatsCalculator = new ScalarStatsCalculator(metadata);
        FilterStatsCalculator filterStatsCalculator = new FilterStatsCalculator(metadata, scalarStatsCalculator, normalizer);

        ImmutableList.Builder<Rule<?>> rules = ImmutableList.builder();
        rules.add(new OutputStatsRule());
        rules.add(new TableScanStatsRule(metadata, normalizer));
        rules.add(new SimpleFilterProjectSemiJoinStatsRule(normalizer, filterStatsCalculator)); // this must be before FilterStatsRule
        rules.add(new FilterStatsRule(filterStatsCalculator));
        rules.add(new ValuesStatsRule(metadata));
        rules.add(new LimitStatsRule(normalizer));
        rules.add(new EnforceSingleRowStatsRule(normalizer));
        rules.add(new ProjectStatsRule(scalarStatsCalculator, normalizer));
        rules.add(new ExchangeStatsRule(normalizer));
        rules.add(new JoinStatsRule(filterStatsCalculator, normalizer));
        rules.add(new AggregationStatsRule(normalizer));
        rules.add(new UnionStatsRule(normalizer));
        rules.add(new AssignUniqueIdStatsRule());
        rules.add(new SemiJoinStatsRule());

        return new ComposableStatsCalculator(rules.build());
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
    @ForAsyncHttp
    public static ExecutorService createAsyncHttpResponseCoreExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("async-http-response-%s"));
    }

    @Provides
    @Singleton
    @ForAsyncHttp
    public static BoundedExecutor createAsyncHttpResponseExecutor(@ForAsyncHttp ExecutorService coreExecutor, TaskManagerConfig config)
    {
        return new BoundedExecutor(coreExecutor, config.getHttpResponseThreads());
    }

    @Provides
    @Singleton
    @ForAsyncHttp
    public static ScheduledExecutorService createAsyncHttpTimeoutExecutor(TaskManagerConfig config)
    {
        return newScheduledThreadPool(config.getHttpTimeoutThreads(), daemonThreadsNamed("async-http-timeout-%s"));
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static ExecutorService createStatementResponseCoreExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("statement-response-%s"));
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static BoundedExecutor createStatementResponseExecutor(@ForStatementResource ExecutorService coreExecutor, TaskManagerConfig config)
    {
        return new BoundedExecutor(coreExecutor, config.getHttpResponseThreads());
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static ScheduledExecutorService createStatementTimeoutExecutor(TaskManagerConfig config)
    {
        return newScheduledThreadPool(config.getHttpTimeoutThreads(), daemonThreadsNamed("statement-timeout-%s"));
    }

    @Provides
    @Singleton
    @ForTransactionManager
    public static ScheduledExecutorService createTransactionIdleCheckExecutor()
    {
        return newSingleThreadScheduledExecutor(daemonThreadsNamed("transaction-idle-check"));
    }

    @Provides
    @Singleton
    @ForTransactionManager
    public static ExecutorService createTransactionFinishingExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("transaction-finishing-%s"));
    }

    @Provides
    @Singleton
    public static TransactionManager createTransactionManager(
            TransactionManagerConfig config,
            CatalogManager catalogManager,
            @ForTransactionManager ScheduledExecutorService idleCheckExecutor,
            @ForTransactionManager ExecutorService finishingExecutor)
    {
        return TransactionManager.create(config, idleCheckExecutor, catalogManager, finishingExecutor);
    }

    private static void bindFailureDetector(Binder binder, boolean coordinator)
    {
        // TODO: this is a hack until the coordinator module works correctly
        if (coordinator) {
            binder.install(new FailureDetectorModule());
            jaxrsBinder(binder).bind(NodeResource.class);
            jaxrsBinder(binder).bind(WorkerResource.class);
            httpClientBinder(binder).bindHttpClient("workerInfo", ForWorkerInfo.class);
        }
        else {
            binder.bind(FailureDetector.class).toInstance(new FailureDetector()
            {
                @Override
                public Set<ServiceDescriptor> getFailed()
                {
                    return ImmutableSet.of();
                }

                @Override
                public State getState(HostAddress hostAddress)
                {
                    // failure detector is not available on workers
                    return State.UNKNOWN;
                }
            });
        }
    }

    public static class ExecutorCleanup
    {
        private final List<ExecutorService> executors;

        @Inject
        public ExecutorCleanup(
                @ForExchange ScheduledExecutorService exchangeExecutor,
                @ForAsyncHttp ExecutorService httpResponseExecutor,
                @ForAsyncHttp ScheduledExecutorService httpTimeoutExecutor,
                @ForTransactionManager ExecutorService transactionFinishingExecutor,
                @ForTransactionManager ScheduledExecutorService transactionIdleExecutor)
        {
            executors = ImmutableList.of(
                    exchangeExecutor,
                    httpResponseExecutor,
                    httpTimeoutExecutor,
                    transactionFinishingExecutor,
                    transactionIdleExecutor);
        }

        @PreDestroy
        public void shutdown()
        {
            executors.forEach(ExecutorService::shutdownNow);
        }
    }
}
