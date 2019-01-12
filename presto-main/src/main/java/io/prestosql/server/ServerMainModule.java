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
package io.prestosql.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.slice.Slice;
import io.airlift.stats.GcMonitor;
import io.airlift.stats.JmxGcMonitor;
import io.airlift.stats.PauseMeter;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.GroupByHashPageIndexerFactory;
import io.prestosql.PagesIndexPageSorter;
import io.prestosql.SystemSessionProperties;
import io.prestosql.block.BlockEncodingManager;
import io.prestosql.block.BlockJsonSerde;
import io.prestosql.client.NodeVersion;
import io.prestosql.client.ServerInfo;
import io.prestosql.connector.ConnectorManager;
import io.prestosql.connector.system.SystemConnectorModule;
import io.prestosql.event.SplitMonitor;
import io.prestosql.execution.ExecutionFailureInfo;
import io.prestosql.execution.ExplainAnalyzeContext;
import io.prestosql.execution.LocationFactory;
import io.prestosql.execution.MemoryRevokingScheduler;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.execution.SqlTaskManager;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.TaskManagementExecutor;
import io.prestosql.execution.TaskManager;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.execution.TaskStatus;
import io.prestosql.execution.executor.MultilevelSplitQueue;
import io.prestosql.execution.executor.TaskExecutor;
import io.prestosql.execution.scheduler.FlatNetworkTopology;
import io.prestosql.execution.scheduler.LegacyNetworkTopology;
import io.prestosql.execution.scheduler.NetworkTopology;
import io.prestosql.execution.scheduler.NodeScheduler;
import io.prestosql.execution.scheduler.NodeSchedulerConfig;
import io.prestosql.execution.scheduler.NodeSchedulerExporter;
import io.prestosql.index.IndexManager;
import io.prestosql.memory.LocalMemoryManager;
import io.prestosql.memory.LocalMemoryManagerExporter;
import io.prestosql.memory.MemoryInfo;
import io.prestosql.memory.MemoryManagerConfig;
import io.prestosql.memory.MemoryPoolAssignmentsRequest;
import io.prestosql.memory.MemoryResource;
import io.prestosql.memory.NodeMemoryConfig;
import io.prestosql.memory.ReservedSystemMemoryConfig;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.ColumnPropertyManager;
import io.prestosql.metadata.DiscoveryNodeManager;
import io.prestosql.metadata.ForNodeManager;
import io.prestosql.metadata.HandleJsonModule;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.SchemaPropertyManager;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.metadata.StaticCatalogStore;
import io.prestosql.metadata.StaticCatalogStoreConfig;
import io.prestosql.metadata.TablePropertyManager;
import io.prestosql.metadata.ViewDefinition;
import io.prestosql.operator.ExchangeClientConfig;
import io.prestosql.operator.ExchangeClientFactory;
import io.prestosql.operator.ExchangeClientSupplier;
import io.prestosql.operator.ForExchange;
import io.prestosql.operator.LookupJoinOperators;
import io.prestosql.operator.OperatorStats;
import io.prestosql.operator.PagesIndex;
import io.prestosql.operator.index.IndexJoinLookupStats;
import io.prestosql.server.remotetask.HttpLocationFactory;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncoding;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spiller.FileSingleStreamSpillerFactory;
import io.prestosql.spiller.GenericPartitioningSpillerFactory;
import io.prestosql.spiller.GenericSpillerFactory;
import io.prestosql.spiller.LocalSpillManager;
import io.prestosql.spiller.NodeSpillConfig;
import io.prestosql.spiller.PartitioningSpillerFactory;
import io.prestosql.spiller.SingleStreamSpillerFactory;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.spiller.SpillerStats;
import io.prestosql.split.PageSinkManager;
import io.prestosql.split.PageSinkProvider;
import io.prestosql.split.PageSourceManager;
import io.prestosql.split.PageSourceProvider;
import io.prestosql.split.SplitManager;
import io.prestosql.sql.Serialization.ExpressionDeserializer;
import io.prestosql.sql.Serialization.ExpressionSerializer;
import io.prestosql.sql.Serialization.FunctionCallDeserializer;
import io.prestosql.sql.SqlEnvironmentConfig;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.parser.SqlParserOptions;
import io.prestosql.sql.planner.CompilerConfig;
import io.prestosql.sql.planner.LocalExecutionPlanner;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.transaction.TransactionManagerConfig;
import io.prestosql.type.TypeDeserializer;
import io.prestosql.type.TypeRegistry;
import io.prestosql.util.FinalizerService;
import io.prestosql.version.EmbedVersion;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.execution.scheduler.NodeSchedulerConfig.NetworkTopologyType.FLAT;
import static io.prestosql.execution.scheduler.NodeSchedulerConfig.NetworkTopologyType.LEGACY;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
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
        }
        else {
            install(new WorkerModule());
        }

        install(new InternalCommunicationModule());

        configBinder(binder).bindConfig(FeaturesConfig.class);

        binder.bind(SqlParser.class).in(Scopes.SINGLETON);
        binder.bind(SqlParserOptions.class).toInstance(sqlParserOptions);
        sqlParserOptions.useEnhancedErrorHandler(serverConfig.isEnhancedErrorReporting());

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

        // schema properties
        binder.bind(SchemaPropertyManager.class).in(Scopes.SINGLETON);

        // table properties
        binder.bind(TablePropertyManager.class).in(Scopes.SINGLETON);

        // column properties
        binder.bind(ColumnPropertyManager.class).in(Scopes.SINGLETON);

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
        jaxrsBinder(binder).bind(TaskExecutorResource.class);
        newExporter(binder).export(TaskExecutorResource.class).withGeneratedName();
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
        binder.bind(EmbedVersion.class).in(Scopes.SINGLETON);
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
        jaxrsBinder(binder).bind(PagesResponseWriter.class);

        // exchange client
        binder.bind(ExchangeClientSupplier.class).to(ExchangeClientFactory.class).in(Scopes.SINGLETON);
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

        // split monitor
        binder.bind(SplitMonitor.class).in(Scopes.SINGLETON);

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

    public static class ExecutorCleanup
    {
        private final List<ExecutorService> executors;

        @Inject
        public ExecutorCleanup(
                @ForExchange ScheduledExecutorService exchangeExecutor,
                @ForAsyncHttp ExecutorService httpResponseExecutor,
                @ForAsyncHttp ScheduledExecutorService httpTimeoutExecutor)
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
        }
    }
}
