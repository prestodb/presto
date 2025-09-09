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
package com.facebook.presto.flightshim;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.node.NodeConfig;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.drift.codec.guice.ThriftCodecModule;
import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.ConnectorCodecManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.cost.HistoryBasedOptimizationConfig;
import com.facebook.presto.cost.StatsCalculatorModule;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.metadata.AnalyzePropertyManager;
import com.facebook.presto.metadata.BuiltInProcedureRegistry;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.ColumnPropertyManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.MaterializedViewPropertyManager;
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
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.security.AccessControlModule;
import com.facebook.presto.server.PluginManagerConfig;
import com.facebook.presto.server.security.SecurityConfig;
import com.facebook.presto.sessionpropertyproviders.NativeWorkerSessionPropertyProvider;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.RowExpressionSerde;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.procedure.ProcedureRegistry;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.session.WorkerSessionPropertyProvider;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSinkProvider;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.SqlEnvironmentConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.analyzer.JavaFeaturesConfig;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.expressions.JsonCodecRowExpressionSerde;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.gen.RowExpressionPredicateCompiler;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.tracing.TracingConfig;
import com.facebook.presto.transaction.NoOpTransactionManager;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.transaction.TransactionManagerConfig;
import com.facebook.presto.type.TypeDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import jakarta.inject.Singleton;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class FlightShimModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        // FlightShim configs
        configBinder(binder).bindConfig(FlightShimConfig.class, FlightShimConfig.CONFIG_PREFIX);
        configBinder(binder).bindConfig(PluginManagerConfig.class);

        // Presto configs
        configBinder(binder).bindConfig(QueryManagerConfig.class);
        configBinder(binder).bindConfig(TaskManagerConfig.class);
        configBinder(binder).bindConfig(NodeSchedulerConfig.class);
        configBinder(binder).bindConfig(WarningCollectorConfig.class);
        configBinder(binder).bindConfig(MemoryManagerConfig.class);
        configBinder(binder).bindConfig(NodeMemoryConfig.class);
        configBinder(binder).bindConfig(SessionPropertyProviderConfig.class);
        configBinder(binder).bindConfig(SecurityConfig.class);
        configBinder(binder).bindConfig(StaticFunctionNamespaceStoreConfig.class);
        configBinder(binder).bindConfig(StaticTypeManagerStoreConfig.class);
        configBinder(binder).bindConfig(NodeSpillConfig.class);
        configBinder(binder).bindConfig(SqlEnvironmentConfig.class);
        configBinder(binder).bindConfig(CompilerConfig.class);
        configBinder(binder).bindConfig(TracingConfig.class);

        // json codecs
        jsonCodecBinder(binder).bindJsonCodec(ViewDefinition.class);
        jsonCodecBinder(binder).bindJsonCodec(RowExpression.class);

        // Determine the NodeVersion - required by ConnectorManager
        NodeVersion nodeVersion = new NodeVersion("1");
        binder.bind(NodeVersion.class).toInstance(nodeVersion);

        // additional required by ConnectorManager
        binder.install(new AccessControlModule());
        binder.install(new StatsCalculatorModule());
        binder.bind(ConnectorCodecManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPlanOptimizerManager.class).in(Scopes.SINGLETON);

        // index manager - required for ConnectorManager
        binder.bind(IndexManager.class).in(Scopes.SINGLETON);

        // handle resolver
        binder.install(new HandleJsonModule());
        binder.bind(ObjectMapper.class).toProvider(JsonObjectMapperProvider.class);

        // Worker session property providers
        MapBinder<String, WorkerSessionPropertyProvider> mapBinder =
                newMapBinder(binder, String.class, WorkerSessionPropertyProvider.class);
        mapBinder.addBinding("native-worker").to(NativeWorkerSessionPropertyProvider.class).in(Scopes.SINGLETON);

        // history statistics
        configBinder(binder).bindConfig(HistoryBasedOptimizationConfig.class);

        // catalog manager
        binder.bind(StaticCatalogStore.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(StaticCatalogStoreConfig.class);
        binder.bind(ConnectorManager.class).in(Scopes.SINGLETON);
        binder.bind(CatalogManager.class).in(Scopes.SINGLETON);

        // property managers
        binder.bind(SystemSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(SessionPropertyManager.class).in(Scopes.SINGLETON);
        binder.bind(SchemaPropertyManager.class).in(Scopes.SINGLETON);
        binder.bind(TablePropertyManager.class).in(Scopes.SINGLETON);
        binder.bind(MaterializedViewPropertyManager.class).in(Scopes.SINGLETON);
        binder.bind(ColumnPropertyManager.class).in(Scopes.SINGLETON);
        binder.bind(AnalyzePropertyManager.class).in(Scopes.SINGLETON);

        // transaction manager
        configBinder(binder).bindConfig(TransactionManagerConfig.class);
        // Install no-op transaction manager on workers, since only coordinators manage transactions.
        binder.bind(TransactionManager.class).to(NoOpTransactionManager.class).in(Scopes.SINGLETON);

        // metadata
        binder.bind(FunctionAndTypeManager.class).in(Scopes.SINGLETON);
        binder.bind(TableFunctionRegistry.class).in(Scopes.SINGLETON);
        binder.bind(MetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(Metadata.class).to(MetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(StaticFunctionNamespaceStore.class).in(Scopes.SINGLETON);
        binder.bind(StaticTypeManagerStore.class).in(Scopes.SINGLETON);
        binder.bind(BuiltInProcedureRegistry.class).in(Scopes.SINGLETON);
        binder.bind(ProcedureRegistry.class).to(BuiltInProcedureRegistry.class).in(Scopes.SINGLETON);

        // type
        binder.bind(TypeManager.class).to(FunctionAndTypeManager.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        newSetBinder(binder, Type.class);
        binder.bind(TypeDeserializer.class).in(Scopes.SINGLETON);

        // block encodings
        binder.bind(BlockEncodingManager.class).in(Scopes.SINGLETON);
        binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, BlockEncoding.class);
        jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
        jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);

        // features config
        configBinder(binder).bindConfig(FeaturesConfig.class);
        configBinder(binder).bindConfig(FunctionsConfig.class);
        configBinder(binder).bindConfig(JavaFeaturesConfig.class);

        // PageSorter - required by ConnectorManager
        binder.bind(PageSorter.class).to(PagesIndexPageSorter.class).in(Scopes.SINGLETON);

        // PageIndexer - required by ConnectorManager
        binder.bind(PagesIndex.Factory.class).to(PagesIndex.DefaultFactory.class);
        binder.bind(PageIndexerFactory.class).to(GroupByHashPageIndexerFactory.class).in(Scopes.SINGLETON);

        // split manager
        binder.bind(SplitManager.class).in(Scopes.SINGLETON);

        // partitioning provider manager
        binder.bind(PartitioningProviderManager.class).in(Scopes.SINGLETON);

        // Node manager binding
        binder.bind(InternalNodeManager.class).to(InMemoryNodeManager.class).in(Scopes.SINGLETON);
        binder.bind(PluginNodeManager.class).in(Scopes.SINGLETON);
        binder.bind(NodeManager.class).to(PluginNodeManager.class).in(Scopes.SINGLETON);

        // TODO: Decouple and remove: required by SessionPropertyDefaults, PluginManager, InternalResourceGroupManager, ConnectorManager
        configBinder(binder).bindConfig(NodeConfig.class);
        binder.bind(NodeInfo.class).in(Scopes.SINGLETON);

        // compilers - required by ConnectorManager
        binder.bind(JoinCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(JoinCompiler.class).withGeneratedName();
        binder.bind(OrderingCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(OrderingCompiler.class).withGeneratedName();
        binder.bind(DeterminismEvaluator.class).to(RowExpressionDeterminismEvaluator.class).in(Scopes.SINGLETON);
        binder.bind(DomainTranslator.class).to(RowExpressionDomainTranslator.class).in(Scopes.SINGLETON);
        binder.bind(PredicateCompiler.class).to(RowExpressionPredicateCompiler.class).in(Scopes.SINGLETON);

        // for thrift serde
        binder.install(new ThriftCodecModule());
        binder.bind(ConnectorCodecManager.class).in(Scopes.SINGLETON);

        // page sink provider - required by ConnectorManager
        binder.bind(PageSinkManager.class).in(Scopes.SINGLETON);
        binder.bind(PageSinkProvider.class).to(PageSinkManager.class).in(Scopes.SINGLETON);

        // data stream provider - required by ConnectorManager
        binder.bind(PageSourceManager.class).in(Scopes.SINGLETON);
        binder.bind(PageSourceProvider.class).to(PageSourceManager.class).in(Scopes.SINGLETON);

        // expression manager - required by ConnectorManager
        binder.bind(ExpressionOptimizerManager.class).in(Scopes.SINGLETON);
        binder.bind(RowExpressionSerde.class).to(JsonCodecRowExpressionSerde.class).in(Scopes.SINGLETON);

        binder.bind(FlightShimPluginManager.class).in(Scopes.SINGLETON);
        binder.bind(BufferAllocator.class).to(RootAllocator.class).in(Scopes.SINGLETON);
        binder.bind(FlightShimProducer.class).in(Scopes.SINGLETON);

        binder.bind(FlightShimServerExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FlightShimServerExecutionMBean.class).withGeneratedName();
    }

    @Provides
    @Singleton
    @ForFlightShimServer
    public static ExecutorService createFlightShimServerExecutor(FlightShimConfig config)
    {
        return new ThreadPoolExecutor(0, config.getReadThreadPoolSize(), 1L, TimeUnit.MINUTES, new SynchronousQueue<>(), threadsNamed("flight-shim-%s"), new ThreadPoolExecutor.CallerRunsPolicy());
    }
}
