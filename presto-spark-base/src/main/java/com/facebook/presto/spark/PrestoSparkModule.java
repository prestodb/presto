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
package com.facebook.presto.spark;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.airlift.node.NodeConfig;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.ClientRequestFilterManager;
import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.connector.ConnectorTypeSerdeManager;
import com.facebook.presto.connector.system.SystemConnectorModule;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostCalculatorUsingExchanges;
import com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.dispatcher.QueryPrerequisitesManager;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.event.QueryMonitorConfig;
import com.facebook.presto.event.SplitMonitor;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.ExplainAnalyzeContext;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.executor.MultilevelSplitQueue;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroupManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.nodeSelection.SimpleTtlNodeSelectorConfig;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.metadata.AnalyzePropertyManager;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.ColumnPropertyManager;
import com.facebook.presto.metadata.ConnectorMetadataUpdaterManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.StaticCatalogStore;
import com.facebook.presto.metadata.StaticCatalogStoreConfig;
import com.facebook.presto.metadata.StaticFunctionNamespaceStore;
import com.facebook.presto.metadata.StaticFunctionNamespaceStoreConfig;
import com.facebook.presto.metadata.TableFunctionRegistry;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.nodeManager.PluginNodeManager;
import com.facebook.presto.operator.FileFragmentResultCacheConfig;
import com.facebook.presto.operator.FileFragmentResultCacheManager;
import com.facebook.presto.operator.FragmentCacheStats;
import com.facebook.presto.operator.FragmentResultCacheManager;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.NoOpFragmentResultCacheManager;
import com.facebook.presto.operator.OperatorInfo;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.TableCommitContext;
import com.facebook.presto.operator.TaskMemoryReservationSummary;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.resourcemanager.NoopResourceGroupService;
import com.facebook.presto.resourcemanager.ResourceGroupService;
import com.facebook.presto.server.ConnectorMetadataUpdateHandleJsonSerde;
import com.facebook.presto.server.ForJsonMetadataUpdateHandle;
import com.facebook.presto.server.NodeStatusNotificationManager;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.server.PluginManagerConfig;
import com.facebook.presto.server.QuerySessionSupplier;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.server.SessionPropertyDefaults;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.remotetask.RemoteTaskStats;
import com.facebook.presto.server.security.PasswordAuthenticatorManager;
import com.facebook.presto.server.security.PrestoAuthenticatorManager;
import com.facebook.presto.server.security.SecurityConfig;
import com.facebook.presto.spark.accesscontrol.PrestoSparkAccessControlChecker;
import com.facebook.presto.spark.accesscontrol.PrestoSparkAuthenticatorProvider;
import com.facebook.presto.spark.accesscontrol.PrestoSparkCredentialsProvider;
import com.facebook.presto.spark.classloader_interface.SparkProcessType;
import com.facebook.presto.spark.execution.BroadcastFileInfo;
import com.facebook.presto.spark.execution.PrestoSparkBroadcastTableCacheManager;
import com.facebook.presto.spark.execution.PrestoSparkExecutionExceptionFactory;
import com.facebook.presto.spark.execution.http.BatchTaskUpdateRequest;
import com.facebook.presto.spark.execution.property.NativeExecutionConnectorConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionNodeConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionSystemConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionVeloxConfig;
import com.facebook.presto.spark.execution.shuffle.PrestoSparkLocalShuffleReadInfo;
import com.facebook.presto.spark.execution.shuffle.PrestoSparkLocalShuffleWriteInfo;
import com.facebook.presto.spark.execution.task.PrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.node.PrestoSparkInternalNodeManager;
import com.facebook.presto.spark.node.PrestoSparkNodePartitioningManager;
import com.facebook.presto.spark.node.PrestoSparkQueryManager;
import com.facebook.presto.spark.node.PrestoSparkTaskManager;
import com.facebook.presto.spark.planner.PrestoSparkPlanFragmenter;
import com.facebook.presto.spark.planner.PrestoSparkQueryPlanner;
import com.facebook.presto.spark.planner.PrestoSparkRddFactory;
import com.facebook.presto.spark.planner.PrestoSparkStatsCalculatorModule;
import com.facebook.presto.spark.planner.optimizers.AdaptivePlanOptimizers;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.spi.ConnectorTypeSerde;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.plan.SimplePlanFragment;
import com.facebook.presto.spi.plan.SimplePlanFragmentSerde;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spiller.GenericPartitioningSpillerFactory;
import com.facebook.presto.spiller.GenericSpillerFactory;
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
import com.facebook.presto.sql.Serialization.VariableReferenceExpressionDeserializer;
import com.facebook.presto.sql.Serialization.VariableReferenceExpressionSerializer;
import com.facebook.presto.sql.SqlEnvironmentConfig;
import com.facebook.presto.sql.analyzer.AnalyzerProviderManager;
import com.facebook.presto.sql.analyzer.BuiltInAnalyzerProvider;
import com.facebook.presto.sql.analyzer.BuiltInQueryAnalyzer;
import com.facebook.presto.sql.analyzer.BuiltInQueryPreparer;
import com.facebook.presto.sql.analyzer.BuiltInQueryPreparerProvider;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
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
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanOptimizers;
import com.facebook.presto.sql.planner.plan.JsonCodecSimplePlanFragmentSerde;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.facebook.presto.sql.planner.sanity.PlanCheckerProviderManager;
import com.facebook.presto.sql.planner.sanity.PlanCheckerProviderManagerConfig;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.tracing.TracerProviderManager;
import com.facebook.presto.tracing.TracingConfig;
import com.facebook.presto.transaction.InMemoryTransactionManager;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.transaction.TransactionManagerConfig;
import com.facebook.presto.ttl.clusterttlprovidermanagers.ClusterTtlProviderManager;
import com.facebook.presto.ttl.clusterttlprovidermanagers.ThrowingClusterTtlProviderManager;
import com.facebook.presto.ttl.nodettlfetchermanagers.NodeTtlFetcherManager;
import com.facebook.presto.ttl.nodettlfetchermanagers.ThrowingNodeTtlFetcherManager;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.util.PrestoDataDefBindingHelper;
import com.facebook.presto.version.EmbedVersion;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.testing.TestingMBeanServer;

import javax.inject.Singleton;
import javax.management.MBeanServer;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.airlift.json.smile.SmileCodecBinder.smileCodecBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class PrestoSparkModule
        extends AbstractConfigurationAwareModule
{
    private final SparkProcessType sparkProcessType;
    private final SqlParserOptions sqlParserOptions;

    public PrestoSparkModule(SparkProcessType sparkProcessType, SqlParserOptions sqlParserOptions)
    {
        this.sparkProcessType = requireNonNull(sparkProcessType, "sparkProcessType is null");
        this.sqlParserOptions = requireNonNull(sqlParserOptions, "sqlParserOptions is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        // configs
        // TODO: decouple configuration properties that don't make sense on Spark
        configBinder(binder).bindConfig(NodeSchedulerConfig.class);
        configBinder(binder).bindConfig(SimpleTtlNodeSelectorConfig.class);
        configBinder(binder).bindConfig(QueryManagerConfig.class);
        configBinder(binder).bindConfigGlobalDefaults(QueryManagerConfig.class, PrestoSparkSettingsRequirements::setDefaults);
        configBinder(binder).bindConfig(FeaturesConfig.class);
        configBinder(binder).bindConfig(FunctionsConfig.class);
        configBinder(binder).bindConfig(JavaFeaturesConfig.class);
        configBinder(binder).bindConfigGlobalDefaults(FeaturesConfig.class, PrestoSparkSettingsRequirements::setDefaults);
        configBinder(binder).bindConfig(MemoryManagerConfig.class);
        configBinder(binder).bindConfig(TaskManagerConfig.class);
        configBinder(binder).bindConfig(TransactionManagerConfig.class);
        configBinder(binder).bindConfig(NodeMemoryConfig.class);
        configBinder(binder).bindConfig(WarningCollectorConfig.class);
        configBinder(binder).bindConfig(NodeSpillConfig.class);
        configBinder(binder).bindConfig(CompilerConfig.class);
        configBinder(binder).bindConfig(SqlEnvironmentConfig.class);
        configBinder(binder).bindConfig(StaticFunctionNamespaceStoreConfig.class);
        configBinder(binder).bindConfig(PrestoSparkConfig.class);
        configBinder(binder).bindConfig(TracingConfig.class);
        configBinder(binder).bindConfig(NativeExecutionVeloxConfig.class);
        configBinder(binder).bindConfig(NativeExecutionSystemConfig.class);
        configBinder(binder).bindConfig(NativeExecutionNodeConfig.class);
        configBinder(binder).bindConfig(NativeExecutionConnectorConfig.class);
        configBinder(binder).bindConfig(PlanCheckerProviderManagerConfig.class);
        configBinder(binder).bindConfig(SecurityConfig.class);

        // json codecs
        jsonCodecBinder(binder).bindJsonCodec(ViewDefinition.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(PrestoSparkTaskDescriptor.class);
        jsonCodecBinder(binder).bindJsonCodec(PlanFragment.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskSource.class);
        jsonCodecBinder(binder).bindJsonCodec(TableCommitContext.class);
        jsonCodecBinder(binder).bindJsonCodec(ExplainAnalyzeContext.class);
        jsonCodecBinder(binder).bindJsonCodec(ExecutionFailureInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(StageInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(OperatorStats.class);
        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(PrestoSparkQueryStatusInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(PrestoSparkQueryData.class);
        jsonCodecBinder(binder).bindListJsonCodec(TaskMemoryReservationSummary.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(ServerInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(PrestoSparkLocalShuffleReadInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(PrestoSparkLocalShuffleWriteInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(BatchTaskUpdateRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(BroadcastFileInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(SimplePlanFragment.class);
        binder.bind(SimplePlanFragmentSerde.class).to(JsonCodecSimplePlanFragmentSerde.class).in(Scopes.SINGLETON);

        // smile codecs
        smileCodecBinder(binder).bindSmileCodec(TaskSource.class);
        smileCodecBinder(binder).bindSmileCodec(TaskInfo.class);

        PrestoSparkConfig prestoSparkConfig = buildConfigObject(PrestoSparkConfig.class);
        if (prestoSparkConfig.isSmileSerializationEnabled()) {
            binder.bind(new TypeLiteral<Codec<TaskSource>>() {}).to(new TypeLiteral<SmileCodec<TaskSource>>() {}).in(Scopes.SINGLETON);
            binder.bind(new TypeLiteral<Codec<TaskInfo>>() {}).to(new TypeLiteral<SmileCodec<TaskInfo>>() {}).in(Scopes.SINGLETON);
        }
        else {
            binder.bind(new TypeLiteral<Codec<TaskSource>>() {}).to(new TypeLiteral<JsonCodec<TaskSource>>() {}).in(Scopes.SINGLETON);
            binder.bind(new TypeLiteral<Codec<TaskInfo>>() {}).to(new TypeLiteral<JsonCodec<TaskInfo>>() {}).in(Scopes.SINGLETON);
        }

        // index manager
        binder.bind(IndexManager.class).in(Scopes.SINGLETON);

        // handle resolver
        binder.install(new HandleJsonModule());

        // plugin manager
        configBinder(binder).bindConfig(PluginManagerConfig.class);
        binder.bind(PluginManager.class).in(Scopes.SINGLETON);

        // catalog manager
        binder.bind(StaticCatalogStore.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(StaticCatalogStoreConfig.class);

        // catalog
        binder.bind(ConnectorManager.class).in(Scopes.SINGLETON);
        binder.bind(CatalogManager.class).in(Scopes.SINGLETON);

        // property managers
        binder.bind(SessionPropertyManager.class).toProvider(PrestoSparkSessionPropertyManagerProvider.class).in(Scopes.SINGLETON);
        binder.bind(SystemSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(PrestoSparkSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(SessionPropertyDefaults.class).in(Scopes.SINGLETON);
        binder.bind(SchemaPropertyManager.class).in(Scopes.SINGLETON);
        binder.bind(TablePropertyManager.class).in(Scopes.SINGLETON);
        binder.bind(ColumnPropertyManager.class).in(Scopes.SINGLETON);
        binder.bind(AnalyzePropertyManager.class).in(Scopes.SINGLETON);
        binder.bind(QuerySessionSupplier.class).in(Scopes.SINGLETON);

        // expression manager
        binder.bind(ExpressionOptimizerManager.class).in(Scopes.SINGLETON);

        // tracer provider managers
        binder.bind(TracerProviderManager.class).in(Scopes.SINGLETON);

        // block encodings
        binder.bind(BlockEncodingManager.class).in(Scopes.SINGLETON);
        binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, BlockEncoding.class);
        jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
        jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);

        // metadata
        binder.bind(FunctionAndTypeManager.class).in(Scopes.SINGLETON);
        binder.bind(TableFunctionRegistry.class).in(Scopes.SINGLETON);
        binder.bind(MetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(Metadata.class).to(MetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(StaticFunctionNamespaceStore.class).in(Scopes.SINGLETON);

        // type
        newSetBinder(binder, Type.class);
        binder.bind(TypeManager.class).to(FunctionAndTypeManager.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);

        // PageSorter
        binder.bind(PageSorter.class).to(PagesIndexPageSorter.class).in(Scopes.SINGLETON);

        // PageIndexer
        binder.bind(PagesIndex.Factory.class).to(PagesIndex.DefaultFactory.class);
        binder.bind(PageIndexerFactory.class).to(GroupByHashPageIndexerFactory.class).in(Scopes.SINGLETON);

        // compilers
        binder.bind(JoinFilterFunctionCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(JoinFilterFunctionCompiler.class).withGeneratedName();
        binder.bind(JoinCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(JoinCompiler.class).withGeneratedName();
        binder.bind(OrderingCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(OrderingCompiler.class).withGeneratedName();
        binder.bind(LookupJoinOperators.class).in(Scopes.SINGLETON);
        binder.bind(DomainTranslator.class).to(RowExpressionDomainTranslator.class).in(Scopes.SINGLETON);
        binder.bind(PredicateCompiler.class).to(RowExpressionPredicateCompiler.class).in(Scopes.SINGLETON);
        binder.bind(DeterminismEvaluator.class).to(RowExpressionDeterminismEvaluator.class).in(Scopes.SINGLETON);
        binder.bind(ExpressionCompiler.class).in(Scopes.SINGLETON);
        binder.bind(PageFunctionCompiler.class).in(Scopes.SINGLETON);

        // split manager
        binder.bind(SplitManager.class).in(Scopes.SINGLETON);

        // partitioning provider manager
        binder.bind(PartitioningProviderManager.class).in(Scopes.SINGLETON);

        // Metadata Extractor
        binder.bind(ExecutorService.class).annotatedWith(ForMetadataExtractor.class)
                .toInstance(newCachedThreadPool(threadsNamed("metadata-extractor-%s")));
        binder.bind(MetadataExtractorMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(MetadataExtractorMBean.class).as(generatedNameOf(MetadataExtractor.class));

        // executors
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("presto-spark-executor-%s"));
        binder.bind(Executor.class).toInstance(executor);
        binder.bind(ExecutorService.class).toInstance(executor);
        // Set the initial thread pool size to 1 (instead of 0) to avoid the thread pool hogging CPU due to JDK8 bug: https://bugs.openjdk.org/browse/JDK-8129861
        binder.bind(ScheduledExecutorService.class).toInstance(newScheduledThreadPool(5, daemonThreadsNamed("presto-spark-scheduled-executor-%s")));

        // task executor
        binder.bind(EmbedVersion.class).in(Scopes.SINGLETON);
        binder.bind(MultilevelSplitQueue.class).in(Scopes.SINGLETON);
        binder.bind(TaskExecutor.class).in(Scopes.SINGLETON);

        // data stream provider
        binder.bind(PageSourceManager.class).in(Scopes.SINGLETON);
        binder.bind(PageSourceProvider.class).to(PageSourceManager.class).in(Scopes.SINGLETON);

        // connector distributed metadata manager
        binder.bind(ConnectorMetadataUpdaterManager.class).in(Scopes.SINGLETON);

        // connector metadata update handle serde manager
        binder.bind(ConnectorTypeSerdeManager.class).in(Scopes.SINGLETON);

        // connector metadata update handle json serde
        binder.bind(new TypeLiteral<ConnectorTypeSerde<ConnectorMetadataUpdateHandle>>() {})
                .annotatedWith(ForJsonMetadataUpdateHandle.class)
                .to(ConnectorMetadataUpdateHandleJsonSerde.class)
                .in(Scopes.SINGLETON);

        // page sink provider
        binder.bind(PageSinkManager.class).in(Scopes.SINGLETON);
        binder.bind(PageSinkProvider.class).to(PageSinkManager.class).in(Scopes.SINGLETON);

        // query explainer
        binder.bind(QueryExplainer.class).in(Scopes.SINGLETON);

        // parser
        binder.bind(PlanChecker.class).in(Scopes.SINGLETON);
        binder.bind(SqlParser.class).in(Scopes.SINGLETON);
        binder.bind(SqlParserOptions.class).toInstance(sqlParserOptions);

        // analyzer
        binder.bind(BuiltInQueryPreparer.class).in(Scopes.SINGLETON);
        binder.bind(BuiltInQueryPreparerProvider.class).in(Scopes.SINGLETON);
        binder.bind(QueryPreparerProviderManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, QueryExplainer.class);
        binder.bind(BuiltInQueryAnalyzer.class).in(Scopes.SINGLETON);
        binder.bind(BuiltInAnalyzerProvider.class).in(Scopes.SINGLETON);
        binder.bind(AnalyzerProviderManager.class).in(Scopes.SINGLETON);

        // planner
        binder.bind(PlanFragmenter.class).in(Scopes.SINGLETON);
        binder.bind(PlanOptimizers.class).in(Scopes.SINGLETON);
        binder.bind(AdaptivePlanOptimizers.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPlanOptimizerManager.class).in(Scopes.SINGLETON);
        binder.bind(LocalExecutionPlanner.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(FileFragmentResultCacheConfig.class);
        binder.bind(FragmentCacheStats.class).in(Scopes.SINGLETON);
        binder.bind(IndexJoinLookupStats.class).in(Scopes.SINGLETON);
        binder.bind(QueryIdGenerator.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addKeySerializerBinding(VariableReferenceExpression.class).to(VariableReferenceExpressionSerializer.class);
        jsonBinder(binder).addKeyDeserializerBinding(VariableReferenceExpression.class).to(VariableReferenceExpressionDeserializer.class);

        // statistics calculator / cost calculator
        binder.install(new PrestoSparkStatsCalculatorModule());
        binder.bind(CostCalculator.class).to(CostCalculatorUsingExchanges.class).in(Scopes.SINGLETON);
        binder.bind(CostCalculator.class).annotatedWith(CostCalculator.EstimatedExchanges.class).to(CostCalculatorWithEstimatedExchanges.class).in(Scopes.SINGLETON);
        binder.bind(CostComparator.class).in(Scopes.SINGLETON);

        // JMX (Do not export to the real MXBean server, as the Presto context may be created multiple times per JVM)
        binder.bind(MBeanServer.class).toInstance(new TestingMBeanServer());
        binder.bind(MBeanExporter.class).in(Scopes.SINGLETON);

        // spill
        binder.bind(SpillerFactory.class).to(GenericSpillerFactory.class).in(Scopes.SINGLETON);
        binder.bind(SingleStreamSpillerFactory.class).to(TempStorageSingleStreamSpillerFactory.class).in(Scopes.SINGLETON);
        binder.bind(PartitioningSpillerFactory.class).to(GenericPartitioningSpillerFactory.class).in(Scopes.SINGLETON);
        binder.bind(SpillerStats.class).in(Scopes.SINGLETON);
        binder.bind(StandaloneSpillerFactory.class).to(TempStorageStandaloneSpillerFactory.class).in(Scopes.SINGLETON);

        // monitoring
        jsonCodecBinder(binder).bindJsonCodec(OperatorInfo.class);
        binder.bind(QueryMonitor.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(QueryMonitorConfig.class);
        binder.bind(SplitMonitor.class).in(Scopes.SINGLETON);

        // Determine the NodeVersion
        ServerConfig serverConfig = buildConfigObject(ServerConfig.class);
        NodeVersion nodeVersion = new NodeVersion(serverConfig.getPrestoVersion());
        binder.bind(NodeVersion.class).toInstance(nodeVersion);

        //Support DDL statements. Helper class binds DDL statements to DataDefinitionTasks
        PrestoDataDefBindingHelper.bindDDLDefinitionTasks(binder);

        // TODO: Decouple node specific system tables
        binder.bind(QueryManager.class).to(PrestoSparkQueryManager.class).in(Scopes.SINGLETON);
        binder.bind(TaskManager.class).to(PrestoSparkTaskManager.class).in(Scopes.SINGLETON);
        binder.install(new SystemConnectorModule());

        // TODO: support explain analyze for Spark
        binder.bind(new TypeLiteral<Optional<ExplainAnalyzeContext>>() {}).toInstance(Optional.of(new ExplainAnalyzeContext((queryId) -> {
            throw new UnsupportedOperationException("explain analyze is not supported");
        })));

        // TODO: support CBO, supply real nodes count
        binder.bind(TaskCountEstimator.class).toInstance(new TaskCountEstimator(() -> 1000));

        // TODO: Decouple and remove: required by ConnectorManager
        binder.bind(InternalNodeManager.class).toInstance(new PrestoSparkInternalNodeManager());
        binder.bind(PluginNodeManager.class).in(Scopes.SINGLETON);

        // TODO: Decouple and remove: required by PluginManager
        binder.bind(InternalResourceGroupManager.class).in(Scopes.SINGLETON);
        binder.bind(ResourceGroupManager.class).to(InternalResourceGroupManager.class);
        binder.bind(new TypeLiteral<ResourceGroupManager<?>>() {}).to(new TypeLiteral<InternalResourceGroupManager<?>>() {});
        binder.bind(ClusterMemoryPoolManager.class).toInstance(((poolId, listener) -> {}));
        binder.bind(QueryPrerequisitesManager.class).in(Scopes.SINGLETON);
        binder.bind(ResourceGroupService.class).to(NoopResourceGroupService.class).in(Scopes.SINGLETON);
        binder.bind(NodeTtlFetcherManager.class).to(ThrowingNodeTtlFetcherManager.class).in(Scopes.SINGLETON);
        binder.bind(ClusterTtlProviderManager.class).to(ThrowingClusterTtlProviderManager.class).in(Scopes.SINGLETON);
        binder.bind(NodeStatusNotificationManager.class).in(Scopes.SINGLETON);

        // TODO: Decouple and remove: required by SessionPropertyDefaults, PluginManager, InternalResourceGroupManager, ConnectorManager
        configBinder(binder).bindConfig(NodeConfig.class);
        binder.bind(NodeInfo.class).in(Scopes.SINGLETON);

        // TODO: Decouple and remove: required by LocalExecutionPlanner, PlanFragmenter
        binder.bind(NodePartitioningManager.class).to(PrestoSparkNodePartitioningManager.class).in(Scopes.SINGLETON);

        binder.bind(PasswordAuthenticatorManager.class).in(Scopes.SINGLETON);
        binder.bind(PrestoAuthenticatorManager.class).in(Scopes.SINGLETON);

        binder.bind(RemoteTaskStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RemoteTaskStats.class).withGeneratedName();

        // spark specific
        binder.bind(SparkProcessType.class).toInstance(sparkProcessType);
        binder.bind(PrestoSparkExecutionExceptionFactory.class).in(Scopes.SINGLETON);
        binder.bind(PrestoSparkSettingsRequirements.class).in(Scopes.SINGLETON);
        binder.bind(PrestoSparkQueryPlanner.class).in(Scopes.SINGLETON);
        binder.bind(PrestoSparkAccessControlChecker.class).in(Scopes.SINGLETON);
        binder.bind(PrestoSparkPlanFragmenter.class).in(Scopes.SINGLETON);
        binder.bind(PrestoSparkRddFactory.class).in(Scopes.SINGLETON);
        binder.bind(PrestoSparkTaskExecutorFactory.class).in(Scopes.SINGLETON);
        binder.bind(PrestoSparkQueryExecutionFactory.class).in(Scopes.SINGLETON);
        binder.bind(PrestoSparkService.class).in(Scopes.SINGLETON);
        binder.bind(PrestoSparkBroadcastTableCacheManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, PrestoSparkServiceWaitTimeMetrics.class);
        newOptionalBinder(binder, ErrorClassifier.class);

        // extra credentials and authenticator for Presto-on-Spark
        newSetBinder(binder, PrestoSparkCredentialsProvider.class);
        newSetBinder(binder, PrestoSparkAuthenticatorProvider.class);
        binder.bind(ClientRequestFilterManager.class).in(Scopes.SINGLETON);
        binder.bind(PlanCheckerProviderManager.class).in(Scopes.SINGLETON);
        binder.bind(NodeManager.class).to(PluginNodeManager.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static TransactionManager createTransactionManager(
            TransactionManagerConfig config,
            CatalogManager catalogManager,
            ScheduledExecutorService scheduledExecutor,
            ExecutorService executor)
    {
        return InMemoryTransactionManager.create(config, scheduledExecutor, catalogManager, executor);
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
}
