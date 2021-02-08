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
package com.facebook.presto.testing;

import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.connector.system.AnalyzePropertiesSystemTable;
import com.facebook.presto.connector.system.CatalogSystemTable;
import com.facebook.presto.connector.system.ColumnPropertiesSystemTable;
import com.facebook.presto.connector.system.GlobalSystemConnector;
import com.facebook.presto.connector.system.GlobalSystemConnectorFactory;
import com.facebook.presto.connector.system.NodeSystemTable;
import com.facebook.presto.connector.system.SchemaPropertiesSystemTable;
import com.facebook.presto.connector.system.TablePropertiesSystemTable;
import com.facebook.presto.connector.system.TransactionsSystemTable;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostCalculatorUsingExchanges;
import com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.FilterStatsCalculator;
import com.facebook.presto.cost.ScalarStatsCalculator;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsNormalizer;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.AlterFunctionTask;
import com.facebook.presto.execution.CommitTask;
import com.facebook.presto.execution.CreateFunctionTask;
import com.facebook.presto.execution.CreateTableTask;
import com.facebook.presto.execution.CreateViewTask;
import com.facebook.presto.execution.DataDefinitionTask;
import com.facebook.presto.execution.DeallocateTask;
import com.facebook.presto.execution.DropFunctionTask;
import com.facebook.presto.execution.DropTableTask;
import com.facebook.presto.execution.DropViewTask;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.PrepareTask;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.QueryPreparer;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.execution.RenameColumnTask;
import com.facebook.presto.execution.RenameTableTask;
import com.facebook.presto.execution.ResetSessionTask;
import com.facebook.presto.execution.RollbackTask;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.SetSessionTask;
import com.facebook.presto.execution.StartTransactionTask;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.resourceGroups.NoOpResourceGroupManager;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.StreamingPlanSection;
import com.facebook.presto.execution.scheduler.StreamingSubPlan;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.execution.warnings.DefaultWarningCollector;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.metadata.AnalyzePropertyManager;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.ColumnPropertyManager;
import com.facebook.presto.metadata.ConnectorMetadataUpdaterManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.NoOpFragmentResultCacheManager;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.operator.TableCommitContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.server.PluginManagerConfig;
import com.facebook.presto.server.SessionPropertyDefaults;
import com.facebook.presto.server.security.PasswordAuthenticatorManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spiller.FileSingleStreamSpillerFactory;
import com.facebook.presto.spiller.GenericPartitioningSpillerFactory;
import com.facebook.presto.spiller.GenericSpillerFactory;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.spiller.SpillerStats;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.sql.gen.RowExpressionPredicateCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanOptimizers;
import com.facebook.presto.sql.planner.RemoteSourceFactory;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.tree.AlterFunction;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.DropFunction;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.testing.PageConsumerOperator.PageConsumerOutputFactory;
import com.facebook.presto.transaction.InMemoryTransactionManager;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.transaction.TransactionManagerConfig;
import com.facebook.presto.util.FinalizerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.testing.TestingMBeanServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.cost.StatsCalculatorModule.createNewStatsCalculator;
import static com.facebook.presto.execution.scheduler.StreamingPlanSection.extractStreamingSections;
import static com.facebook.presto.execution.scheduler.TableWriteInfo.createTableWriteInfo;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.GROUPED_SCHEDULING;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.REWINDABLE_GROUPED_SCHEDULING;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.testing.TreeAssertions.assertFormattedSql;
import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class LocalQueryRunner
        implements QueryRunner
{
    private final Session defaultSession;
    private final ExecutorService notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;
    private final FinalizerService finalizerService;

    private final SqlParser sqlParser;
    private final PlanFragmenter planFragmenter;
    private final InMemoryNodeManager nodeManager;
    private final PageSorter pageSorter;
    private final PageIndexerFactory pageIndexerFactory;
    private final MetadataManager metadata;
    private final ScalarStatsCalculator scalarStatsCalculator;
    private final StatsNormalizer statsNormalizer;
    private final FilterStatsCalculator filterStatsCalculator;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final CostCalculator estimatedExchangesCostCalculator;
    private final TaskCountEstimator taskCountEstimator;
    private final TestingAccessControlManager accessControl;
    private final SplitManager splitManager;
    private final BlockEncodingManager blockEncodingManager;
    private final PageSourceManager pageSourceManager;
    private final IndexManager indexManager;
    private final PartitioningProviderManager partitioningProviderManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final ConnectorPlanOptimizerManager planOptimizerManager;
    private final ConnectorMetadataUpdaterManager distributedMetadataManager;
    private final PageSinkManager pageSinkManager;
    private final TransactionManager transactionManager;
    private final FileSingleStreamSpillerFactory singleStreamSpillerFactory;
    private final SpillerFactory spillerFactory;
    private final PartitioningSpillerFactory partitioningSpillerFactory;

    private final PageFunctionCompiler pageFunctionCompiler;
    private final ExpressionCompiler expressionCompiler;
    private final JoinFilterFunctionCompiler joinFilterFunctionCompiler;
    private final JoinCompiler joinCompiler;
    private final ConnectorManager connectorManager;
    private final PluginManager pluginManager;
    private final ImmutableMap<Class<? extends Statement>, DataDefinitionTask<?>> dataDefinitionTask;

    private final boolean alwaysRevokeMemory;
    private final NodeSpillConfig nodeSpillConfig;
    private final NodeSchedulerConfig nodeSchedulerConfig;
    private boolean printPlan;

    private final PlanChecker distributedPlanChecker;
    private final PlanChecker singleNodePlanChecker;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public LocalQueryRunner(Session defaultSession)
    {
        this(defaultSession, new FeaturesConfig(), new NodeSpillConfig(), false, false);
    }

    public LocalQueryRunner(Session defaultSession, FeaturesConfig featuresConfig)
    {
        this(defaultSession, featuresConfig, new NodeSpillConfig(), false, false);
    }

    public LocalQueryRunner(Session defaultSession, FeaturesConfig featuresConfig, NodeSpillConfig nodeSpillConfig, boolean withInitialTransaction, boolean alwaysRevokeMemory)
    {
        this(defaultSession, featuresConfig, nodeSpillConfig, withInitialTransaction, alwaysRevokeMemory, 1);
    }

    private LocalQueryRunner(Session defaultSession, FeaturesConfig featuresConfig, NodeSpillConfig nodeSpillConfig, boolean withInitialTransaction, boolean alwaysRevokeMemory, int nodeCountForStats)
    {
        requireNonNull(defaultSession, "defaultSession is null");
        checkArgument(!defaultSession.getTransactionId().isPresent() || !withInitialTransaction, "Already in transaction");

        this.nodeSpillConfig = requireNonNull(nodeSpillConfig, "nodeSpillConfig is null");
        this.alwaysRevokeMemory = alwaysRevokeMemory;
        this.notificationExecutor = newCachedThreadPool(daemonThreadsNamed("local-query-runner-executor-%s"));
        this.yieldExecutor = newScheduledThreadPool(2, daemonThreadsNamed("local-query-runner-scheduler-%s"));
        this.finalizerService = new FinalizerService();
        finalizerService.start();

        this.sqlParser = new SqlParser();
        this.nodeManager = new InMemoryNodeManager();
        this.pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
        this.indexManager = new IndexManager();
        this.nodeSchedulerConfig = new NodeSchedulerConfig().setIncludeCoordinator(true);
        NodeScheduler nodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                nodeManager,
                new NodeSelectionStats(),
                nodeSchedulerConfig,
                new NodeTaskMap(finalizerService));
        this.pageSinkManager = new PageSinkManager();
        CatalogManager catalogManager = new CatalogManager();
        this.transactionManager = InMemoryTransactionManager.create(
                new TransactionManagerConfig().setIdleTimeout(new Duration(1, TimeUnit.DAYS)),
                yieldExecutor,
                catalogManager,
                notificationExecutor);
        this.partitioningProviderManager = new PartitioningProviderManager();
        this.nodePartitioningManager = new NodePartitioningManager(nodeScheduler, partitioningProviderManager);
        this.planOptimizerManager = new ConnectorPlanOptimizerManager();
        this.distributedMetadataManager = new ConnectorMetadataUpdaterManager();

        this.blockEncodingManager = new BlockEncodingManager();
        featuresConfig.setIgnoreStatsCalculatorFailures(false);

        this.metadata = new MetadataManager(
                new FunctionAndTypeManager(transactionManager, blockEncodingManager, featuresConfig, new HandleResolver(), ImmutableSet.of()),
                blockEncodingManager,
                new SessionPropertyManager(
                        new SystemSessionProperties(
                                new QueryManagerConfig(),
                                new TaskManagerConfig(),
                                new MemoryManagerConfig(),
                                featuresConfig,
                                new NodeMemoryConfig(),
                                new WarningCollectorConfig())),
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                new ColumnPropertyManager(),
                new AnalyzePropertyManager(),
                transactionManager);
        this.splitManager = new SplitManager(metadata, new QueryManagerConfig(), nodeSchedulerConfig);
        this.distributedPlanChecker = new PlanChecker(featuresConfig, false);
        this.singleNodePlanChecker = new PlanChecker(featuresConfig, true);
        this.planFragmenter = new PlanFragmenter(this.metadata, this.nodePartitioningManager, new QueryManagerConfig(), sqlParser, featuresConfig);
        this.joinCompiler = new JoinCompiler(metadata, featuresConfig);
        this.pageIndexerFactory = new GroupByHashPageIndexerFactory(joinCompiler);
        this.statsNormalizer = new StatsNormalizer();
        this.scalarStatsCalculator = new ScalarStatsCalculator(metadata);
        this.filterStatsCalculator = new FilterStatsCalculator(metadata, scalarStatsCalculator, statsNormalizer);
        this.statsCalculator = createNewStatsCalculator(metadata, scalarStatsCalculator, statsNormalizer, filterStatsCalculator);
        this.taskCountEstimator = new TaskCountEstimator(() -> nodeCountForStats);
        this.costCalculator = new CostCalculatorUsingExchanges(taskCountEstimator);
        this.estimatedExchangesCostCalculator = new CostCalculatorWithEstimatedExchanges(costCalculator, taskCountEstimator);
        this.accessControl = new TestingAccessControlManager(transactionManager);
        this.pageSourceManager = new PageSourceManager();

        this.pageFunctionCompiler = new PageFunctionCompiler(metadata, 0);
        this.expressionCompiler = new ExpressionCompiler(metadata, pageFunctionCompiler);
        this.joinFilterFunctionCompiler = new JoinFilterFunctionCompiler(metadata);

        NodeInfo nodeInfo = new NodeInfo("test");
        this.connectorManager = new ConnectorManager(
                metadata,
                catalogManager,
                accessControl,
                splitManager,
                pageSourceManager,
                indexManager,
                partitioningProviderManager,
                planOptimizerManager,
                distributedMetadataManager,
                pageSinkManager,
                new HandleResolver(),
                nodeManager,
                nodeInfo,
                metadata.getFunctionAndTypeManager(),
                pageSorter,
                pageIndexerFactory,
                transactionManager,
                new RowExpressionDomainTranslator(metadata),
                new RowExpressionPredicateCompiler(metadata),
                new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager()),
                new FilterStatsCalculator(metadata, scalarStatsCalculator, statsNormalizer),
                blockEncodingManager);

        GlobalSystemConnectorFactory globalSystemConnectorFactory = new GlobalSystemConnectorFactory(ImmutableSet.of(
                new NodeSystemTable(nodeManager),
                new CatalogSystemTable(metadata, accessControl),
                new SchemaPropertiesSystemTable(transactionManager, metadata),
                new TablePropertiesSystemTable(transactionManager, metadata),
                new ColumnPropertiesSystemTable(transactionManager, metadata),
                new AnalyzePropertiesSystemTable(transactionManager, metadata),
                new TransactionsSystemTable(metadata.getFunctionAndTypeManager(), transactionManager)),
                ImmutableSet.of());

        this.pluginManager = new PluginManager(
                nodeInfo,
                new PluginManagerConfig(),
                connectorManager,
                metadata,
                new NoOpResourceGroupManager(),
                accessControl,
                new PasswordAuthenticatorManager(),
                new EventListenerManager(),
                blockEncodingManager,
                new TestingTempStorageManager(),
                new SessionPropertyDefaults(nodeInfo));

        connectorManager.addConnectorFactory(globalSystemConnectorFactory);
        connectorManager.createConnection(GlobalSystemConnector.NAME, GlobalSystemConnector.NAME, ImmutableMap.of());

        // add bogus connector for testing session properties
        catalogManager.registerCatalog(createBogusTestingCatalog(TESTING_CATALOG));

        // rewrite session to use managed SessionPropertyMetadata
        this.defaultSession = new Session(
                defaultSession.getQueryId(),
                withInitialTransaction ? Optional.of(transactionManager.beginTransaction(false)) : defaultSession.getTransactionId(),
                defaultSession.isClientTransactionSupport(),
                defaultSession.getIdentity(),
                defaultSession.getSource(),
                defaultSession.getCatalog(),
                defaultSession.getSchema(),
                defaultSession.getTraceToken(),
                defaultSession.getTimeZoneKey(),
                defaultSession.getLocale(),
                defaultSession.getRemoteUserAddress(),
                defaultSession.getUserAgent(),
                defaultSession.getClientInfo(),
                defaultSession.getClientTags(),
                defaultSession.getResourceEstimates(),
                defaultSession.getStartTime(),
                defaultSession.getSystemProperties(),
                defaultSession.getConnectorProperties(),
                defaultSession.getUnprocessedCatalogProperties(),
                metadata.getSessionPropertyManager(),
                defaultSession.getPreparedStatements(),
                defaultSession.getSessionFunctions());

        dataDefinitionTask = ImmutableMap.<Class<? extends Statement>, DataDefinitionTask<?>>builder()
                .put(CreateTable.class, new CreateTableTask())
                .put(CreateView.class, new CreateViewTask(jsonCodec(ViewDefinition.class), sqlParser, new FeaturesConfig()))
                .put(CreateFunction.class, new CreateFunctionTask(sqlParser))
                .put(AlterFunction.class, new AlterFunctionTask(sqlParser))
                .put(DropFunction.class, new DropFunctionTask(sqlParser))
                .put(DropTable.class, new DropTableTask())
                .put(DropView.class, new DropViewTask())
                .put(RenameColumn.class, new RenameColumnTask())
                .put(RenameTable.class, new RenameTableTask())
                .put(ResetSession.class, new ResetSessionTask())
                .put(SetSession.class, new SetSessionTask())
                .put(Prepare.class, new PrepareTask(sqlParser))
                .put(Deallocate.class, new DeallocateTask())
                .put(StartTransaction.class, new StartTransactionTask())
                .put(Commit.class, new CommitTask())
                .put(Rollback.class, new RollbackTask())
                .build();

        SpillerStats spillerStats = new SpillerStats();
        this.singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(blockEncodingManager, spillerStats, featuresConfig, nodeSpillConfig);
        this.partitioningSpillerFactory = new GenericPartitioningSpillerFactory(this.singleStreamSpillerFactory);
        this.spillerFactory = new GenericSpillerFactory(singleStreamSpillerFactory);
    }

    public static LocalQueryRunner queryRunnerWithInitialTransaction(Session defaultSession)
    {
        checkArgument(!defaultSession.getTransactionId().isPresent(), "Already in transaction!");
        return new LocalQueryRunner(defaultSession, new FeaturesConfig(), new NodeSpillConfig(), true, false);
    }

    public static LocalQueryRunner queryRunnerWithFakeNodeCountForStats(Session defaultSession, int nodeCount)
    {
        return new LocalQueryRunner(defaultSession, new FeaturesConfig(), new NodeSpillConfig(), false, false, nodeCount);
    }

    @Override
    public void close()
    {
        notificationExecutor.shutdownNow();
        yieldExecutor.shutdownNow();
        connectorManager.stop();
        finalizerService.destroy();
        singleStreamSpillerFactory.destroy();
    }

    @Override
    public int getNodeCount()
    {
        return 1;
    }

    public FunctionAndTypeManager getFunctionAndTypeManager()
    {
        return metadata.getFunctionAndTypeManager();
    }

    @Override
    public TransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    public SqlParser getSqlParser()
    {
        return sqlParser;
    }

    @Override
    public Metadata getMetadata()
    {
        return metadata;
    }

    @Override
    public NodePartitioningManager getNodePartitioningManager()
    {
        return nodePartitioningManager;
    }

    @Override
    public ConnectorPlanOptimizerManager getPlanOptimizerManager()
    {
        return planOptimizerManager;
    }

    public PageSourceManager getPageSourceManager()
    {
        return pageSourceManager;
    }

    public SplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public StatsCalculator getStatsCalculator()
    {
        return statsCalculator;
    }

    @Override
    public Optional<EventListener> getEventListener()
    {
        return Optional.empty();
    }

    public CostCalculator getCostCalculator()
    {
        return costCalculator;
    }

    public CostCalculator getEstimatedExchangesCostCalculator()
    {
        return estimatedExchangesCostCalculator;
    }

    @Override
    public TestingAccessControlManager getAccessControl()
    {
        return accessControl;
    }

    public ExecutorService getExecutor()
    {
        return notificationExecutor;
    }

    public ScheduledExecutorService getScheduler()
    {
        return yieldExecutor;
    }

    @Override
    public Session getDefaultSession()
    {
        return defaultSession;
    }

    public ExpressionCompiler getExpressionCompiler()
    {
        return expressionCompiler;
    }

    public void createCatalog(String catalogName, ConnectorFactory connectorFactory, Map<String, String> properties)
    {
        nodeManager.addCurrentNodeConnector(new ConnectorId(catalogName));
        connectorManager.addConnectorFactory(connectorFactory);
        connectorManager.createConnection(catalogName, connectorFactory.getName(), properties);
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        pluginManager.installPlugin(plugin);
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadFunctionNamespaceManager(String functionNamespaceManagerName, String catalogName, Map<String, String> properties)
    {
        metadata.getFunctionAndTypeManager().loadFunctionNamespaceManager(functionNamespaceManagerName, catalogName, properties);
    }

    public LocalQueryRunner printPlan()
    {
        printPlan = true;
        return this;
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
    {
        lock.readLock().lock();
        try {
            return transaction(transactionManager, accessControl)
                    .readOnly()
                    .execute(session, transactionSession -> {
                        return getMetadata().listTables(transactionSession, new QualifiedTablePrefix(catalog, schema));
                    });
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean tableExists(Session session, String table)
    {
        lock.readLock().lock();
        try {
            return transaction(transactionManager, accessControl)
                    .readOnly()
                    .execute(session, transactionSession -> {
                        return MetadataUtil.tableExists(getMetadata(), transactionSession, table);
                    });
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public MaterializedResult execute(@Language("SQL") String sql)
    {
        return execute(defaultSession, sql);
    }

    @Override
    public MaterializedResult execute(Session session, @Language("SQL") String sql)
    {
        return executeWithPlan(session, sql, new DefaultWarningCollector(
                new WarningCollectorConfig(),
                SystemSessionProperties.getWarningHandlingLevel(session))).getMaterializedResult();
    }

    @Override
    public MaterializedResultWithPlan executeWithPlan(Session session, String sql, WarningCollector warningCollector)
    {
        return inTransaction(session, transactionSession -> executeInternal(transactionSession, sql, warningCollector));
    }

    public <T> T inTransaction(Function<Session, T> transactionSessionConsumer)
    {
        return inTransaction(defaultSession, transactionSessionConsumer);
    }

    public <T> T inTransaction(Session session, Function<Session, T> transactionSessionConsumer)
    {
        return transaction(transactionManager, accessControl)
                .singleStatement()
                .execute(session, transactionSessionConsumer);
    }

    private MaterializedResultWithPlan executeInternal(Session session, @Language("SQL") String sql, WarningCollector warningCollector)
    {
        lock.readLock().lock();
        try (Closer closer = Closer.create()) {
            AtomicReference<MaterializedResult.Builder> builder = new AtomicReference<>();
            PageConsumerOutputFactory outputFactory = new PageConsumerOutputFactory(types -> {
                builder.compareAndSet(null, MaterializedResult.resultBuilder(session, types));
                return builder.get()::page;
            });

            TaskContext taskContext = TestingTaskContext.builder(notificationExecutor, yieldExecutor, session)
                    .setMaxSpillSize(nodeSpillConfig.getMaxSpillPerNode())
                    .setQueryMaxSpillSize(nodeSpillConfig.getQueryMaxSpillPerNode())
                    .build();

            Plan plan = createPlan(session, sql, warningCollector);
            List<Driver> drivers = createDrivers(session, plan, outputFactory, taskContext);
            drivers.forEach(closer::register);

            boolean done = false;
            while (!done) {
                boolean processed = false;
                for (Driver driver : drivers) {
                    if (alwaysRevokeMemory) {
                        driver.getDriverContext().getOperatorContexts().stream()
                                .filter(operatorContext -> operatorContext.getOperatorStats().getRevocableMemoryReservation().getValue() > 0)
                                .forEach(OperatorContext::requestMemoryRevoking);
                    }

                    if (!driver.isFinished()) {
                        driver.process();
                        processed = true;
                    }
                }
                done = !processed;
            }

            verify(builder.get() != null, "Output operator was not created");
            return new MaterializedResultWithPlan(builder.get().build(), plan);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Lock getExclusiveLock()
    {
        return lock.writeLock();
    }

    public List<Driver> createDrivers(@Language("SQL") String sql, OutputFactory outputFactory, TaskContext taskContext)
    {
        return createDrivers(defaultSession, sql, outputFactory, taskContext);
    }

    public List<Driver> createDrivers(Session session, @Language("SQL") String sql, OutputFactory outputFactory, TaskContext taskContext)
    {
        Plan plan = createPlan(session, sql, WarningCollector.NOOP);
        return createDrivers(session, plan, outputFactory, taskContext);
    }

    private List<Driver> createDrivers(Session session, Plan plan, OutputFactory outputFactory, TaskContext taskContext)
    {
        if (printPlan) {
            System.out.println(PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata.getFunctionAndTypeManager(), plan.getStatsAndCosts(), session, 0, false));
        }

        SubPlan subplan = createSubPlans(session, plan, true);
        if (!subplan.getChildren().isEmpty()) {
            throw new AssertionError("Expected subplan to have no children");
        }

        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
                metadata,
                Optional.empty(),
                pageSourceManager,
                indexManager,
                partitioningProviderManager,
                nodePartitioningManager,
                pageSinkManager,
                distributedMetadataManager,
                expressionCompiler,
                pageFunctionCompiler,
                joinFilterFunctionCompiler,
                new IndexJoinLookupStats(),
                new TaskManagerConfig().setTaskConcurrency(4),
                spillerFactory,
                singleStreamSpillerFactory,
                partitioningSpillerFactory,
                blockEncodingManager,
                new PagesIndex.TestingFactory(false),
                joinCompiler,
                new LookupJoinOperators(),
                new OrderingCompiler(),
                jsonCodec(TableCommitContext.class),
                new RowExpressionDeterminismEvaluator(metadata),
                new NoOpFragmentResultCacheManager(),
                new ObjectMapper());

        // plan query
        StageExecutionDescriptor stageExecutionDescriptor = subplan.getFragment().getStageExecutionDescriptor();
        StreamingPlanSection streamingPlanSection = extractStreamingSections(subplan);
        checkState(streamingPlanSection.getChildren().isEmpty(), "expected no materialized exchanges");
        StreamingSubPlan streamingSubPlan = streamingPlanSection.getPlan();
        LocalExecutionPlan localExecutionPlan = executionPlanner.plan(
                taskContext,
                stageExecutionDescriptor,
                subplan.getFragment().getRoot(),
                subplan.getFragment().getPartitioningScheme(),
                subplan.getFragment().getTableScanSchedulingOrder(),
                outputFactory,
                Optional.empty(),
                new UnsupportedRemoteSourceFactory(),
                createTableWriteInfo(streamingSubPlan, metadata, session),
                false);

        // generate sources
        List<TaskSource> sources = new ArrayList<>();
        long sequenceId = 0;
        for (TableScanNode tableScan : findTableScanNodes(subplan.getFragment().getRoot())) {
            SplitSource splitSource = splitManager.getSplits(
                    session,
                    tableScan.getTable(),
                    getSplitSchedulingStrategy(stageExecutionDescriptor, tableScan.getId()),
                    WarningCollector.NOOP);

            ImmutableSet.Builder<ScheduledSplit> scheduledSplits = ImmutableSet.builder();
            while (!splitSource.isFinished()) {
                for (Split split : getNextBatch(splitSource)) {
                    scheduledSplits.add(new ScheduledSplit(sequenceId++, tableScan.getId(), split));
                }
            }

            sources.add(new TaskSource(tableScan.getId(), scheduledSplits.build(), true));
        }

        // create drivers
        List<Driver> drivers = new ArrayList<>();
        Map<PlanNodeId, DriverFactory> driverFactoriesBySource = new HashMap<>();
        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            for (int i = 0; i < driverFactory.getDriverInstances().orElse(1); i++) {
                if (driverFactory.getSourceId().isPresent()) {
                    checkState(driverFactoriesBySource.put(driverFactory.getSourceId().get(), driverFactory) == null);
                }
                else {
                    DriverContext driverContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), false).addDriverContext();
                    Driver driver = driverFactory.createDriver(driverContext);
                    drivers.add(driver);
                }
            }
        }

        // add sources to the drivers
        Set<PlanNodeId> tableScanPlanNodeIds = ImmutableSet.copyOf(subplan.getFragment().getTableScanSchedulingOrder());
        for (TaskSource source : sources) {
            DriverFactory driverFactory = driverFactoriesBySource.get(source.getPlanNodeId());
            checkState(driverFactory != null);
            boolean partitioned = tableScanPlanNodeIds.contains(driverFactory.getSourceId().get());
            for (ScheduledSplit split : source.getSplits()) {
                DriverContext driverContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), partitioned).addDriverContext();
                Driver driver = driverFactory.createDriver(driverContext);
                driver.updateSource(new TaskSource(split.getPlanNodeId(), ImmutableSet.of(split), true));
                drivers.add(driver);
            }
        }

        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            driverFactory.noMoreDrivers();
        }

        return ImmutableList.copyOf(drivers);
    }

    private static SplitSchedulingStrategy getSplitSchedulingStrategy(StageExecutionDescriptor stageExecutionDescriptor, PlanNodeId scanNodeId)
    {
        if (stageExecutionDescriptor.isRecoverableGroupedExecution()) {
            return REWINDABLE_GROUPED_SCHEDULING;
        }
        if (stageExecutionDescriptor.isScanGroupedExecution(scanNodeId)) {
            return GROUPED_SCHEDULING;
        }
        return UNGROUPED_SCHEDULING;
    }

    public SubPlan createSubPlans(Session session, Plan plan, boolean forceSingleNode)
    {
        return planFragmenter.createSubPlans(
                session,
                plan,
                forceSingleNode,
                new PlanNodeIdAllocator()
                {
                    @Override
                    public PlanNodeId getNextId()
                    {
                        throw new UnsupportedOperationException();
                    }
                },
                WarningCollector.NOOP);
    }

    @Override
    public Plan createPlan(Session session, @Language("SQL") String sql, WarningCollector warningCollector)
    {
        return createPlan(session, sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, warningCollector);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, LogicalPlanner.Stage stage, WarningCollector warningCollector)
    {
        return createPlan(session, sql, stage, true, warningCollector);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, LogicalPlanner.Stage stage, boolean forceSingleNode, WarningCollector warningCollector)
    {
        PreparedQuery preparedQuery = new QueryPreparer(sqlParser).prepareQuery(session, sql, warningCollector);

        assertFormattedSql(sqlParser, createParsingOptions(session), preparedQuery.getStatement());

        return createPlan(session, sql, getPlanOptimizers(forceSingleNode), stage, warningCollector);
    }

    public List<PlanOptimizer> getPlanOptimizers(boolean forceSingleNode)
    {
        FeaturesConfig featuresConfig = new FeaturesConfig()
                .setDistributedIndexJoinsEnabled(false)
                .setOptimizeHashGeneration(true);
        return new PlanOptimizers(
                metadata,
                sqlParser,
                forceSingleNode,
                new MBeanExporter(new TestingMBeanServer()),
                splitManager,
                planOptimizerManager,
                pageSourceManager,
                statsCalculator,
                costCalculator,
                estimatedExchangesCostCalculator,
                new CostComparator(featuresConfig),
                taskCountEstimator).getPlanningTimeOptimizers();
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, List<PlanOptimizer> optimizers, WarningCollector warningCollector)
    {
        return createPlan(session, sql, optimizers, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, warningCollector);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, List<PlanOptimizer> optimizers, LogicalPlanner.Stage stage, WarningCollector warningCollector)
    {
        Statement wrappedStatement = sqlParser.createStatement(sql, createParsingOptions(session, warningCollector));
        PreparedQuery preparedQuery = new QueryPreparer(sqlParser).prepareQuery(session, wrappedStatement, warningCollector);

        assertFormattedSql(sqlParser, createParsingOptions(session), preparedQuery.getStatement());

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        QueryExplainer queryExplainer = new QueryExplainer(
                optimizers,
                planFragmenter,
                metadata,
                accessControl,
                sqlParser,
                statsCalculator,
                costCalculator,
                dataDefinitionTask,
                distributedPlanChecker);
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, accessControl, Optional.of(queryExplainer), preparedQuery.getParameters(), warningCollector);

        LogicalPlanner logicalPlanner = new LogicalPlanner(wrappedStatement instanceof Explain, session, optimizers, singleNodePlanChecker, idAllocator, metadata, sqlParser, statsCalculator, costCalculator, warningCollector);

        Analysis analysis = analyzer.analyze(preparedQuery.getStatement());
        return logicalPlanner.plan(analysis, stage);
    }

    private static List<Split> getNextBatch(SplitSource splitSource)
    {
        return getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), 1000)).getSplits();
    }

    private static List<TableScanNode> findTableScanNodes(PlanNode node)
    {
        return searchFrom(node)
                .where(TableScanNode.class::isInstance)
                .findAll();
    }

    private static class UnsupportedRemoteSourceFactory
            implements RemoteSourceFactory
    {
        @Override
        public SourceOperatorFactory createRemoteSource(Session session, int operatorId, PlanNodeId planNodeId, List<Type> types)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceOperatorFactory createMergeRemoteSource(Session session, int operatorId, PlanNodeId planNodeId, List<Type> types, List<Integer> outputChannels, List<Integer> sortChannels, List<SortOrder> sortOrder)
        {
            throw new UnsupportedOperationException();
        }
    }
}
