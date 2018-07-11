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

import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.TaskSource;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.connector.system.CatalogSystemTable;
import com.facebook.presto.connector.system.GlobalSystemConnector;
import com.facebook.presto.connector.system.GlobalSystemConnectorFactory;
import com.facebook.presto.connector.system.NodeSystemTable;
import com.facebook.presto.connector.system.SchemaPropertiesSystemTable;
import com.facebook.presto.connector.system.TablePropertiesSystemTable;
import com.facebook.presto.connector.system.TransactionsSystemTable;
import com.facebook.presto.cost.CoefficientBasedStatsCalculator;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostCalculatorUsingExchanges;
import com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.SelectingStatsCalculator;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.CommitTask;
import com.facebook.presto.execution.CreateTableTask;
import com.facebook.presto.execution.CreateViewTask;
import com.facebook.presto.execution.DataDefinitionTask;
import com.facebook.presto.execution.DeallocateTask;
import com.facebook.presto.execution.DropTableTask;
import com.facebook.presto.execution.DropViewTask;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.PrepareTask;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.RenameColumnTask;
import com.facebook.presto.execution.RenameTableTask;
import com.facebook.presto.execution.ResetSessionTask;
import com.facebook.presto.execution.RollbackTask;
import com.facebook.presto.execution.SetPathTask;
import com.facebook.presto.execution.SetSessionTask;
import com.facebook.presto.execution.StartTransactionTask;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.resourceGroups.NoOpResourceGroupManager;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.PipelineExecutionStrategy;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.server.NoOpSessionSupplier;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.server.PluginManagerConfig;
import com.facebook.presto.server.ServerMainModule;
import com.facebook.presto.server.security.PasswordAuthenticatorManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
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
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanOptimizers;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.facebook.presto.sql.planner.sanity.PlanSanityChecker;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.SetPath;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.testing.PageConsumerOperator.PageConsumerOutputFactory;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.transaction.TransactionManagerConfig;
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.airlift.node.NodeInfo;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static com.facebook.presto.execution.SqlQueryManager.unwrapExecuteStatement;
import static com.facebook.presto.execution.SqlQueryManager.validateParameters;
import static com.facebook.presto.operator.PipelineExecutionStrategy.GROUPED_EXECUTION;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.GROUPED_SCHEDULING;
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
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Collections.emptyList;
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
    private final InMemoryNodeManager nodeManager;
    private final TypeRegistry typeRegistry;
    private final PageSorter pageSorter;
    private final PageIndexerFactory pageIndexerFactory;
    private final MetadataManager metadata;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final CostCalculator estimatedExchangesCostCalculator;
    private final TestingAccessControlManager accessControl;
    private final SplitManager splitManager;
    private final BlockEncodingManager blockEncodingManager;
    private final PageSourceManager pageSourceManager;
    private final IndexManager indexManager;
    private final NodePartitioningManager nodePartitioningManager;
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
    private boolean printPlan;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public LocalQueryRunner(Session defaultSession)
    {
        this(
                defaultSession,
                new FeaturesConfig(),
                false,
                false);
    }

    public LocalQueryRunner(Session defaultSession, boolean alwaysRevokeMemory)
    {
        this(defaultSession,
                new FeaturesConfig(),
                false,
                alwaysRevokeMemory);
    }

    public LocalQueryRunner(Session defaultSession, FeaturesConfig featuresConfig)
    {
        this(defaultSession, featuresConfig, false, false);
    }

    public LocalQueryRunner(Session defaultSession, FeaturesConfig featuresConfig, boolean withInitialTransaction, boolean alwaysRevokeMemory)
    {
        this(defaultSession, featuresConfig, new NodeSpillConfig(), withInitialTransaction, alwaysRevokeMemory);
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
        this.typeRegistry = new TypeRegistry();
        this.pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
        this.indexManager = new IndexManager();
        NodeScheduler nodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                nodeManager,
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(finalizerService));
        this.pageSinkManager = new PageSinkManager();
        CatalogManager catalogManager = new CatalogManager();
        this.transactionManager = TransactionManager.create(
                new TransactionManagerConfig().setIdleTimeout(new Duration(1, TimeUnit.DAYS)),
                yieldExecutor,
                catalogManager,
                notificationExecutor);
        this.nodePartitioningManager = new NodePartitioningManager(nodeScheduler);

        this.splitManager = new SplitManager(new QueryManagerConfig());
        this.blockEncodingManager = new BlockEncodingManager(typeRegistry);
        this.metadata = new MetadataManager(
                featuresConfig,
                typeRegistry,
                blockEncodingManager,
                new SessionPropertyManager(new SystemSessionProperties(new QueryManagerConfig(), new TaskManagerConfig(), new MemoryManagerConfig(), featuresConfig)),
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                transactionManager);
        this.joinCompiler = new JoinCompiler(metadata, featuresConfig);
        this.pageIndexerFactory = new GroupByHashPageIndexerFactory(joinCompiler);
        this.statsCalculator = new SelectingStatsCalculator(
                new CoefficientBasedStatsCalculator(metadata),
                ServerMainModule.createNewStatsCalculator(metadata));
        this.costCalculator = new CostCalculatorUsingExchanges(() -> nodeCountForStats);
        this.estimatedExchangesCostCalculator = new CostCalculatorWithEstimatedExchanges(costCalculator, () -> nodeCountForStats);
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
                nodePartitioningManager,
                pageSinkManager,
                new HandleResolver(),
                nodeManager,
                nodeInfo,
                typeRegistry,
                pageSorter,
                pageIndexerFactory,
                transactionManager);

        GlobalSystemConnectorFactory globalSystemConnectorFactory = new GlobalSystemConnectorFactory(ImmutableSet.of(
                new NodeSystemTable(nodeManager),
                new CatalogSystemTable(metadata, accessControl),
                new SchemaPropertiesSystemTable(transactionManager, metadata),
                new TablePropertiesSystemTable(transactionManager, metadata),
                new TransactionsSystemTable(typeRegistry, transactionManager)),
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
                new NoOpSessionSupplier(),
                typeRegistry);

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
                defaultSession.getPath(),
                defaultSession.getTraceToken(),
                defaultSession.getTimeZoneKey(),
                defaultSession.getLocale(),
                defaultSession.getRemoteUserAddress(),
                defaultSession.getUserAgent(),
                defaultSession.getClientInfo(),
                defaultSession.getClientTags(),
                defaultSession.getClientCapabilities(),
                defaultSession.getResourceEstimates(),
                defaultSession.getStartTime(),
                defaultSession.getSystemProperties(),
                defaultSession.getConnectorProperties(),
                defaultSession.getUnprocessedCatalogProperties(),
                metadata.getSessionPropertyManager(),
                defaultSession.getPreparedStatements());

        dataDefinitionTask = ImmutableMap.<Class<? extends Statement>, DataDefinitionTask<?>>builder()
                .put(CreateTable.class, new CreateTableTask())
                .put(CreateView.class, new CreateViewTask(jsonCodec(ViewDefinition.class), sqlParser, new FeaturesConfig()))
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
                .put(SetPath.class, new SetPathTask())
                .build();

        SpillerStats spillerStats = new SpillerStats();
        this.singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(blockEncodingManager, spillerStats, featuresConfig);
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

    public TypeRegistry getTypeManager()
    {
        return typeRegistry;
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
        return inTransaction(session, transactionSession -> executeInternal(transactionSession, sql));
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

    private MaterializedResult executeInternal(Session session, @Language("SQL") String sql)
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

            List<Driver> drivers = createDrivers(session, sql, outputFactory, taskContext);
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
            return builder.get().build();
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
        Plan plan = createPlan(session, sql);

        if (printPlan) {
            System.out.println(PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata.getFunctionRegistry(), statsCalculator, estimatedExchangesCostCalculator, session));
        }

        SubPlan subplan = PlanFragmenter.createSubPlans(session, metadata, nodePartitioningManager, plan, true);
        if (!subplan.getChildren().isEmpty()) {
            throw new AssertionError("Expected subplan to have no children");
        }

        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
                metadata,
                sqlParser,
                statsCalculator,
                costCalculator,
                Optional.empty(),
                pageSourceManager,
                indexManager,
                nodePartitioningManager,
                pageSinkManager,
                null,
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
                new OrderingCompiler());

        // plan query
        PipelineExecutionStrategy pipelineExecutionStrategy = subplan.getFragment().getPipelineExecutionStrategy();
        LocalExecutionPlan localExecutionPlan = executionPlanner.plan(
                taskContext,
                pipelineExecutionStrategy == GROUPED_EXECUTION,
                subplan.getFragment().getRoot(),
                subplan.getFragment().getPartitioningScheme().getOutputLayout(),
                plan.getTypes(),
                subplan.getFragment().getPartitionedSources(),
                outputFactory);

        // generate sources
        List<TaskSource> sources = new ArrayList<>();
        long sequenceId = 0;
        for (TableScanNode tableScan : findTableScanNodes(subplan.getFragment().getRoot())) {
            TableLayoutHandle layout = tableScan.getLayout().get();

            SplitSource splitSource = splitManager.getSplits(
                    session,
                    layout,
                    pipelineExecutionStrategy == GROUPED_EXECUTION ? GROUPED_SCHEDULING : UNGROUPED_SCHEDULING);

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
                    DriverContext driverContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver()).addDriverContext();
                    Driver driver = driverFactory.createDriver(driverContext);
                    drivers.add(driver);
                }
            }
        }

        // add sources to the drivers
        for (TaskSource source : sources) {
            DriverFactory driverFactory = driverFactoriesBySource.get(source.getPlanNodeId());
            checkState(driverFactory != null);
            for (ScheduledSplit split : source.getSplits()) {
                DriverContext driverContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver()).addDriverContext();
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

    @Override
    public Plan createPlan(Session session, @Language("SQL") String sql)
    {
        return createPlan(session, sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, LogicalPlanner.Stage stage)
    {
        return createPlan(session, sql, stage, true);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, LogicalPlanner.Stage stage, boolean forceSingleNode)
    {
        Statement statement = unwrapExecuteStatement(sqlParser.createStatement(sql, createParsingOptions(session)), sqlParser, session);

        assertFormattedSql(sqlParser, createParsingOptions(session), statement);

        return createPlan(session, sql, getPlanOptimizers(forceSingleNode), stage);
    }

    public List<PlanOptimizer> getPlanOptimizers(boolean forceSingleNode)
    {
        FeaturesConfig featuresConfig = new FeaturesConfig()
                .setDistributedIndexJoinsEnabled(false)
                .setOptimizeHashGeneration(true);
        return new PlanOptimizers(
                metadata,
                sqlParser,
                featuresConfig,
                forceSingleNode,
                new MBeanExporter(new TestingMBeanServer()),
                statsCalculator,
                costCalculator,
                estimatedExchangesCostCalculator,
                new CostComparator(featuresConfig)).get();
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, List<PlanOptimizer> optimizers)
    {
        return createPlan(session, sql, optimizers, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, List<PlanOptimizer> optimizers, LogicalPlanner.Stage stage)
    {
        Statement wrapped = sqlParser.createStatement(sql, createParsingOptions(session));
        Statement statement = unwrapExecuteStatement(wrapped, sqlParser, session);

        List<Expression> parameters = emptyList();
        if (wrapped instanceof Execute) {
            parameters = ((Execute) wrapped).getParameters();
        }
        validateParameters(statement, parameters);

        assertFormattedSql(sqlParser, createParsingOptions(session), statement);

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        QueryExplainer queryExplainer = new QueryExplainer(
                optimizers,
                metadata,
                nodePartitioningManager,
                accessControl,
                sqlParser,
                statsCalculator,
                costCalculator,
                dataDefinitionTask);
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, accessControl, Optional.of(queryExplainer), parameters);

        LogicalPlanner logicalPlanner = new LogicalPlanner(session, optimizers, new PlanSanityChecker(true), idAllocator, metadata, sqlParser);

        Analysis analysis = analyzer.analyze(statement);
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
}
