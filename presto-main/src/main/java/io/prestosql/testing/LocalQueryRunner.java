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
package io.prestosql.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.prestosql.GroupByHashPageIndexerFactory;
import io.prestosql.PagesIndexPageSorter;
import io.prestosql.ScheduledSplit;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.TaskSource;
import io.prestosql.block.BlockEncodingManager;
import io.prestosql.connector.ConnectorId;
import io.prestosql.connector.ConnectorManager;
import io.prestosql.connector.system.CatalogSystemTable;
import io.prestosql.connector.system.ColumnPropertiesSystemTable;
import io.prestosql.connector.system.GlobalSystemConnector;
import io.prestosql.connector.system.GlobalSystemConnectorFactory;
import io.prestosql.connector.system.NodeSystemTable;
import io.prestosql.connector.system.SchemaPropertiesSystemTable;
import io.prestosql.connector.system.TablePropertiesSystemTable;
import io.prestosql.connector.system.TransactionsSystemTable;
import io.prestosql.cost.CostCalculator;
import io.prestosql.cost.CostCalculatorUsingExchanges;
import io.prestosql.cost.CostCalculatorWithEstimatedExchanges;
import io.prestosql.cost.CostComparator;
import io.prestosql.cost.StatsCalculator;
import io.prestosql.cost.TaskCountEstimator;
import io.prestosql.eventlistener.EventListenerManager;
import io.prestosql.execution.CommitTask;
import io.prestosql.execution.CreateTableTask;
import io.prestosql.execution.CreateViewTask;
import io.prestosql.execution.DataDefinitionTask;
import io.prestosql.execution.DeallocateTask;
import io.prestosql.execution.DropTableTask;
import io.prestosql.execution.DropViewTask;
import io.prestosql.execution.Lifespan;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.execution.PrepareTask;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.execution.QueryPreparer;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.execution.RenameColumnTask;
import io.prestosql.execution.RenameTableTask;
import io.prestosql.execution.ResetSessionTask;
import io.prestosql.execution.RollbackTask;
import io.prestosql.execution.SetPathTask;
import io.prestosql.execution.SetSessionTask;
import io.prestosql.execution.StartTransactionTask;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.execution.resourceGroups.NoOpResourceGroupManager;
import io.prestosql.execution.scheduler.LegacyNetworkTopology;
import io.prestosql.execution.scheduler.NodeScheduler;
import io.prestosql.execution.scheduler.NodeSchedulerConfig;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.index.IndexManager;
import io.prestosql.memory.MemoryManagerConfig;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.ColumnPropertyManager;
import io.prestosql.metadata.HandleResolver;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.MetadataUtil;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.QualifiedTablePrefix;
import io.prestosql.metadata.SchemaPropertyManager;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.metadata.Split;
import io.prestosql.metadata.TableLayoutHandle;
import io.prestosql.metadata.TablePropertyManager;
import io.prestosql.metadata.ViewDefinition;
import io.prestosql.operator.Driver;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.DriverFactory;
import io.prestosql.operator.LookupJoinOperators;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OutputFactory;
import io.prestosql.operator.PagesIndex;
import io.prestosql.operator.StageExecutionStrategy;
import io.prestosql.operator.TaskContext;
import io.prestosql.operator.index.IndexJoinLookupStats;
import io.prestosql.server.PluginManager;
import io.prestosql.server.PluginManagerConfig;
import io.prestosql.server.SessionPropertyDefaults;
import io.prestosql.server.security.PasswordAuthenticatorManager;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spiller.FileSingleStreamSpillerFactory;
import io.prestosql.spiller.GenericPartitioningSpillerFactory;
import io.prestosql.spiller.GenericSpillerFactory;
import io.prestosql.spiller.NodeSpillConfig;
import io.prestosql.spiller.PartitioningSpillerFactory;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.spiller.SpillerStats;
import io.prestosql.split.PageSinkManager;
import io.prestosql.split.PageSourceManager;
import io.prestosql.split.SplitManager;
import io.prestosql.split.SplitSource;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.Analyzer;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.LocalExecutionPlanner;
import io.prestosql.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import io.prestosql.sql.planner.LogicalPlanner;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.PlanFragmenter;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.PlanOptimizers;
import io.prestosql.sql.planner.SubPlan;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.planPrinter.PlanPrinter;
import io.prestosql.sql.planner.sanity.PlanSanityChecker;
import io.prestosql.sql.tree.Commit;
import io.prestosql.sql.tree.CreateTable;
import io.prestosql.sql.tree.CreateView;
import io.prestosql.sql.tree.Deallocate;
import io.prestosql.sql.tree.DropTable;
import io.prestosql.sql.tree.DropView;
import io.prestosql.sql.tree.Prepare;
import io.prestosql.sql.tree.RenameColumn;
import io.prestosql.sql.tree.RenameTable;
import io.prestosql.sql.tree.ResetSession;
import io.prestosql.sql.tree.Rollback;
import io.prestosql.sql.tree.SetPath;
import io.prestosql.sql.tree.SetSession;
import io.prestosql.sql.tree.StartTransaction;
import io.prestosql.sql.tree.Statement;
import io.prestosql.testing.PageConsumerOperator.PageConsumerOutputFactory;
import io.prestosql.transaction.InMemoryTransactionManager;
import io.prestosql.transaction.TransactionManager;
import io.prestosql.transaction.TransactionManagerConfig;
import io.prestosql.type.TypeRegistry;
import io.prestosql.util.FinalizerService;
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.prestosql.cost.StatsCalculatorModule.createNewStatsCalculator;
import static io.prestosql.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.GROUPED_SCHEDULING;
import static io.prestosql.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.sql.ParsingUtil.createParsingOptions;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.prestosql.sql.testing.TreeAssertions.assertFormattedSql;
import static io.prestosql.testing.TestingSession.TESTING_CATALOG;
import static io.prestosql.testing.TestingSession.createBogusTestingCatalog;
import static io.prestosql.transaction.TransactionBuilder.transaction;
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
    private final TypeRegistry typeRegistry;
    private final PageSorter pageSorter;
    private final PageIndexerFactory pageIndexerFactory;
    private final MetadataManager metadata;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final CostCalculator estimatedExchangesCostCalculator;
    private final TaskCountEstimator taskCountEstimator;
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
    private final NodeSchedulerConfig nodeSchedulerConfig;
    private boolean printPlan;

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
        this.typeRegistry = new TypeRegistry();
        this.pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
        this.indexManager = new IndexManager();
        this.nodeSchedulerConfig = new NodeSchedulerConfig().setIncludeCoordinator(true);
        NodeScheduler nodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                nodeManager,
                nodeSchedulerConfig,
                new NodeTaskMap(finalizerService));
        this.pageSinkManager = new PageSinkManager();
        CatalogManager catalogManager = new CatalogManager();
        this.transactionManager = InMemoryTransactionManager.create(
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
                new ColumnPropertyManager(),
                transactionManager);
        this.planFragmenter = new PlanFragmenter(this.metadata, this.nodePartitioningManager, new QueryManagerConfig());
        this.joinCompiler = new JoinCompiler(metadata, featuresConfig);
        this.pageIndexerFactory = new GroupByHashPageIndexerFactory(joinCompiler);
        this.statsCalculator = createNewStatsCalculator(metadata);
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
                new ColumnPropertiesSystemTable(transactionManager, metadata),
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
                new SessionPropertyDefaults(nodeInfo),
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
        return executeWithPlan(session, sql, WarningCollector.NOOP).getMaterializedResult();
    }

    @Override
    public MaterializedResultWithPlan executeWithPlan(Session session, String sql, WarningCollector warningCollector)
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

    private MaterializedResultWithPlan executeInternal(Session session, @Language("SQL") String sql)
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

            Plan plan = createPlan(session, sql, WarningCollector.NOOP);
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
            System.out.println(PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata.getFunctionRegistry(), plan.getStatsAndCosts(), session, 0, false));
        }

        SubPlan subplan = planFragmenter.createSubPlans(session, plan, true);
        if (!subplan.getChildren().isEmpty()) {
            throw new AssertionError("Expected subplan to have no children");
        }

        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
                metadata,
                sqlParser,
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
        StageExecutionStrategy stageExecutionStrategy = subplan.getFragment().getStageExecutionStrategy();
        LocalExecutionPlan localExecutionPlan = executionPlanner.plan(
                taskContext,
                stageExecutionStrategy,
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
                    stageExecutionStrategy.isGroupedExecution(tableScan.getId()) ? GROUPED_SCHEDULING : UNGROUPED_SCHEDULING);

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
        ImmutableSet<PlanNodeId> partitionedSources = ImmutableSet.copyOf(subplan.getFragment().getPartitionedSources());
        for (TaskSource source : sources) {
            DriverFactory driverFactory = driverFactoriesBySource.get(source.getPlanNodeId());
            checkState(driverFactory != null);
            boolean partitioned = partitionedSources.contains(driverFactory.getSourceId().get());
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
        PreparedQuery preparedQuery = new QueryPreparer(sqlParser).prepareQuery(session, sql);

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
                featuresConfig,
                forceSingleNode,
                new MBeanExporter(new TestingMBeanServer()),
                splitManager,
                pageSourceManager,
                statsCalculator,
                costCalculator,
                estimatedExchangesCostCalculator,
                new CostComparator(featuresConfig),
                taskCountEstimator).get();
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, List<PlanOptimizer> optimizers, WarningCollector warningCollector)
    {
        return createPlan(session, sql, optimizers, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, warningCollector);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, List<PlanOptimizer> optimizers, LogicalPlanner.Stage stage, WarningCollector warningCollector)
    {
        PreparedQuery preparedQuery = new QueryPreparer(sqlParser).prepareQuery(session, sql);

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
                dataDefinitionTask);
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, accessControl, Optional.of(queryExplainer), preparedQuery.getParameters(), warningCollector);

        LogicalPlanner logicalPlanner = new LogicalPlanner(session, optimizers, new PlanSanityChecker(true), idAllocator, metadata, sqlParser, statsCalculator, costCalculator, warningCollector);

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
}
