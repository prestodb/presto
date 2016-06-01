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

import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.Session;
import com.facebook.presto.TaskSource;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.connector.system.CatalogSystemTable;
import com.facebook.presto.connector.system.GlobalSystemConnector;
import com.facebook.presto.connector.system.GlobalSystemConnectorFactory;
import com.facebook.presto.connector.system.NodeSystemTable;
import com.facebook.presto.connector.system.TablePropertiesSystemTable;
import com.facebook.presto.connector.system.TransactionsSystemTable;
import com.facebook.presto.execution.CommitTask;
import com.facebook.presto.execution.CreateTableTask;
import com.facebook.presto.execution.CreateViewTask;
import com.facebook.presto.execution.DataDefinitionTask;
import com.facebook.presto.execution.DeallocateTask;
import com.facebook.presto.execution.DropTableTask;
import com.facebook.presto.execution.DropViewTask;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.PrepareTask;
import com.facebook.presto.execution.RenameColumnTask;
import com.facebook.presto.execution.RenameTableTask;
import com.facebook.presto.execution.ResetSessionTask;
import com.facebook.presto.execution.RollbackTask;
import com.facebook.presto.execution.SetSessionTask;
import com.facebook.presto.execution.StartTransactionTask;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.GenericPageProcessor;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.PageSourceOperator;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.ProjectionFunctions;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.testing.PageConsumerOperator.PageConsumerOutputFactory;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.transaction.TransactionManagerConfig;
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.type.TypeUtils;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;

import javax.inject.Provider;

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

import static com.facebook.presto.execution.SqlQueryManager.unwrapExecuteStatement;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.testing.TreeAssertions.assertFormattedSql;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class LocalQueryRunner
        implements QueryRunner
{
    private final Session defaultSession;
    private final ExecutorService executor;
    private final ScheduledExecutorService transactionCheckExecutor;
    private final FinalizerService finalizerService;

    private final SqlParser sqlParser;
    private final InMemoryNodeManager nodeManager;
    private final TypeRegistry typeRegistry;
    private final MetadataManager metadata;
    private final TestingAccessControlManager accessControl;
    private final TestingEventListenerManager eventListener;
    private final SplitManager splitManager;
    private final BlockEncodingSerde blockEncodingSerde;
    private final PageSourceManager pageSourceManager;
    private final IndexManager indexManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final PageSinkManager pageSinkManager;
    private final TransactionManager transactionManager;

    private final ExpressionCompiler compiler;
    private final ConnectorManager connectorManager;
    private final ImmutableMap<Class<? extends Statement>, DataDefinitionTask<?>> dataDefinitionTask;

    private boolean printPlan;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public LocalQueryRunner(Session defaultSession)
    {
        this(defaultSession, new FeaturesConfig().setExperimentalSyntaxEnabled(true), false);
    }

    public LocalQueryRunner(Session defaultSession, FeaturesConfig featuresConfig)
    {
        this(defaultSession, featuresConfig, false);
    }

    private LocalQueryRunner(Session defaultSession, FeaturesConfig featuresConfig, boolean withInitialTransaction)
    {
        requireNonNull(defaultSession, "defaultSession is null");
        checkArgument(!defaultSession.getTransactionId().isPresent() || !withInitialTransaction, "Already in transaction");

        this.executor = newCachedThreadPool(daemonThreadsNamed("local-query-runner-%s"));
        this.transactionCheckExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("transaction-idle-check"));
        this.finalizerService = new FinalizerService();
        finalizerService.start();

        this.sqlParser = new SqlParser();
        this.nodeManager = new InMemoryNodeManager();
        this.typeRegistry = new TypeRegistry();
        this.indexManager = new IndexManager();
        NodeScheduler nodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                nodeManager,
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(finalizerService));
        this.pageSinkManager = new PageSinkManager();
        this.transactionManager = TransactionManager.create(
                new TransactionManagerConfig().setIdleTimeout(new Duration(1, TimeUnit.DAYS)),
                transactionCheckExecutor,
                executor);
        this.nodePartitioningManager = new NodePartitioningManager(nodeScheduler);

        this.splitManager = new SplitManager();
        this.blockEncodingSerde = new BlockEncodingManager(typeRegistry);
        this.metadata = new MetadataManager(
                featuresConfig,
                typeRegistry,
                blockEncodingSerde,
                new SessionPropertyManager(),
                new TablePropertyManager(),
                transactionManager);
        this.accessControl = new TestingAccessControlManager(transactionManager);
        this.eventListener = new TestingEventListenerManager();
        this.pageSourceManager = new PageSourceManager();

        this.compiler = new ExpressionCompiler(metadata);

        this.connectorManager = new ConnectorManager(
                metadata,
                accessControl,
                splitManager,
                pageSourceManager,
                indexManager,
                nodePartitioningManager,
                pageSinkManager,
                new HandleResolver(),
                nodeManager,
                transactionManager);

        GlobalSystemConnectorFactory globalSystemConnectorFactory = new GlobalSystemConnectorFactory(ImmutableSet.of(
                new NodeSystemTable(nodeManager),
                new CatalogSystemTable(metadata),
                new TablePropertiesSystemTable(metadata),
                new TransactionsSystemTable(typeRegistry, transactionManager)),
                ImmutableSet.of());

        connectorManager.addConnectorFactory(globalSystemConnectorFactory);
        connectorManager.createConnection(GlobalSystemConnector.NAME, GlobalSystemConnector.NAME, ImmutableMap.of());

        // rewrite session to use managed SessionPropertyMetadata
        this.defaultSession = new Session(
                defaultSession.getQueryId(),
                withInitialTransaction ? Optional.of(transactionManager.beginTransaction(false)) : defaultSession.getTransactionId(),
                defaultSession.isClientTransactionSupport(),
                defaultSession.getIdentity(),
                defaultSession.getSource(),
                defaultSession.getCatalog(),
                defaultSession.getSchema(),
                defaultSession.getTimeZoneKey(),
                defaultSession.getLocale(),
                defaultSession.getRemoteUserAddress(),
                defaultSession.getUserAgent(),
                defaultSession.getStartTime(),
                defaultSession.getSystemProperties(),
                defaultSession.getCatalogProperties(),
                metadata.getSessionPropertyManager(),
                defaultSession.getPreparedStatements());

        dataDefinitionTask = ImmutableMap.<Class<? extends Statement>, DataDefinitionTask<?>>builder()
                .put(CreateTable.class, new CreateTableTask())
                .put(CreateView.class, new CreateViewTask(jsonCodec(ViewDefinition.class), sqlParser, accessControl, new FeaturesConfig()))
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
    }

    public static LocalQueryRunner queryRunnerWithInitialTransaction(Session defaultSession)
    {
        checkArgument(!defaultSession.getTransactionId().isPresent(), "Already in transaction!");
        return new LocalQueryRunner(defaultSession, new FeaturesConfig().setExperimentalSyntaxEnabled(true), true);
    }

    @Override
    public void close()
    {
        executor.shutdownNow();
        transactionCheckExecutor.shutdownNow();
        connectorManager.stop();
        finalizerService.destroy();
    }

    @Override
    public int getNodeCount()
    {
        return 1;
    }

    public InMemoryNodeManager getNodeManager()
    {
        return nodeManager;
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

    @Override
    public Metadata getMetadata()
    {
        return metadata;
    }

    @Override
    public TestingAccessControlManager getAccessControl()
    {
        return accessControl;
    }

    public ExecutorService getExecutor()
    {
        return executor;
    }

    @Override
    public Session getDefaultSession()
    {
        return defaultSession;
    }

    public void createCatalog(String catalogName, ConnectorFactory connectorFactory, Map<String, String> properties)
    {
        nodeManager.addCurrentNodeDatasource(catalogName);
        connectorManager.addConnectorFactory(connectorFactory);
        connectorManager.createConnection(catalogName, connectorFactory.getName(), properties);
    }

    @Deprecated
    public void createCatalog(String catalogName, com.facebook.presto.spi.ConnectorFactory connectorFactory, Map<String, String> properties)
    {
        nodeManager.addCurrentNodeDatasource(catalogName);
        connectorManager.addConnectorFactory(connectorFactory);
        connectorManager.createConnection(catalogName, connectorFactory.getName(), properties);
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        throw new UnsupportedOperationException();
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
            return transaction(transactionManager)
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
            return transaction(transactionManager)
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
        return transaction(transactionManager)
                .singleStatement()
                .execute(session, transactionSessionConsumer);
    }

    private MaterializedResult executeInternal(Session session, @Language("SQL") String sql)
    {
        lock.readLock().lock();
        try {
            AtomicReference<MaterializedResult.Builder> builder = new AtomicReference<>();
            PageConsumerOutputFactory outputFactory = new PageConsumerOutputFactory(types -> {
                builder.compareAndSet(null, MaterializedResult.resultBuilder(session, types));
                return builder.get()::page;
            });

            TaskContext taskContext = createTaskContext(executor, session);
            List<Driver> drivers = createDrivers(session, sql, outputFactory, taskContext);

            boolean done = false;
            while (!done) {
                boolean processed = false;
                for (Driver driver : drivers) {
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
            System.out.println(PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata, session));
        }

        SubPlan subplan = new PlanFragmenter().createSubPlans(session, metadata, plan);
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
                compiler,
                new IndexJoinLookupStats(),
                new CompilerConfig().setInterpreterEnabled(false), // make sure tests fail if compiler breaks
                new TaskManagerConfig().setTaskConcurrency(4));

        // plan query
        LocalExecutionPlan localExecutionPlan = executionPlanner.plan(
                session,
                subplan.getFragment().getRoot(),
                subplan.getFragment().getPartitioningScheme().getOutputLayout(),
                plan.getTypes(),
                outputFactory);

        // generate sources
        List<TaskSource> sources = new ArrayList<>();
        long sequenceId = 0;
        for (TableScanNode tableScan : findTableScanNodes(subplan.getFragment().getRoot())) {
            TableLayoutHandle layout = tableScan.getLayout().get();

            SplitSource splitSource = splitManager.getSplits(session, layout);

            ImmutableSet.Builder<ScheduledSplit> scheduledSplits = ImmutableSet.builder();
            while (!splitSource.isFinished()) {
                for (Split split : getFutureValue(splitSource.getNextBatch(1000))) {
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
                    DriverContext driverContext = taskContext.addPipelineContext(driverFactory.isInputDriver(), driverFactory.isOutputDriver()).addDriverContext();
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
                DriverContext driverContext = taskContext.addPipelineContext(driverFactory.isInputDriver(), driverFactory.isOutputDriver()).addDriverContext();
                Driver driver = driverFactory.createDriver(driverContext);
                driver.updateSource(new TaskSource(split.getPlanNodeId(), ImmutableSet.of(split), true));
                drivers.add(driver);
            }
        }

        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            driverFactory.close();
        }

        return ImmutableList.copyOf(drivers);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql)
    {
        FeaturesConfig featuresConfig = new FeaturesConfig()
                .setExperimentalSyntaxEnabled(true)
                .setDistributedIndexJoinsEnabled(false)
                .setOptimizeHashGeneration(true);
        PlanOptimizersFactory planOptimizersFactory = new PlanOptimizersFactory(metadata, sqlParser, featuresConfig, true);
        return createPlan(session, sql, featuresConfig, planOptimizersFactory);
    }

    public Plan createPlan(Session session, @Language("SQL") String sql, FeaturesConfig featuresConfig, Provider<List<PlanOptimizer>> optimizerProvider)
    {
        Statement statement = unwrapExecuteStatement(sqlParser.createStatement(sql), sqlParser, session);

        assertFormattedSql(sqlParser, statement);

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        QueryExplainer queryExplainer = new QueryExplainer(
                optimizerProvider.get(),
                metadata,
                accessControl,
                sqlParser,
                dataDefinitionTask,
                featuresConfig.isExperimentalSyntaxEnabled());
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, accessControl, Optional.of(queryExplainer), featuresConfig.isExperimentalSyntaxEnabled());

        Analysis analysis = analyzer.analyze(statement);
        return new LogicalPlanner(session, optimizerProvider.get(), idAllocator, metadata, sqlParser).plan(analysis);
    }

    public OperatorFactory createTableScanOperator(int operatorId, PlanNodeId planNodeId, String tableName, String... columnNames)
    {
        return createTableScanOperator(defaultSession, operatorId, planNodeId, tableName, columnNames);
    }

    public OperatorFactory createTableScanOperator(
            Session session,
            int operatorId,
            PlanNodeId planNodeId,
            String tableName,
            String... columnNames)
    {
        checkArgument(session.getCatalog().isPresent(), "catalog not set");
        checkArgument(session.getSchema().isPresent(), "schema not set");

        // look up the table
        QualifiedObjectName qualifiedTableName = new QualifiedObjectName(session.getCatalog().get(), session.getSchema().get(), tableName);
        TableHandle tableHandle = metadata.getTableHandle(session, qualifiedTableName).orElse(null);
        checkArgument(tableHandle != null, "Table %s does not exist", qualifiedTableName);

        // lookup the columns
        Map<String, ColumnHandle> allColumnHandles = metadata.getColumnHandles(session, tableHandle);
        ImmutableList.Builder<ColumnHandle> columnHandlesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypesBuilder = ImmutableList.builder();
        for (String columnName : columnNames) {
            ColumnHandle columnHandle = allColumnHandles.get(columnName);
            checkArgument(columnHandle != null, "Table %s does not have a column %s", tableName, columnName);
            columnHandlesBuilder.add(columnHandle);
            ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle, columnHandle);
            columnTypesBuilder.add(columnMetadata.getType());
        }
        List<ColumnHandle> columnHandles = columnHandlesBuilder.build();
        List<Type> columnTypes = columnTypesBuilder.build();

        // get the split for this table
        List<TableLayoutResult> layouts = metadata.getLayouts(session, tableHandle, Constraint.alwaysTrue(), Optional.empty());
        Split split = getLocalQuerySplit(session, layouts.get(0).getLayout().getHandle());

        return new OperatorFactory()
        {
            @Override
            public List<Type> getTypes()
            {
                return columnTypes;
            }

            @Override
            public Operator createOperator(DriverContext driverContext)
            {
                OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, "BenchmarkSource");
                ConnectorPageSource pageSource = pageSourceManager.createPageSource(session, split, columnHandles);
                return new PageSourceOperator(pageSource, columnTypes, operatorContext);
            }

            @Override
            public void close()
            {
            }

            @Override
            public OperatorFactory duplicate()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public OperatorFactory createHashProjectOperator(int operatorId, PlanNodeId planNodeId, List<Type> columnTypes)
    {
        ImmutableList.Builder<ProjectionFunction> projectionFunctions = ImmutableList.builder();
        for (int i = 0; i < columnTypes.size(); i++) {
            projectionFunctions.add(ProjectionFunctions.singleColumn(columnTypes.get(i), i));
        }
        projectionFunctions.add(new HashProjectionFunction(columnTypes));
        return new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                operatorId,
                planNodeId,
                () -> new GenericPageProcessor(FilterFunctions.TRUE_FUNCTION, projectionFunctions.build()),
                ImmutableList.copyOf(Iterables.concat(columnTypes, ImmutableList.of(BIGINT))));
    }

    private Split getLocalQuerySplit(Session session, TableLayoutHandle handle)
    {
        SplitSource splitSource = splitManager.getSplits(session, handle);
        List<Split> splits = new ArrayList<>();
        splits.addAll(getFutureValue(splitSource.getNextBatch(1000)));
        while (!splitSource.isFinished()) {
            splits.addAll(getFutureValue(splitSource.getNextBatch(1000)));
        }
        checkArgument(splits.size() == 1, "Expected only one split for a local query, but got %s splits", splits.size());
        return splits.get(0);
    }

    private static List<TableScanNode> findTableScanNodes(PlanNode node)
    {
        ImmutableList.Builder<TableScanNode> tableScanNodes = ImmutableList.builder();
        findTableScanNodes(node, tableScanNodes);
        return tableScanNodes.build();
    }

    private static void findTableScanNodes(PlanNode node, ImmutableList.Builder<TableScanNode> builder)
    {
        for (PlanNode source : node.getSources()) {
            findTableScanNodes(source, builder);
        }

        if (node instanceof TableScanNode) {
            builder.add((TableScanNode) node);
        }
    }

    private static class HashProjectionFunction
            implements ProjectionFunction
    {
        private final List<Type> columnTypes;

        public HashProjectionFunction(List<Type> columnTypes)
        {
            this.columnTypes = columnTypes;
        }

        @Override
        public Type getType()
        {
            return BIGINT;
        }

        @Override
        public void project(int position, Block[] blocks, BlockBuilder output)
        {
            BIGINT.writeLong(output, TypeUtils.getHashPosition(columnTypes, blocks, position));
        }

        @Override
        public void project(RecordCursor cursor, BlockBuilder output)
        {
            throw new UnsupportedOperationException("Operation not supported");
        }

        @Override
        public Set<Integer> getInputChannels()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDeterministic()
        {
            throw new UnsupportedOperationException();
        }
    }
}
