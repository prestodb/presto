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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.TaskSource;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.connector.system.CatalogSystemTable;
import com.facebook.presto.connector.system.NodeSystemTable;
import com.facebook.presto.connector.system.SystemConnector;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.metadata.TableLayoutResult;
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
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockEncodingSerde;
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
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.intellij.lang.annotations.Language;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.testing.TreeAssertions.assertFormattedSql;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class LocalQueryRunner
        implements QueryRunner
{
    private final Session defaultSession;
    private final ExecutorService executor;

    private final SqlParser sqlParser;
    private final InMemoryNodeManager nodeManager;
    private final TypeRegistry typeRegistry;
    private final MetadataManager metadata;
    private final SplitManager splitManager;
    private final BlockEncodingSerde blockEncodingSerde;
    private final PageSourceManager pageSourceManager;
    private final IndexManager indexManager;
    private final PageSinkManager pageSinkManager;

    private final ExpressionCompiler compiler;
    private final ConnectorManager connectorManager;
    private final boolean hashEnabled;

    private boolean printPlan;

    public LocalQueryRunner(Session defaultSession)
    {
        this.defaultSession = checkNotNull(defaultSession, "defaultSession is null");
        this.hashEnabled = SystemSessionProperties.isOptimizeHashGenerationEnabled(defaultSession, false);
        this.executor = newCachedThreadPool(daemonThreadsNamed("local-query-runner-%s"));

        this.sqlParser = new SqlParser();
        this.nodeManager = new InMemoryNodeManager();
        this.typeRegistry = new TypeRegistry();
        this.indexManager = new IndexManager();
        this.pageSinkManager = new PageSinkManager();

        this.splitManager = new SplitManager();
        this.blockEncodingSerde = new BlockEncodingManager(typeRegistry);
        this.metadata = new MetadataManager(new FeaturesConfig().setExperimentalSyntaxEnabled(true), typeRegistry, splitManager, blockEncodingSerde);
        this.pageSourceManager = new PageSourceManager();

        this.compiler = new ExpressionCompiler(metadata);

        this.connectorManager = new ConnectorManager(
                metadata,
                splitManager,
                pageSourceManager,
                indexManager,
                pageSinkManager,
                new HandleResolver(),
                ImmutableMap.<String, ConnectorFactory>of(),
                nodeManager
        );

        Connector systemConnector = new SystemConnector(nodeManager, ImmutableSet.of(
                new NodeSystemTable(nodeManager),
                new CatalogSystemTable(metadata)));

        connectorManager.createConnection(SystemConnector.NAME, systemConnector);
    }

    public static LocalQueryRunner createHashEnabledQueryRunner(LocalQueryRunner localQueryRunner)
    {
        Session session = localQueryRunner.getDefaultSession();
        Session.SessionBuilder builder = Session.builder()
                .setUser(session.getUser())
                .setSource(session.getSource())
                .setCatalog(session.getCatalog())
                .setTimeZoneKey(session.getTimeZoneKey())
                .setLocale(session.getLocale())
                .setSystemProperties(ImmutableMap.of("optimizer.optimize_hash_generation", "true"));
        return new LocalQueryRunner(builder.build());
    }

    @Override
    public void close()
    {
        executor.shutdownNow();
        connectorManager.stop();
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

    public Metadata getMetadata()
    {
        return metadata;
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
        connectorManager.createConnection(catalogName, connectorFactory, properties);
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

    public boolean isHashEnabled()
    {
        return hashEnabled;
    }

    public static class MaterializedOutputFactory
            implements OutputFactory
    {
        private final AtomicReference<MaterializingOperator> materializingOperator = new AtomicReference<>();

        private MaterializingOperator getMaterializingOperator()
        {
            MaterializingOperator operator = materializingOperator.get();
            checkState(operator != null, "Output not created");
            return operator;
        }

        @Override
        public OperatorFactory createOutputOperator(int operatorId, List<Type> sourceTypes)
        {
            checkNotNull(sourceTypes, "sourceType is null");

            return new OperatorFactory()
            {
                @Override
                public List<Type> getTypes()
                {
                    return ImmutableList.of();
                }

                @Override
                public Operator createOperator(DriverContext driverContext)
                {
                    OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, MaterializingOperator.class.getSimpleName());
                    MaterializingOperator operator = new MaterializingOperator(operatorContext, sourceTypes);

                    if (!materializingOperator.compareAndSet(null, operator)) {
                        throw new IllegalArgumentException("Output already created");
                    }
                    return operator;
                }

                @Override
                public void close()
                {
                }
            };
        }
    }

    @Override
    public List<QualifiedTableName> listTables(Session session, String catalog, String schema)
    {
        return getMetadata().listTables(session, new QualifiedTablePrefix(catalog, schema));
    }

    @Override
    public boolean tableExists(Session session, String table)
    {
        QualifiedTableName name = new QualifiedTableName(session.getCatalog(), session.getSchema(), table);
        return getMetadata().getTableHandle(session, name).isPresent();
    }

    @Override
    public MaterializedResult execute(@Language("SQL") String sql)
    {
        return execute(defaultSession, sql);
    }

    @Override
    public MaterializedResult execute(Session session, @Language("SQL") String sql)
    {
        MaterializedOutputFactory outputFactory = new MaterializedOutputFactory();

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

        return outputFactory.getMaterializingOperator().getMaterializedResult();
    }

    public List<Driver> createDrivers(@Language("SQL") String sql, OutputFactory outputFactory, TaskContext taskContext)
    {
        return createDrivers(defaultSession, sql, outputFactory, taskContext);
    }

    public List<Driver> createDrivers(Session session, @Language("SQL") String sql, OutputFactory outputFactory, TaskContext taskContext)
    {
        Statement statement = sqlParser.createStatement(sql);

        assertFormattedSql(sqlParser, statement);

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        FeaturesConfig featuresConfig = new FeaturesConfig()
                .setExperimentalSyntaxEnabled(true)
                .setDistributedIndexJoinsEnabled(false)
                .setOptimizeHashGeneration(true);
        PlanOptimizersFactory planOptimizersFactory = new PlanOptimizersFactory(metadata, sqlParser, indexManager, featuresConfig, true);

        QueryExplainer queryExplainer = new QueryExplainer(session, planOptimizersFactory.get(), metadata, sqlParser, featuresConfig.isExperimentalSyntaxEnabled());
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, Optional.of(queryExplainer), featuresConfig.isExperimentalSyntaxEnabled());

        Analysis analysis = analyzer.analyze(statement);
        Plan plan = new LogicalPlanner(session, planOptimizersFactory.get(), idAllocator, metadata).plan(analysis);

        if (printPlan) {
            System.out.println(PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata));
        }

        SubPlan subplan = new PlanFragmenter().createSubPlans(plan);
        if (!subplan.getChildren().isEmpty()) {
            throw new AssertionError("Expected subplan to have no children");
        }

        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
                metadata,
                sqlParser,
                pageSourceManager,
                indexManager,
                pageSinkManager,
                null,
                compiler,
                new IndexJoinLookupStats(),
                new CompilerConfig().setInterpreterEnabled(false), // make sure tests fail if compiler breaks
                new TaskManagerConfig().setTaskDefaultConcurrency(4)
        );

        // plan query
        LocalExecutionPlan localExecutionPlan = executionPlanner.plan(session,
                subplan.getFragment().getRoot(),
                subplan.getFragment().getOutputLayout(),
                plan.getTypes(),
                subplan.getFragment().getDistribution(),
                outputFactory);

        // generate sources
        List<TaskSource> sources = new ArrayList<>();
        long sequenceId = 0;
        for (TableScanNode tableScan : findTableScanNodes(subplan.getFragment().getRoot())) {
            TableLayoutHandle layout = tableScan.getLayout().get();

            SplitSource splitSource = splitManager.getSplits(layout);

            ImmutableSet.Builder<ScheduledSplit> scheduledSplits = ImmutableSet.builder();
            while (!splitSource.isFinished()) {
                for (Split split : getFutureValue(splitSource.getNextBatch(1000))) {
                    scheduledSplits.add(new ScheduledSplit(sequenceId++, split));
                }
            }

            sources.add(new TaskSource(tableScan.getId(), scheduledSplits.build(), true));
        }

        // create drivers
        List<Driver> drivers = new ArrayList<>();
        Map<PlanNodeId, Driver> driversBySource = new HashMap<>();
        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            for (int i = 0; i < driverFactory.getDriverInstances(); i++) {
                DriverContext driverContext = taskContext.addPipelineContext(driverFactory.isInputDriver(), driverFactory.isOutputDriver()).addDriverContext();
                Driver driver = driverFactory.createDriver(driverContext);
                drivers.add(driver);
                for (PlanNodeId sourceId : driver.getSourceIds()) {
                    driversBySource.put(sourceId, driver);
                }
            }
            driverFactory.close();
        }

        // add sources to the drivers
        for (TaskSource source : sources) {
            for (Driver driver : driversBySource.values()) {
                driver.updateSource(source);
            }
        }

        return ImmutableList.copyOf(drivers);
    }

    public OperatorFactory createTableScanOperator(int operatorId, String tableName, String... columnNames)
    {
        return createTableScanOperator(defaultSession, operatorId, tableName, columnNames);
    }

    public OperatorFactory createTableScanOperator(
            Session session,
            int operatorId,
            String tableName,
            String... columnNames)
    {
        // look up the table
        TableHandle tableHandle = metadata.getTableHandle(session, new QualifiedTableName(session.getCatalog(), session.getSchema(), tableName)).orElse(null);
        checkArgument(tableHandle != null, "Table %s does not exist", tableName);

        // lookup the columns
        Map<String, ColumnHandle> allColumnHandles = metadata.getColumnHandles(tableHandle);
        ImmutableList.Builder<ColumnHandle> columnHandlesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypesBuilder = ImmutableList.builder();
        for (String columnName : columnNames) {
            ColumnHandle columnHandle = allColumnHandles.get(columnName);
            checkArgument(columnHandle != null, "Table %s does not have a column %s", tableName, columnName);
            columnHandlesBuilder.add(columnHandle);
            ColumnMetadata columnMetadata = metadata.getColumnMetadata(tableHandle, columnHandle);
            columnTypesBuilder.add(columnMetadata.getType());
        }
        List<ColumnHandle> columnHandles = columnHandlesBuilder.build();
        List<Type> columnTypes = columnTypesBuilder.build();

        // get the split for this table
        List<TableLayoutResult> layouts = metadata.getLayouts(tableHandle, Constraint.alwaysTrue(), Optional.empty());
        Split split = getLocalQuerySplit(layouts.get(0).getLayout().getHandle());

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
                OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, "BenchmarkSource");
                ConnectorPageSource pageSource = pageSourceManager.createPageSource(split, columnHandles);
                return new PageSourceOperator(pageSource, columnTypes, operatorContext);
            }

            @Override
            public void close()
            {
            }
        };
    }

    public OperatorFactory createHashProjectOperator(int operatorId, List<Type> columnTypes)
    {
        ImmutableList.Builder<ProjectionFunction> projectionFunctions = ImmutableList.builder();
        for (int i = 0; i < columnTypes.size(); i++) {
            projectionFunctions.add(ProjectionFunctions.singleColumn(columnTypes.get(i), i));
        }
        projectionFunctions.add(new HashProjectionFunction(columnTypes));
        return new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                operatorId,
                new GenericPageProcessor(FilterFunctions.TRUE_FUNCTION, projectionFunctions.build()),
                ImmutableList.copyOf(Iterables.concat(columnTypes, ImmutableList.of(BIGINT))));
    }

    private Split getLocalQuerySplit(TableLayoutHandle handle)
    {
        SplitSource splitSource = splitManager.getSplits(handle);
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
    }
}
