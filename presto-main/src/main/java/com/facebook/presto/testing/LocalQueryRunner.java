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
import com.facebook.presto.TaskSource;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.connector.system.CatalogSystemTable;
import com.facebook.presto.connector.system.NodesSystemTable;
import com.facebook.presto.connector.system.SystemConnector;
import com.facebook.presto.connector.system.SystemDataStreamProvider;
import com.facebook.presto.connector.system.SystemSplitManager;
import com.facebook.presto.connector.system.SystemTablesManager;
import com.facebook.presto.connector.system.SystemTablesMetadata;
import com.facebook.presto.execution.SplitSource;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.OutputTableHandleResolver;
import com.facebook.presto.metadata.Partition;
import com.facebook.presto.metadata.PartitionResult;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.RecordSinkManager;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.DataStreamManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DistributedLogicalPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;
import org.intellij.lang.annotations.Language;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class LocalQueryRunner
    implements QueryRunner
{
    private final ConnectorSession defaultSession;
    private final ExecutorService executor;

    private final InMemoryNodeManager nodeManager;
    private final TypeRegistry typeRegistry;
    private final MetadataManager metadata;
    private final SplitManager splitManager;
    private final DataStreamManager dataStreamProvider;
    private final IndexManager indexManager;
    private final RecordSinkManager recordSinkManager;

    private final ExpressionCompiler compiler;
    private final ConnectorManager connectorManager;

    private boolean printPlan;

    public LocalQueryRunner(ConnectorSession defaultSession, ExecutorService executor)
    {
        this.defaultSession = checkNotNull(defaultSession, "defaultSession is null");
        this.executor = checkNotNull(executor, "executor is null");

        this.nodeManager = new InMemoryNodeManager();
        this.typeRegistry = new TypeRegistry();
        this.metadata = new MetadataManager(new FeaturesConfig().setExperimentalSyntaxEnabled(true), typeRegistry);
        this.splitManager = new SplitManager();
        this.dataStreamProvider = new DataStreamManager();
        this.indexManager = new IndexManager();
        this.recordSinkManager = new RecordSinkManager();

        this.compiler = new ExpressionCompiler(metadata);

        // sys schema
        SystemTablesMetadata systemTablesMetadata = new SystemTablesMetadata();
        SystemSplitManager systemSplitManager = new SystemSplitManager(nodeManager);
        SystemDataStreamProvider systemDataStreamProvider = new SystemDataStreamProvider();
        SystemTablesManager systemTablesManager = new SystemTablesManager(systemTablesMetadata, systemSplitManager, systemDataStreamProvider, ImmutableSet.<SystemTable>of());

        // sys.node
        systemTablesManager.addTable(new NodesSystemTable(nodeManager));

        // sys.catalog
        systemTablesManager.addTable(new CatalogSystemTable(metadata));

        this.connectorManager = new ConnectorManager(
                metadata,
                splitManager,
                dataStreamProvider,
                indexManager,
                recordSinkManager,
                new HandleResolver(),
                new OutputTableHandleResolver(),
                ImmutableMap.<String, ConnectorFactory>of(),
                ImmutableMap.<String, Connector>of(
                        SystemConnector.CONNECTOR_ID, new SystemConnector(systemTablesMetadata, systemSplitManager, systemDataStreamProvider)),
                nodeManager);
    }

    @Override
    public void close()
    {
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

    public MetadataManager getMetadata()
    {
        return metadata;
    }

    public ExecutorService getExecutor()
    {
        return executor;
    }

    public void createCatalog(String catalogName, ConnectorFactory connectorFactory, Map<String, String> properties)
    {
        nodeManager.addCurrentNodeDatasource(catalogName);
        connectorManager.createConnection(catalogName, connectorFactory, properties);
    }

    public QueryRunner printPlan()
    {
        printPlan = true;
        return this;
    }

    private static class MaterializedOutputFactory
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
        public OperatorFactory createOutputOperator(final int operatorId, final List<Type> sourceType)
        {
            checkNotNull(sourceType, "sourceType is null");

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
                    MaterializingOperator operator = new MaterializingOperator(operatorContext, sourceType);

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
    public List<QualifiedTableName> listTables(ConnectorSession session, String catalog, String schema)
    {
        return getMetadata().listTables(session, new QualifiedTablePrefix(catalog, schema));
    }

    @Override
    public boolean tableExists(ConnectorSession session, String table)
    {
        QualifiedTableName name =  new QualifiedTableName(session.getCatalog(), session.getSchema(), table);
        Optional<TableHandle> handle = getMetadata().getTableHandle(session, name);
        return handle.isPresent();
    }

    @Override
    public MaterializedResult execute(@Language("SQL") String sql)
    {
        return execute(defaultSession, sql);
    }

    @Override
    public MaterializedResult execute(ConnectorSession session, @Language("SQL") String sql)
    {
        MaterializedOutputFactory outputFactory = new MaterializedOutputFactory();

        TaskContext taskContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session);
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

    public List<Driver> createDrivers(ConnectorSession session, @Language("SQL") String sql, OutputFactory outputFactory, TaskContext taskContext)
    {
        Statement statement = SqlParser.createStatement(sql);

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        FeaturesConfig featuresConfig = new FeaturesConfig().setExperimentalSyntaxEnabled(true);
        PlanOptimizersFactory planOptimizersFactory = new PlanOptimizersFactory(metadata, splitManager, indexManager, featuresConfig);

        QueryExplainer queryExplainer = new QueryExplainer(session, planOptimizersFactory.get(), metadata, featuresConfig.isExperimentalSyntaxEnabled());
        Analyzer analyzer = new Analyzer(session, metadata, Optional.of(queryExplainer), featuresConfig.isExperimentalSyntaxEnabled());

        Analysis analysis = analyzer.analyze(statement);

        Plan plan = new LogicalPlanner(session, planOptimizersFactory.get(), idAllocator, metadata).plan(analysis);
        if (printPlan) {
            System.out.println(PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata));
        }

        SubPlan subplan = new DistributedLogicalPlanner(session, metadata, idAllocator).createSubPlans(plan, true);
        if (!subplan.getChildren().isEmpty()) {
            throw new AssertionError("Expected subplan to have no children");
        }

        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
                new NodeInfo(new NodeConfig()
                        .setEnvironment("test")
                        .setNodeId("test-node")),
                metadata,
                dataStreamProvider,
                indexManager,
                recordSinkManager,
                null,
                compiler);

        // plan query
        LocalExecutionPlan localExecutionPlan = executionPlanner.plan(session,
                subplan.getFragment().getRoot(),
                plan.getTypes(),
                outputFactory);

        // generate sources
        List<TaskSource> sources = new ArrayList<>();
        long sequenceId = 0;
        for (PlanNode sourceNode : subplan.getFragment().getSources()) {
            if (sourceNode instanceof ValuesNode) {
                continue;
            }

            TableScanNode tableScan = (TableScanNode) sourceNode;

            SplitSource splitSource = splitManager.getPartitionSplits(tableScan.getTable(), getPartitions(tableScan));

            ImmutableSet.Builder<ScheduledSplit> scheduledSplits = ImmutableSet.builder();
            while (!splitSource.isFinished()) {
                try {
                    for (Split split : splitSource.getNextBatch(1000)) {
                        scheduledSplits.add(new ScheduledSplit(sequenceId++, split));
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
            }

            sources.add(new TaskSource(tableScan.getId(), scheduledSplits.build(), true));
        }

        // create drivers
        List<Driver> drivers = new ArrayList<>();
        Map<PlanNodeId, Driver> driversBySource = new HashMap<>();
        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            DriverContext driverContext = taskContext.addPipelineContext(driverFactory.isInputDriver(), driverFactory.isOutputDriver()).addDriverContext();
            Driver driver = driverFactory.createDriver(driverContext);
            drivers.add(driver);
            for (PlanNodeId sourceId : driver.getSourceIds()) {
                driversBySource.put(sourceId, driver);
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

    private List<Partition> getPartitions(TableScanNode node)
    {
        if (node.getGeneratedPartitions().isPresent()) {
            return node.getGeneratedPartitions().get().getPartitions();
        }

        // Otherwise return all partitions
        PartitionResult matchingPartitions = splitManager.getPartitions(node.getTable(), Optional.<TupleDomain<ColumnHandle>>absent());
        return matchingPartitions.getPartitions();
    }

    public OperatorFactory createTableScanOperator(int operatorId, String tableName, String... columnNames)
    {
        return createTableScanOperator(defaultSession, operatorId, tableName, columnNames);
    }

    public OperatorFactory createTableScanOperator(
            ConnectorSession session,
            final int operatorId,
            String tableName,
            String... columnNames)
    {
        // look up the table
        TableHandle tableHandle = metadata.getTableHandle(session, new QualifiedTableName(session.getCatalog(), session.getSchema(), tableName)).orNull();
        checkArgument(tableHandle != null, "Table %s does not exist", tableName);

        // lookup the columns
        ImmutableList.Builder<ColumnHandle> columnHandlesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypesBuilder = ImmutableList.builder();
        for (String columnName : columnNames) {
            ColumnHandle columnHandle = metadata.getColumnHandle(tableHandle, columnName).orNull();
            checkArgument(columnHandle != null, "Table %s does not have a column %s", tableName, columnName);
            columnHandlesBuilder.add(columnHandle);
            ColumnMetadata columnMetadata = metadata.getColumnMetadata(tableHandle, columnHandle);
            columnTypesBuilder.add(columnMetadata.getType());
        }
        final List<ColumnHandle> columnHandles = columnHandlesBuilder.build();
        final List<Type> columnTypes = columnTypesBuilder.build();

        // get the split for this table
        final Split split = getLocalQuerySplit(tableHandle);

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
                return dataStreamProvider.createNewDataStream(operatorContext, split, columnHandles);
            }

            @Override
            public void close()
            {
            }
        };
    }

    private Split getLocalQuerySplit(TableHandle tableHandle)
    {
        try {
            List<Partition> partitions = splitManager.getPartitions(tableHandle, Optional.<TupleDomain<ColumnHandle>>absent()).getPartitions();
            SplitSource splitSource = splitManager.getPartitionSplits(tableHandle, partitions);
            Split split = Iterables.getOnlyElement(splitSource.getNextBatch(1000));
            checkState(splitSource.isFinished(), "Expected only one split for a local query");
            return split;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }
}
