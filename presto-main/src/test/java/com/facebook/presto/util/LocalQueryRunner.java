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
package com.facebook.presto.util;

import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.connector.dual.DualDataStreamProvider;
import com.facebook.presto.connector.dual.DualMetadata;
import com.facebook.presto.connector.dual.DualSplitManager;
import com.facebook.presto.connector.informationSchema.InformationSchemaDataStreamProvider;
import com.facebook.presto.connector.informationSchema.InformationSchemaSplitManager;
import com.facebook.presto.connector.system.CatalogSystemTable;
import com.facebook.presto.connector.system.NodesSystemTable;
import com.facebook.presto.connector.system.SystemDataStreamProvider;
import com.facebook.presto.connector.system.SystemSplitManager;
import com.facebook.presto.connector.system.SystemTablesManager;
import com.facebook.presto.connector.system.SystemTablesMetadata;
import com.facebook.presto.execution.DataSource;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.importer.MockPeriodicImportManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.LocalStorageManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.MaterializingOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.RecordSinkManager;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.split.DataStreamManager;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.Session;
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
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.storage.MockStorageManager;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchSplitManager;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;
import org.intellij.lang.annotations.Language;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.metadata.MockLocalStorageManager.createMockLocalStorageManager;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_CATALOG;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_SCHEMA;
import static com.facebook.presto.sql.parser.TreeAssertions.assertFormattedSql;
import static com.facebook.presto.tpch.TpchMetadata.TPCH_CATALOG_NAME;
import static com.facebook.presto.tpch.TpchMetadata.TPCH_SCHEMA_NAME;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.testng.Assert.assertTrue;

public class LocalQueryRunner
{
    private final Metadata metadata;
    private final SplitManager splitManager;
    private final DataStreamProvider dataStreamProvider;
    private final LocalStorageManager storageManager;
    private final RecordSinkManager recordSinkManager;
    private final Session session;
    private final ExecutorService executor;
    private final ExpressionCompiler compiler;
    private boolean printPlan;

    public LocalQueryRunner(Metadata metadata,
            SplitManager splitManager,
            DataStreamProvider dataStreamProvider,
            LocalStorageManager storageManager,
            RecordSinkManager recordSinkManager,
            Session session,
            ExecutorService executor)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.recordSinkManager = checkNotNull(recordSinkManager, "recordSinkManager is null");
        this.session = checkNotNull(session, "session is null");
        this.executor = checkNotNull(executor, "executor is null");
        this.compiler = new ExpressionCompiler(metadata);
    }

    public LocalQueryRunner printPlan()
    {
        printPlan = true;
        return this;
    }

    private static class MaterializedOutputFactory
            implements OutputFactory
    {
        private MaterializingOperator materializingOperator;

        private MaterializingOperator getMaterializingOperator()
        {
            checkState(materializingOperator != null, "Output not created");
            return materializingOperator;
        }

        @Override
        public OperatorFactory createOutputOperator(final int operatorId, final List<TupleInfo> sourceTupleInfo)
        {
            checkNotNull(sourceTupleInfo, "sourceTupleInfo is null");

            return new OperatorFactory()
            {
                @Override
                public List<TupleInfo> getTupleInfos()
                {
                    return ImmutableList.of();
                }

                @Override
                public Operator createOperator(DriverContext driverContext)
                {
                    checkState(materializingOperator == null, "Output already created");
                    OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, MaterializingOperator.class.getSimpleName());
                    materializingOperator = new MaterializingOperator(operatorContext, sourceTupleInfo);
                    return materializingOperator;
                }

                @Override
                public void close()
                {
                }
            };
        }
    }

    public MaterializedResult execute(@Language("SQL") String sql)
    {
        MaterializedOutputFactory outputFactory = new MaterializedOutputFactory();
        List<Driver> drivers = createDrivers(sql, outputFactory);

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

    public List<Driver> createDrivers(@Language("SQL") String sql, OutputFactory outputFactory)
    {
        return createDrivers(sql, outputFactory, new TaskContext(new TaskId("query", "stage", "task"), executor, session));
    }

    public List<Driver> createDrivers(@Language("SQL") String sql, OutputFactory outputFactory, TaskContext taskContext)
    {
        Statement statement = SqlParser.createStatement(sql);

        if (printPlan) {
            assertFormattedSql(statement);
        }

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanOptimizersFactory planOptimizersFactory = new PlanOptimizersFactory(metadata, splitManager);

        QueryExplainer queryExplainer = new QueryExplainer(session, planOptimizersFactory.get(), metadata, new MockPeriodicImportManager(), new MockStorageManager());
        Analyzer analyzer = new Analyzer(session, metadata, Optional.of(queryExplainer));

        Analysis analysis = analyzer.analyze(statement);

        Plan plan = new LogicalPlanner(session, planOptimizersFactory.get(), idAllocator, metadata, new MockPeriodicImportManager(), new MockStorageManager()).plan(analysis);
        if (printPlan) {
            System.out.println(PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes()));
        }

        SubPlan subplan = new DistributedLogicalPlanner(metadata, idAllocator).createSubPlans(plan, true);
        assertTrue(subplan.getChildren().isEmpty(), "Expected subplan to have no children");

        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
                new NodeInfo(new NodeConfig()
                        .setEnvironment("test")
                        .setNodeId("test-node")),
                metadata,
                dataStreamProvider,
                storageManager,
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
            TableScanNode tableScan = (TableScanNode) sourceNode;

            DataSource dataSource = splitManager.getPartitionSplits(tableScan.getTable(), getPartitions(tableScan));

            ImmutableSet.Builder<ScheduledSplit> scheduledSplits = ImmutableSet.builder();
            for (Split split : dataSource.getSplits()) {
                scheduledSplits.add(new ScheduledSplit(sequenceId++, split));
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
        PartitionResult matchingPartitions = splitManager.getPartitions(node.getTable(), Optional.<TupleDomain>absent());
        return matchingPartitions.getPartitions();
    }

    public static LocalQueryRunner createDualLocalQueryRunner(ExecutorService executor)
    {
        return createDualLocalQueryRunner(new Session("user", "test", DEFAULT_CATALOG, DEFAULT_SCHEMA, null, null), executor);
    }

    public static LocalQueryRunner createDualLocalQueryRunner(Session session, ExecutorService executor)
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();

        MetadataManager metadataManager = new MetadataManager();
        SplitManager splitManager = new SplitManager(ImmutableSet.<ConnectorSplitManager>of());
        DataStreamManager dataStreamManager = new DataStreamManager();
        RecordSinkManager recordSinkManager = new RecordSinkManager();

        addDual(nodeManager, metadataManager, splitManager, dataStreamManager);
        addInformationSchema(nodeManager, metadataManager, splitManager, dataStreamManager);

        return new LocalQueryRunner(metadataManager, splitManager, dataStreamManager, createMockLocalStorageManager(), recordSinkManager, session, executor);
    }

    public static LocalQueryRunner createTpchLocalQueryRunner(ExecutorService executor)
    {
        return createTpchLocalQueryRunner(new Session("user", "test", TPCH_CATALOG_NAME, TPCH_SCHEMA_NAME, null, null), executor);
    }

    public static LocalQueryRunner createTpchLocalQueryRunner(Session session, ExecutorService executor)
    {
        return createTpchLocalQueryRunner(session, new InMemoryTpchBlocksProvider(), executor);
    }

    public static LocalQueryRunner createTpchLocalQueryRunner(TpchBlocksProvider tpchBlocksProvider, ExecutorService executor)
    {
        return createTpchLocalQueryRunner(new Session("user", "test", TPCH_CATALOG_NAME, TPCH_SCHEMA_NAME, null, null), tpchBlocksProvider, executor);
    }

    public static LocalQueryRunner createTpchLocalQueryRunner(Session session, TpchBlocksProvider tpchBlocksProvider, ExecutorService executor)
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();

        MetadataManager metadataManager = new MetadataManager();
        SplitManager splitManager = new SplitManager(ImmutableSet.<ConnectorSplitManager>of());
        DataStreamManager dataStreamManager = new DataStreamManager();
        RecordSinkManager recordSinkManager = new RecordSinkManager();

        addDual(nodeManager, metadataManager, splitManager, dataStreamManager);
        addSystem(nodeManager, metadataManager, splitManager, dataStreamManager);
        addInformationSchema(nodeManager, metadataManager, splitManager, dataStreamManager);
        addTpch(nodeManager, metadataManager, splitManager, dataStreamManager, tpchBlocksProvider);

        return new LocalQueryRunner(metadataManager, splitManager, dataStreamManager, createMockLocalStorageManager(), recordSinkManager, session, executor);
    }

    private static void addSystem(InMemoryNodeManager nodeManager, MetadataManager metadataManager, SplitManager splitManager, DataStreamManager dataStreamManager)
    {
        SystemTablesMetadata systemTablesMetadata = new SystemTablesMetadata();
        metadataManager.addInternalSchemaMetadata(MetadataManager.INTERNAL_CONNECTOR_ID, systemTablesMetadata);

        SystemSplitManager systemSplitManager = new SystemSplitManager(nodeManager);
        splitManager.addConnectorSplitManager(systemSplitManager);

        SystemDataStreamProvider systemDataStreamProvider = new SystemDataStreamProvider();
        dataStreamManager.addConnectorDataStreamProvider(systemDataStreamProvider);

        SystemTablesManager systemTablesManager = new SystemTablesManager(systemTablesMetadata, systemSplitManager, systemDataStreamProvider, ImmutableSet.<SystemTable>of());

        systemTablesManager.addTable(new NodesSystemTable(nodeManager));
        systemTablesManager.addTable(new CatalogSystemTable(metadataManager));
    }

    private static void addTpch(InMemoryNodeManager nodeManager,
            MetadataManager metadataManager,
            SplitManager splitManager,
            DataStreamManager dataStreamManager,
            TpchBlocksProvider tpchBlocksProvider)
    {
        metadataManager.addConnectorMetadata(TPCH_CATALOG_NAME, TPCH_CATALOG_NAME, new TpchMetadata());
        splitManager.addConnectorSplitManager(new TpchSplitManager("tpch", nodeManager));
        dataStreamManager.addConnectorDataStreamProvider(new TpchDataStreamProvider(tpchBlocksProvider));
    }

    private static void addInformationSchema(InMemoryNodeManager nodeManager, MetadataManager metadataManager, SplitManager splitManager, DataStreamManager dataStreamManager)
    {
        splitManager.addConnectorSplitManager(new InformationSchemaSplitManager(nodeManager));
        dataStreamManager.addConnectorDataStreamProvider(new InformationSchemaDataStreamProvider(metadataManager, splitManager));
    }

    private static void addDual(InMemoryNodeManager nodeManager, MetadataManager metadataManager, SplitManager splitManager, DataStreamManager dataStreamManager)
    {
        metadataManager.addInternalSchemaMetadata(MetadataManager.INTERNAL_CONNECTOR_ID, new DualMetadata());
        splitManager.addConnectorSplitManager(new DualSplitManager(nodeManager));
        dataStreamManager.addConnectorDataStreamProvider(new DualDataStreamProvider());
    }
}
