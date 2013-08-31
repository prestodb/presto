package com.facebook.presto.util;

import com.facebook.presto.connector.dual.DualDataStreamProvider;
import com.facebook.presto.connector.dual.DualMetadata;
import com.facebook.presto.connector.dual.DualSplitManager;
import com.facebook.presto.connector.informationSchema.InformationSchemaDataStreamProvider;
import com.facebook.presto.connector.informationSchema.InformationSchemaSplitManager;
import com.facebook.presto.connector.system.NodesSystemTable;
import com.facebook.presto.connector.system.SystemDataStreamProvider;
import com.facebook.presto.connector.system.SystemSplitManager;
import com.facebook.presto.connector.system.SystemTablesManager;
import com.facebook.presto.connector.system.SystemTablesMetadata;
import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.importer.MockPeriodicImportManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.LocalStorageManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MockLocalStorageManager;
import com.facebook.presto.noperator.Driver;
import com.facebook.presto.noperator.DriverFactory;
import com.facebook.presto.noperator.DriverOperator.DriverOperatorIterator;
import com.facebook.presto.noperator.InMemoryExchange;
import com.facebook.presto.noperator.MaterializingOperator;
import com.facebook.presto.noperator.NewOperator;
import com.facebook.presto.noperator.NewOperatorFactory;
import com.facebook.presto.noperator.OutputFactory;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.SystemTable;
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
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.NewLocalExecutionPlanner;
import com.facebook.presto.sql.planner.NewLocalExecutionPlanner.NewLocalExecutionPlan;
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
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import org.intellij.lang.annotations.Language;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.analyzer.Session.DEFAULT_CATALOG;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_SCHEMA;
import static com.facebook.presto.sql.parser.TreeAssertions.assertFormattedSql;
import static com.facebook.presto.tpch.TpchMetadata.TPCH_CATALOG_NAME;
import static com.facebook.presto.tpch.TpchMetadata.TPCH_SCHEMA_NAME;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertTrue;

public class LocalQueryRunner
{
    private final Metadata metadata;
    private final SplitManager splitManager;
    private final DataStreamProvider dataStreamProvider;
    private final LocalStorageManager storageManager;
    private final Session session;
    private final ExpressionCompiler compiler;
    private boolean printPlan;

    public LocalQueryRunner(Metadata metadata,
            SplitManager splitManager,
            DataStreamProvider dataStreamProvider,
            LocalStorageManager storageManager,
            Session session)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.session = checkNotNull(session, "session is null");
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
        public NewOperatorFactory createOutputOperator(final List<TupleInfo> sourceTupleInfo)
        {
            checkNotNull(sourceTupleInfo, "sourceTupleInfo is null");

            return new NewOperatorFactory()
            {
                @Override
                public List<TupleInfo> getTupleInfos()
                {
                    return ImmutableList.of();
                }

                @Override
                public NewOperator createOperator(OperatorStats operatorStats, TaskMemoryManager taskMemoryManager)
                {
                    checkState(materializingOperator == null, "Output already created");
                    materializingOperator = new MaterializingOperator(sourceTupleInfo);
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

    private static class InMemoryExchangeOutputFactory
            implements OutputFactory
    {
        private InMemoryExchange inMemoryExchange;

        private InMemoryExchange getInMemoryExchange()
        {
            checkState(inMemoryExchange != null, "Output not created");
            return inMemoryExchange;
        }

        @Override
        public NewOperatorFactory createOutputOperator(final List<TupleInfo> sourceTupleInfo)
        {
            checkNotNull(sourceTupleInfo, "sourceTupleInfo is null");

            checkState(inMemoryExchange == null, "Output already created");
            inMemoryExchange = new InMemoryExchange(sourceTupleInfo);
            NewOperatorFactory operatorFactory = inMemoryExchange.createSinkFactory();
            inMemoryExchange.noMoreSinkFactories();

            return operatorFactory;
        }
    }

    public Operator plan(@Language("SQL") String sql)
    {
        InMemoryExchangeOutputFactory outputFactory = new InMemoryExchangeOutputFactory();
        final List<Driver> drivers = createDrivers(sql, outputFactory);
        final InMemoryExchange inMemoryExchange = outputFactory.getInMemoryExchange();

        return new Operator()
        {
            @Override
            public int getChannelCount()
            {
                return getTupleInfos().size();
            }

            @Override
            public List<TupleInfo> getTupleInfos()
            {
                return inMemoryExchange.getTupleInfos();
            }

            @Override
            public PageIterator iterator(OperatorStats operatorStats)
            {
                return new DriverOperatorIterator(drivers, inMemoryExchange, operatorStats);
            }
        };
    }

    private List<Driver> createDrivers(String sql, OutputFactory outputFactory)
    {
        Statement statement = SqlParser.createStatement(sql);

        if (printPlan) {
            assertFormattedSql(statement);
        }

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanOptimizersFactory planOptimizersFactory = new PlanOptimizersFactory(metadata);

        QueryExplainer queryExplainer = new QueryExplainer(session, planOptimizersFactory.get(), metadata, new MockPeriodicImportManager(), new MockStorageManager());
        Analyzer analyzer = new Analyzer(session, metadata, Optional.of(queryExplainer));

        Analysis analysis = analyzer.analyze(statement);

        Plan plan = new LogicalPlanner(session, planOptimizersFactory.get(), idAllocator, metadata, new MockPeriodicImportManager(), new MockStorageManager()).plan(analysis);
        if (printPlan) {
            System.out.println(PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes()));
        }

        SubPlan subplan = new DistributedLogicalPlanner(metadata, idAllocator).createSubplans(plan, true);
        assertTrue(subplan.getChildren().isEmpty(), "Expected subplan to have no children");

        NewLocalExecutionPlanner executionPlanner = new NewLocalExecutionPlanner(
                new NodeInfo(new NodeConfig()
                        .setEnvironment("test")
                        .setNodeId("test-node")),
                metadata,
                dataStreamProvider,
                storageManager,
                null,
                compiler);

        // plan query
        NewLocalExecutionPlan localExecutionPlan = executionPlanner.plan(session,
                subplan.getFragment().getRoot(),
                plan.getTypes(),
                outputFactory);

        // create drivers
        List<Driver> drivers = new ArrayList<>();
        Map<PlanNodeId, Driver> driversBySource = new HashMap<>();
        List<DriverFactory> driverFactories = localExecutionPlan.getDriverFactories();
        for (DriverFactory driverFactory : driverFactories) {
            Driver driver = driverFactory.createDriver(new OperatorStats(), new TaskMemoryManager(new DataSize(256, MEGABYTE)));
            drivers.add(driver);
            for (PlanNodeId sourceId : driver.getSourceIds()) {
                driversBySource.put(sourceId, driver);
            }
            driverFactory.close();
        }

        // add the splits to the drivers
        for (PlanNode source : subplan.getFragment().getSources()) {
            TableScanNode tableScan = (TableScanNode) source;

            List<Split> splits = ImmutableList.copyOf(splitManager.getSplits(session,
                    tableScan.getTable(),
                    tableScan.getPartitionPredicate(),
                    tableScan.getUpstreamPredicateHint(),
                    Predicates.<Partition>alwaysTrue(),
                    tableScan.getAssignments()).getSplits());

            Driver driver = driversBySource.get(source.getId());
            checkState(driver != null, "Unknown source %s", source.getId());
            for (Split split : splits) {
                driver.addSplit(source.getId(), split);
            }
        }
        for (Driver driver : drivers) {
            for (PlanNodeId sourceId : driver.getSourceIds()) {
                driver.noMoreSplits(sourceId);
            }
        }

        return ImmutableList.copyOf(drivers);
    }

    public static LocalQueryRunner createDualLocalQueryRunner()
    {
        return createDualLocalQueryRunner(new Session("user", "test", DEFAULT_CATALOG, DEFAULT_SCHEMA, null, null));
    }

    public static LocalQueryRunner createDualLocalQueryRunner(Session session)
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();

        MetadataManager metadataManager = new MetadataManager();
        SplitManager splitManager = new SplitManager(metadataManager, ImmutableSet.<ConnectorSplitManager>of());
        DataStreamManager dataStreamManager = new DataStreamManager();

        addDual(nodeManager, metadataManager, splitManager, dataStreamManager);
        addInformationSchema(nodeManager, metadataManager, splitManager, dataStreamManager);

        return new LocalQueryRunner(metadataManager, splitManager, dataStreamManager, MockLocalStorageManager.createMockLocalStorageManager(), session);
    }

    public static LocalQueryRunner createTpchLocalQueryRunner()
    {
        return createTpchLocalQueryRunner(new Session("user", "test", TPCH_CATALOG_NAME, TPCH_SCHEMA_NAME, null, null));
    }

    public static LocalQueryRunner createTpchLocalQueryRunner(Session session)
    {
        return createTpchLocalQueryRunner(session, new InMemoryTpchBlocksProvider());
    }

    public static LocalQueryRunner createTpchLocalQueryRunner(TpchBlocksProvider tpchBlocksProvider)
    {
        return createTpchLocalQueryRunner(new Session("user", "test", TPCH_CATALOG_NAME, TPCH_SCHEMA_NAME, null, null), tpchBlocksProvider);
    }

    public static LocalQueryRunner createTpchLocalQueryRunner(Session session, TpchBlocksProvider tpchBlocksProvider)
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();

        MetadataManager metadataManager = new MetadataManager();
        SplitManager splitManager = new SplitManager(metadataManager, ImmutableSet.<ConnectorSplitManager>of());
        DataStreamManager dataStreamManager = new DataStreamManager();

        addDual(nodeManager, metadataManager, splitManager, dataStreamManager);
        addSystem(nodeManager, metadataManager, splitManager, dataStreamManager);
        addInformationSchema(nodeManager, metadataManager, splitManager, dataStreamManager);
        addTpch(nodeManager, metadataManager, splitManager, dataStreamManager, tpchBlocksProvider);

        return new LocalQueryRunner(metadataManager, splitManager, dataStreamManager, MockLocalStorageManager.createMockLocalStorageManager(), session);
    }

    private static void addSystem(InMemoryNodeManager nodeManager, MetadataManager metadataManager, SplitManager splitManager, DataStreamManager dataStreamManager)
    {
        SystemTablesMetadata systemTablesMetadata = new SystemTablesMetadata();
        metadataManager.addInternalSchemaMetadata(systemTablesMetadata);

        SystemSplitManager systemSplitManager = new SystemSplitManager(nodeManager);
        splitManager.addConnectorSplitManager(systemSplitManager);

        SystemDataStreamProvider systemDataStreamProvider = new SystemDataStreamProvider();
        dataStreamManager.addConnectorDataStreamProvider(systemDataStreamProvider);

        SystemTablesManager systemTablesManager = new SystemTablesManager(systemTablesMetadata, systemSplitManager, systemDataStreamProvider, ImmutableSet.<SystemTable>of());

        systemTablesManager.addTable(new NodesSystemTable(nodeManager));
    }

    private static void addTpch(InMemoryNodeManager nodeManager,
            MetadataManager metadataManager,
            SplitManager splitManager,
            DataStreamManager dataStreamManager,
            TpchBlocksProvider tpchBlocksProvider)
    {
        metadataManager.addConnectorMetadata(TPCH_CATALOG_NAME, new TpchMetadata());
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
        metadataManager.addInternalSchemaMetadata(new DualMetadata());
        splitManager.addConnectorSplitManager(new DualSplitManager(nodeManager));
        dataStreamManager.addConnectorDataStreamProvider(new DualDataStreamProvider());
    }
}
