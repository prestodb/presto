package com.facebook.presto.util;

import com.facebook.presto.importer.MockPeriodicImportManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.metadata.DualTable;
import com.facebook.presto.metadata.HostAddress;
import com.facebook.presto.metadata.InternalColumnHandle;
import com.facebook.presto.metadata.InternalTable;
import com.facebook.presto.metadata.InternalTableHandle;
import com.facebook.presto.metadata.LocalStorageManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MockLocalStorageManager;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.split.InternalSplit;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.Session;
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
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchSplit;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.analyzer.Session.DEFAULT_CATALOG;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_SCHEMA;
import static com.facebook.presto.sql.parser.TreeAssertions.assertFormattedSql;
import static com.facebook.presto.util.MaterializedResult.materialize;
import static com.facebook.presto.util.TestingTpchBlocksProvider.readTpchRecords;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertTrue;

public class LocalQueryRunner
{
    private final DataStreamProvider dataStreamProvider;
    private final Metadata metadata;
    private final LocalStorageManager storageManager;
    private final Session session;

    public LocalQueryRunner(DataStreamProvider dataStreamProvider,
            Metadata metadata,
            LocalStorageManager storageManager,
            Session session)
    {
        this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.session = checkNotNull(session, "session is null");
    }

    public MaterializedResult execute(@Language("SQL") String sql)
    {
        Statement statement = SqlParser.createStatement(sql);

        assertFormattedSql(statement);

        Analyzer analyzer = new Analyzer(session, metadata);

        Analysis analysis = analyzer.analyze(statement);

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanOptimizersFactory planOptimizersFactory = new PlanOptimizersFactory(metadata);
        Plan plan = new LogicalPlanner(session, planOptimizersFactory.get(), idAllocator, metadata, new MockPeriodicImportManager(), new MockStorageManager()).plan(analysis);
        new PlanPrinter().print(plan.getRoot(), plan.getTypes());

        SubPlan subplan = new DistributedLogicalPlanner(metadata, idAllocator).createSubplans(plan, true);
        assertTrue(subplan.getChildren().isEmpty(), "Expected subplan to have no children");

        DataSize maxOperatorMemoryUsage = new DataSize(256, MEGABYTE);
        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(session,
                new NodeInfo(new NodeConfig()
                        .setEnvironment("test")
                        .setNodeId("test-node")),
                metadata,
                plan.getTypes(),
                new OperatorStats(),
                new SourceHashProviderFactory(maxOperatorMemoryUsage),
                maxOperatorMemoryUsage,
                dataStreamProvider,
                storageManager,
                null);

        LocalExecutionPlan localExecutionPlan = executionPlanner.plan(subplan.getFragment().getRoot());

        // add the splits to the sources
        Map<PlanNodeId, SourceOperator> sourceOperators = localExecutionPlan.getSourceOperators();
        for (PlanNode source : subplan.getFragment().getSources()) {
            TableScanNode tableScan = (TableScanNode) source;
            SourceOperator sourceOperator = sourceOperators.get(tableScan.getId());
            Preconditions.checkArgument(sourceOperator != null, "Unknown plan source %s; known sources are %s", tableScan.getId(), sourceOperators.keySet());
            sourceOperator.addSplit(createSplit(tableScan.getTable()));
        }
        for (SourceOperator sourceOperator : sourceOperators.values()) {
            sourceOperator.noMoreSplits();
        }

        return materialize(localExecutionPlan.getRootOperator());
    }

    public static LocalQueryRunner createTpchLocalQueryRunner()
    {
        TestingTpchBlocksProvider tpchBlocksProvider = new TestingTpchBlocksProvider(ImmutableMap.of(
                "orders", readTpchRecords("orders"),
                "lineitem", readTpchRecords("lineitem")));

        DataStreamProvider dataProvider = new TpchDataStreamProvider(tpchBlocksProvider);
        Session session = new Session(null, TpchMetadata.TPCH_CATALOG_NAME, TpchMetadata.TPCH_SCHEMA_NAME);

        return new LocalQueryRunner(dataProvider, TpchMetadata.createTpchMetadata(), MockLocalStorageManager.createMockLocalStorageManager(), session);
    }

    public static LocalQueryRunner createDualLocalQueryRunner()
    {
        return createDualLocalQueryRunner(new Session(null, DEFAULT_CATALOG, DEFAULT_SCHEMA));
    }

    public static LocalQueryRunner createDualLocalQueryRunner(Session session)
    {
        DataStreamProvider dataProvider = new DualTableDataStreamProvider();
        Metadata metadata = new MetadataManager();
        return new LocalQueryRunner(dataProvider, metadata, MockLocalStorageManager.createMockLocalStorageManager(), session);
    }

    private static Split createSplit(TableHandle handle)
    {
        if (handle instanceof TpchTableHandle) {
            return new TpchSplit((TpchTableHandle) handle);
        }
        if (handle instanceof InternalTableHandle) {
            return new InternalSplit((InternalTableHandle) handle, ImmutableMap.<InternalColumnHandle, String>of(), ImmutableList.of(HostAddress.fromParts("127.0.0.1", 0)));
        }
        throw new IllegalArgumentException("unsupported table handle: " + handle.getClass().getName());
    }

    private static class DualTableDataStreamProvider
            implements DataStreamProvider
    {
        private static final QualifiedTableName DEFAULT_DUAL_TABLE = new QualifiedTableName(DEFAULT_CATALOG, DEFAULT_SCHEMA, DualTable.NAME);

        @Override
        public Operator createDataStream(Split split, List<ColumnHandle> columns)
        {
            checkArgument(columns.size() == 1, "expected exactly one column");
            InternalTable table = new DualTable().getInternalTable(DEFAULT_DUAL_TABLE);
            return new AlignmentOperator(ImmutableList.of(table.getColumn(0)));
        }
    }
}
