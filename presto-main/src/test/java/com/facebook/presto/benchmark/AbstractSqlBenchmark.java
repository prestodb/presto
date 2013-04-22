package com.facebook.presto.benchmark;

import com.facebook.presto.importer.MockPeriodicImportManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MockLocalStorageManager;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DistributedLogicalPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.storage.MockStorageManager;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchSplit;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.base.Preconditions;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import org.intellij.lang.annotations.Language;

import java.util.Map;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public abstract class AbstractSqlBenchmark
        extends AbstractOperatorBenchmark
{
    private final PlanFragment fragment;
    private final Metadata metadata;
    private final Analysis analysis;
    private final Session session;
    private final Map<Symbol,Type> symbols;

    protected AbstractSqlBenchmark(String benchmarkName, int warmupIterations, int measuredIterations, @Language("SQL") String query)
    {
        super(benchmarkName, warmupIterations, measuredIterations);

        Statement statement = SqlParser.createStatement(query);

        metadata = TpchMetadata.createTpchMetadata();

        session = new Session(null, TpchMetadata.TPCH_CATALOG_NAME, TpchMetadata.TPCH_SCHEMA_NAME);
        analysis = new Analyzer(session, metadata).analyze(statement);

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanOptimizersFactory planOptimizersFactory = new PlanOptimizersFactory(metadata);

        Plan plan = new LogicalPlanner(session, planOptimizersFactory.get(), idAllocator, metadata, new MockPeriodicImportManager(), new MockStorageManager()).plan(analysis);
        fragment = new DistributedLogicalPlanner(metadata, idAllocator)
                .createSubplans(plan, true)
                .getFragment();

        symbols = plan.getSymbolAllocator().getTypes();
        new PlanPrinter().print(fragment.getRoot(), symbols);
    }

    @Override
    protected Operator createBenchmarkedOperator(TpchBlocksProvider provider)
    {
        DataSize maxOperatorMemoryUsage = new DataSize(100, MEGABYTE);
        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(session,
                new NodeInfo(new NodeConfig()
                        .setEnvironment("test")
                        .setNodeId("test-node")),
                metadata,
                symbols,
                new OperatorStats(),
                new SourceHashProviderFactory(maxOperatorMemoryUsage),
                maxOperatorMemoryUsage,
                new TpchDataStreamProvider(provider),
                MockLocalStorageManager.createMockLocalStorageManager(),
                null);

        LocalExecutionPlan localExecutionPlan = executionPlanner.plan(fragment.getRoot());

        Map<PlanNodeId, SourceOperator> sourceOperators = localExecutionPlan.getSourceOperators();
        for (PlanNode source : fragment.getSources()) {
            TableScanNode tableScan = (TableScanNode) source;
            TpchTableHandle handle = (TpchTableHandle) tableScan.getTable();

            SourceOperator sourceOperator = sourceOperators.get(tableScan.getId());
            Preconditions.checkArgument(sourceOperator != null, "Unknown plan source %s; known sources are %s", tableScan.getId(), sourceOperators.keySet());
            sourceOperator.addSplit(new TpchSplit(handle));
        }
        for (SourceOperator sourceOperator : sourceOperators.values()) {
            sourceOperator.noMoreSplits();
        }

        return localExecutionPlan.getRootOperator();
    }
}
