package com.facebook.presto.benchmark;

import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.server.HackPlanFragmentSourceProvider;
import com.facebook.presto.sql.analyzer.AnalysisResult;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DistributedLogicalPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.TableScanPlanFragmentSource;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.tpch.TpchSplit;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.intellij.lang.annotations.Language;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public abstract class AbstractSqlBenchmark
        extends AbstractOperatorBenchmark
{
    private final PlanFragment fragment;
    private final Metadata metadata;
    private final AnalysisResult analysis;
    private final Session session;

    protected AbstractSqlBenchmark(String benchmarkName, int warmupIterations, int measuredIterations, @Language("SQL") String query)
    {
        super(benchmarkName, warmupIterations, measuredIterations);

        Statement statement = SqlParser.createStatement(query);

        metadata = TpchSchema.createMetadata();

        session = new Session(null, TpchSchema.CATALOG_NAME, TpchSchema.SCHEMA_NAME);
        analysis = new Analyzer(session, metadata).analyze(statement);

        PlanNode plan = new LogicalPlanner(session, metadata).plan(analysis);
        fragment = new DistributedLogicalPlanner(metadata)
                .createSubplans(plan, analysis.getSymbolAllocator(), true)
                .getFragment();

        new PlanPrinter().print(fragment.getRoot(), analysis.getTypes());
    }

    @Override
    protected Operator createBenchmarkedOperator(TpchBlocksProvider provider)
    {
        ImmutableMap.Builder<TableHandle, TableScanPlanFragmentSource> builder = ImmutableMap.builder();
        for (PlanNode source : fragment.getSources()) {
            TableScanNode tableScan = (TableScanNode) source;
            TpchTableHandle handle = (TpchTableHandle) tableScan.getTable();

            builder.put(handle, new TableScanPlanFragmentSource(new TpchSplit(handle)));
        }

        DataSize maxOperatorMemoryUsage = new DataSize(100, MEGABYTE);
        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(session,
                metadata,
                new HackPlanFragmentSourceProvider(new TpchDataStreamProvider(provider), null, jsonCodec(TaskInfo.class)),
                analysis.getTypes(),
                null,
                builder.build(),
                ImmutableMap.<String, ExchangePlanFragmentSource>of(),
                new OperatorStats(),
                new SourceHashProviderFactory(maxOperatorMemoryUsage),
                maxOperatorMemoryUsage
        );

        return executionPlanner.plan(fragment.getRoot());
    }
}
