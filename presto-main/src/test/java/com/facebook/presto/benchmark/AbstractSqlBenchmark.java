package com.facebook.presto.benchmark;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.server.ExchangePlanFragmentSource;
import com.facebook.presto.server.HackPlanFragmentSourceProvider;
import com.facebook.presto.server.QueryTaskInfo;
import com.facebook.presto.server.TableScanPlanFragmentSource;
import com.facebook.presto.sql.analyzer.AnalysisResult;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.FragmentPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.Planner;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.tpch.TpchSplit;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.antlr.runtime.RecognitionException;
import org.intellij.lang.annotations.Language;

import static io.airlift.json.JsonCodec.jsonCodec;

public abstract class AbstractSqlBenchmark
        extends AbstractOperatorBenchmark
{
    private final PlanFragment fragment;
    private final Metadata metadata;
    private final AnalysisResult analysis;

    protected AbstractSqlBenchmark(String benchmarkName, int warmupIterations, int measuredIterations, @Language("SQL") String query)
    {
        super(benchmarkName, warmupIterations, measuredIterations);

        try {
            Statement statement = SqlParser.createStatement(query);

            metadata = TpchSchema.createMetadata();

            Session session = new Session(TpchSchema.CATALOG_NAME, TpchSchema.SCHEMA_NAME);
            analysis = new Analyzer(session, metadata).analyze(statement);

            PlanNode plan = new Planner().plan((Query) statement, analysis);
            fragment = Iterables.getOnlyElement(new FragmentPlanner(metadata).createFragments(plan, analysis.getSymbolAllocator(), true));

            new PlanPrinter().print(fragment.getRoot(), analysis.getTypes());
        }
        catch (RecognitionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected Operator createBenchmarkedOperator(final TpchBlocksProvider provider)
    {
        ImmutableMap.Builder<TableHandle, TableScanPlanFragmentSource> builder = ImmutableMap.builder();
        for (PlanNode source : fragment.getSources()) {
            TableScanNode tableScan = (TableScanNode) source;
            TpchTableHandle handle = (TpchTableHandle) tableScan.getTable();

            builder.put(handle, new TableScanPlanFragmentSource(new TpchSplit(handle)));
        }

        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(metadata,
                new HackPlanFragmentSourceProvider(new TpchDataStreamProvider(provider), null, jsonCodec(QueryTaskInfo.class)),
                analysis.getTypes(),
                null,
                builder.build(),
                ImmutableMap.<String, ExchangePlanFragmentSource>of(),
                new SourceHashProviderFactory());

        return executionPlanner.plan(fragment.getRoot());
    }
}
