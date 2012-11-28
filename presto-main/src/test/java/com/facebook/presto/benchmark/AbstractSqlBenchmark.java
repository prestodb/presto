package com.facebook.presto.benchmark;

import com.facebook.presto.operator.Operator;
import com.facebook.presto.sql.compiler.AnalysisResult;
import com.facebook.presto.sql.compiler.Analyzer;
import com.facebook.presto.sql.compiler.SessionMetadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExecutionPlanner;
import com.facebook.presto.sql.planner.FragmentPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanNode;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.Planner;
import com.facebook.presto.sql.planner.TableScan;
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

import java.util.List;

public abstract class AbstractSqlBenchmark
        extends AbstractOperatorBenchmark
{
    private final PlanFragment fragment;
    private final SessionMetadata sessionMetadata;
    private final AnalysisResult analysis;

    protected AbstractSqlBenchmark(String benchmarkName, int warmupIterations, int measuredIterations, @Language("SQL") String query)
    {
        super(benchmarkName, warmupIterations, measuredIterations);

        try {
            Statement statement = SqlParser.createStatement(query);

            sessionMetadata = new SessionMetadata(TpchSchema.createMetadata());
            sessionMetadata.using(TpchSchema.CATALOG_NAME, TpchSchema.SCHEMA_NAME);
            analysis = new Analyzer(sessionMetadata).analyze(statement);

            PlanNode plan = new Planner().plan((Query) statement, analysis);
            fragment = Iterables.getOnlyElement(new FragmentPlanner(sessionMetadata).createFragments(plan, analysis.getSymbolAllocator(), true));

            new PlanPrinter().print(fragment.getRoot(), analysis.getTypes());
        }
        catch (RecognitionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected Operator createBenchmarkedOperator(final TpchBlocksProvider provider)
    {
        TpchTableHandle table = (TpchTableHandle) ((TableScan) Iterables.getOnlyElement(fragment.getSources())).getTable();
        ExecutionPlanner executionPlanner = new ExecutionPlanner(sessionMetadata, new TpchDataStreamProvider(provider), analysis.getTypes(), new TpchSplit(table));
        return executionPlanner.plan(fragment.getRoot());
    }
}
