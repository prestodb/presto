package com.facebook.presto.benchmark;

import com.facebook.presto.metadata.LegacyStorageManager;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.sql.compiler.AnalysisResult;
import com.facebook.presto.sql.compiler.Analyzer;
import com.facebook.presto.sql.compiler.SessionMetadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExecutionPlanner;
import com.facebook.presto.sql.planner.PlanNode;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.Planner;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.facebook.presto.tpch.TpchLegacyStorageManagerAdapter;
import com.facebook.presto.tpch.TpchSchema;
import com.google.common.base.Throwables;
import org.antlr.runtime.RecognitionException;
import org.intellij.lang.annotations.Language;

public abstract class AbstractSqlBenchmark
        extends AbstractOperatorBenchmark
{
    private final PlanNode plan;
    private final SessionMetadata sessionMetadata;

    protected AbstractSqlBenchmark(String benchmarkName, int warmupIterations, int measuredIterations, @Language("SQL") String query)
    {
        super(benchmarkName, warmupIterations, measuredIterations);

        try {
            Statement statement = SqlParser.createStatement(query);

            sessionMetadata = new SessionMetadata(TpchSchema.createMetadata());
            sessionMetadata.using(TpchSchema.CATALOG_NAME, TpchSchema.SCHEMA_NAME);
            AnalysisResult analysis = new Analyzer(sessionMetadata).analyze(statement);
            plan = new Planner().plan((Query) statement, analysis);

            new PlanPrinter().print(plan);
        }
        catch (RecognitionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected Operator createBenchmarkedOperator(final TpchBlocksProvider provider)
    {
        LegacyStorageManager storage = new TpchLegacyStorageManagerAdapter(new TpchDataStreamProvider(provider));
        ExecutionPlanner executionPlanner = new ExecutionPlanner(sessionMetadata, storage);
        return executionPlanner.plan(plan);
    }
}
