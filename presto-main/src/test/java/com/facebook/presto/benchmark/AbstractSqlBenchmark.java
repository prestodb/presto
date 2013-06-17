package com.facebook.presto.benchmark;

import com.facebook.presto.operator.Operator;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.util.LocalQueryRunner;
import org.intellij.lang.annotations.Language;

import static com.facebook.presto.util.LocalQueryRunner.createTpchLocalQueryRunner;

public abstract class AbstractSqlBenchmark
        extends AbstractOperatorBenchmark
{
    @Language("SQL")
    private final String query;
    private final LocalQueryRunner tpchLocalQueryRunner;

    protected AbstractSqlBenchmark(TpchBlocksProvider tpchBlocksProvider, String benchmarkName, int warmupIterations, int measuredIterations, @Language("SQL") String query)
    {
        super(tpchBlocksProvider, benchmarkName, warmupIterations, measuredIterations);
        this.query = query;
        this.tpchLocalQueryRunner = createTpchLocalQueryRunner(getTpchBlocksProvider());
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        Operator operator = tpchLocalQueryRunner.plan(query);
        return operator;
    }
}
