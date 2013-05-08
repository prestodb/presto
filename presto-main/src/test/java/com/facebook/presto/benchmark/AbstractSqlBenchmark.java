package com.facebook.presto.benchmark;

import com.facebook.presto.operator.Operator;
import com.facebook.presto.util.LocalQueryRunner;
import org.intellij.lang.annotations.Language;

import static com.facebook.presto.util.LocalQueryRunner.createTpchLocalQueryRunner;

public abstract class AbstractSqlBenchmark
        extends AbstractOperatorBenchmark
{
    @Language("SQL")
    private final String query;
    private final LocalQueryRunner tpchLocalQueryRunner;

    protected AbstractSqlBenchmark(String benchmarkName, int warmupIterations, int measuredIterations, @Language("SQL") String query)
    {
        super(benchmarkName, warmupIterations, measuredIterations);
        this.query = query;

        tpchLocalQueryRunner = createTpchLocalQueryRunner(getTpchBlocksProvider());
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        return tpchLocalQueryRunner.plan(query);
    }
}
