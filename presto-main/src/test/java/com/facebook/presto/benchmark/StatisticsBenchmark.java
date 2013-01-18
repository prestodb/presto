package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.tpch.TpchBlocksProvider;
import org.intellij.lang.annotations.Language;

public abstract class StatisticsBenchmark
        extends AbstractSqlBenchmark
{
    protected StatisticsBenchmark(String benchmarkName, int warmupIterations, int measuredIterations, @Language("SQL") String query)
    {
        super(benchmarkName, warmupIterations, measuredIterations, query);
    }

    @Override
    protected long execute(TpchBlocksProvider blocksProvider)
    {
        Operator operator = createBenchmarkedOperator(blocksProvider);

        long outputRows = 0;
        for (PageIterator iterator = operator.iterator(new OperatorStats());  iterator.hasNext(); ) {
            Page page = iterator.next();
            BlockCursor cursor = page.getBlock(0).cursor();
            while (cursor.advanceNextPosition()) {
                outputRows++;
            }
        }
        return outputRows;
    }

    public static final void main(String ... args) throws Exception
    {
        new LongVarianceBenchmark().runBenchmark(new AverageBenchmarkResults());
        new LongVariancePopBenchmark().runBenchmark(new AverageBenchmarkResults());
        new DoubleVarianceBenchmark().runBenchmark(new AverageBenchmarkResults());
        new DoubleVariancePopBenchmark().runBenchmark(new AverageBenchmarkResults());
        new LongStdDevBenchmark().runBenchmark(new AverageBenchmarkResults());
        new LongStdDevPopBenchmark().runBenchmark(new AverageBenchmarkResults());
        new DoubleStdDevBenchmark().runBenchmark(new AverageBenchmarkResults());
        new DoubleStdDevPopBenchmark().runBenchmark(new AverageBenchmarkResults());
    }

    public static class LongVarianceBenchmark
            extends StatisticsBenchmark
    {
        public LongVarianceBenchmark()
        {
            super("stat_long_variance", 25, 150, "select var_samp(orderkey) from orders");
        }
    }

    public static class LongVariancePopBenchmark
            extends StatisticsBenchmark
    {
        public LongVariancePopBenchmark()
        {
            super("stat_long_variance_pop", 25, 150, "select var_pop(orderkey) from orders");
        }
    }

    public static class DoubleVarianceBenchmark
            extends StatisticsBenchmark
    {
        public DoubleVarianceBenchmark()
        {
            super("stat_double_variance", 25, 150, "select var_samp(totalprice) from orders");
        }
    }

    public static class DoubleVariancePopBenchmark
            extends StatisticsBenchmark
    {
        public DoubleVariancePopBenchmark()
        {
            super("stat_double_variance_pop", 25, 150, "select var_pop(totalprice) from orders");
        }
    }

    public static class LongStdDevBenchmark
            extends StatisticsBenchmark
    {
        public LongStdDevBenchmark()
        {
            super("stat_long_stddev", 25, 150, "select stddev_samp(orderkey) from orders");
        }
    }

    public static class LongStdDevPopBenchmark
            extends StatisticsBenchmark
    {
        public LongStdDevPopBenchmark()
        {
            super("stat_long_stddev_pop", 25, 150, "select stddev_pop(orderkey) from orders");
        }
    }

    public static class DoubleStdDevBenchmark
            extends StatisticsBenchmark
    {
        public DoubleStdDevBenchmark()
        {
            super("stat_double_stddev", 25, 150, "select stddev_samp(totalprice) from orders");
        }
    }

    public static class DoubleStdDevPopBenchmark
            extends StatisticsBenchmark
    {
        public DoubleStdDevPopBenchmark()
        {
            super("stat_double_stddev_pop", 25, 150, "select stddev_pop(totalprice) from orders");
        }
    }
}
