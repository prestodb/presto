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
    protected StatisticsBenchmark(final String benchmarkName, final int warmupIterations, final int measuredIterations, @Language("SQL") final String query)
    {
        super(benchmarkName, warmupIterations, measuredIterations, query);
    }

    @Override
    protected long execute(final TpchBlocksProvider blocksProvider)
    {
        final Operator operator = createBenchmarkedOperator(blocksProvider);

        long outputRows = 0;
        for (final PageIterator iterator = operator.iterator(new OperatorStats());  iterator.hasNext(); ) {
            final Page page = iterator.next();
            final BlockCursor cursor = page.getBlock(0).cursor();
            while (cursor.advanceNextPosition()) {
                outputRows++;
            }
        }
        return outputRows;
    }

    public static final void main(String ... args) throws Exception
    {
        new LongVarianceBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new LongVariancePopBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new DoubleVarianceBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new DoubleVariancePopBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new LongStdDevBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new LongStdDevPopBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new DoubleStdDevBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new DoubleStdDevPopBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }

    public static class LongVarianceBenchmark
        extends StatisticsBenchmark
    {
        public LongVarianceBenchmark()
        {
            super("stat_long_variance", 5, 50, "select var_samp(orderkey) from orders");
        }
    }

    public static class LongVariancePopBenchmark
        extends StatisticsBenchmark
    {
        public LongVariancePopBenchmark()
        {
            super("stat_long_variance_pop", 5, 50, "select var_pop(orderkey) from orders");
        }
    }

    public static class DoubleVarianceBenchmark
        extends StatisticsBenchmark
    {
        public DoubleVarianceBenchmark()
        {
            super("stat_double_variance", 5, 50, "select var_samp(totalprice) from orders");
        }
    }

    public static class DoubleVariancePopBenchmark
        extends StatisticsBenchmark
    {
        public DoubleVariancePopBenchmark()
        {
            super("stat_double_variance_pop", 5, 50, "select var_pop(totalprice) from orders");
        }
    }

    public static class LongStdDevBenchmark
        extends StatisticsBenchmark
    {
        public LongStdDevBenchmark()
        {
            super("stat_long_stddev", 5, 50, "select stddev_samp(orderkey) from orders");
        }
    }

    public static class LongStdDevPopBenchmark
        extends StatisticsBenchmark
    {
        public LongStdDevPopBenchmark()
        {
            super("stat_long_stddev_pop", 5, 50, "select stddev_pop(orderkey) from orders");
        }
    }

    public static class DoubleStdDevBenchmark
        extends StatisticsBenchmark
    {
        public DoubleStdDevBenchmark()
        {
            super("stat_double_stddev", 5, 50, "select stddev_samp(totalprice) from orders");
        }
    }

    public static class DoubleStdDevPopBenchmark
        extends StatisticsBenchmark
    {
        public DoubleStdDevPopBenchmark()
        {
            super("stat_double_stddev_pop", 5, 50, "select stddev_pop(totalprice) from orders");
        }
    }
}
