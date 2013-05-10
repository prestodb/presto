package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

import static com.facebook.presto.benchmark.AbstractOperatorBenchmark.DEFAULT_TPCH_BLOCKS_PROVIDER;

public abstract class StatisticsBenchmark
{
    public static void main(String... args)
    {
        new LongVarianceBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
        new LongVariancePopBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
        new DoubleVarianceBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
        new DoubleVariancePopBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
        new LongStdDevBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
        new LongStdDevPopBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
        new DoubleStdDevBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
        new DoubleStdDevPopBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
    }

    public static class LongVarianceBenchmark
            extends AbstractSqlBenchmark
    {
        public LongVarianceBenchmark(TpchBlocksProvider tpchBlocksProvider)
        {
            super(tpchBlocksProvider, "stat_long_variance", 25, 150, "select var_samp(orderkey) from orders");
        }
    }

    public static class LongVariancePopBenchmark
            extends AbstractSqlBenchmark
    {
        public LongVariancePopBenchmark(TpchBlocksProvider tpchBlocksProvider)
        {
            super(tpchBlocksProvider, "stat_long_variance_pop", 25, 150, "select var_pop(orderkey) from orders");
        }
    }

    public static class DoubleVarianceBenchmark
            extends AbstractSqlBenchmark
    {
        public DoubleVarianceBenchmark(TpchBlocksProvider tpchBlocksProvider)
        {
            super(tpchBlocksProvider, "stat_double_variance", 25, 150, "select var_samp(totalprice) from orders");
        }
    }

    public static class DoubleVariancePopBenchmark
            extends AbstractSqlBenchmark
    {
        public DoubleVariancePopBenchmark(TpchBlocksProvider tpchBlocksProvider)
        {
            super(tpchBlocksProvider, "stat_double_variance_pop", 25, 150, "select var_pop(totalprice) from orders");
        }
    }

    public static class LongStdDevBenchmark
            extends AbstractSqlBenchmark
    {
        public LongStdDevBenchmark(TpchBlocksProvider tpchBlocksProvider)
        {
            super(tpchBlocksProvider, "stat_long_stddev", 25, 150, "select stddev_samp(orderkey) from orders");
        }
    }

    public static class LongStdDevPopBenchmark
            extends AbstractSqlBenchmark
    {
        public LongStdDevPopBenchmark(TpchBlocksProvider tpchBlocksProvider)
        {
            super(tpchBlocksProvider, "stat_long_stddev_pop", 25, 150, "select stddev_pop(orderkey) from orders");
        }
    }

    public static class DoubleStdDevBenchmark
            extends AbstractSqlBenchmark
    {
        public DoubleStdDevBenchmark(TpchBlocksProvider tpchBlocksProvider)
        {
            super(tpchBlocksProvider, "stat_double_stddev", 25, 150, "select stddev_samp(totalprice) from orders");
        }
    }

    public static class DoubleStdDevPopBenchmark
            extends AbstractSqlBenchmark
    {
        public DoubleStdDevPopBenchmark(TpchBlocksProvider tpchBlocksProvider)
        {
            super(tpchBlocksProvider, "stat_double_stddev_pop", 25, 150, "select stddev_pop(totalprice) from orders");
        }
    }
}
