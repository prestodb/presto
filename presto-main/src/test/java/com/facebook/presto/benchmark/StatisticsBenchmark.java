/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

import java.util.concurrent.ExecutorService;

import static com.facebook.presto.benchmark.AbstractOperatorBenchmark.DEFAULT_TPCH_BLOCKS_PROVIDER;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public abstract class StatisticsBenchmark
{
    public static void main(String... args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new LongVarianceBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
        new LongVariancePopBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
        new DoubleVarianceBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
        new DoubleVariancePopBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
        new LongStdDevBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
        new LongStdDevPopBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
        new DoubleStdDevBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
        new DoubleStdDevPopBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new AverageBenchmarkResults());
    }

    public static class LongVarianceBenchmark
            extends AbstractSqlBenchmark
    {
        public LongVarianceBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
        {
            super(executor, tpchBlocksProvider, "stat_long_variance", 25, 150, "select var_samp(orderkey) from orders");
        }
    }

    public static class LongVariancePopBenchmark
            extends AbstractSqlBenchmark
    {
        public LongVariancePopBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
        {
            super(executor, tpchBlocksProvider, "stat_long_variance_pop", 25, 150, "select var_pop(orderkey) from orders");
        }
    }

    public static class DoubleVarianceBenchmark
            extends AbstractSqlBenchmark
    {
        public DoubleVarianceBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
        {
            super(executor, tpchBlocksProvider, "stat_double_variance", 25, 150, "select var_samp(totalprice) from orders");
        }
    }

    public static class DoubleVariancePopBenchmark
            extends AbstractSqlBenchmark
    {
        public DoubleVariancePopBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
        {
            super(executor, tpchBlocksProvider, "stat_double_variance_pop", 25, 150, "select var_pop(totalprice) from orders");
        }
    }

    public static class LongStdDevBenchmark
            extends AbstractSqlBenchmark
    {
        public LongStdDevBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
        {
            super(executor, tpchBlocksProvider, "stat_long_stddev", 25, 150, "select stddev_samp(orderkey) from orders");
        }
    }

    public static class LongStdDevPopBenchmark
            extends AbstractSqlBenchmark
    {
        public LongStdDevPopBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
        {
            super(executor, tpchBlocksProvider, "stat_long_stddev_pop", 25, 150, "select stddev_pop(orderkey) from orders");
        }
    }

    public static class DoubleStdDevBenchmark
            extends AbstractSqlBenchmark
    {
        public DoubleStdDevBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
        {
            super(executor, tpchBlocksProvider, "stat_double_stddev", 25, 150, "select stddev_samp(totalprice) from orders");
        }
    }

    public static class DoubleStdDevPopBenchmark
            extends AbstractSqlBenchmark
    {
        public DoubleStdDevPopBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
        {
            super(executor, tpchBlocksProvider, "stat_double_stddev_pop", 25, 150, "select stddev_pop(totalprice) from orders");
        }
    }
}
