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
package io.prestosql.benchmark;

import io.prestosql.testing.LocalQueryRunner;

import static io.prestosql.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;

public abstract class StatisticsBenchmark
{
    public static void main(String... args)
    {
        LocalQueryRunner localQueryRunner = createLocalQueryRunner();
        new LongVarianceBenchmark(localQueryRunner).runBenchmark(new AverageBenchmarkResults());
        new LongVariancePopBenchmark(localQueryRunner).runBenchmark(new AverageBenchmarkResults());
        new DoubleVarianceBenchmark(localQueryRunner).runBenchmark(new AverageBenchmarkResults());
        new DoubleVariancePopBenchmark(localQueryRunner).runBenchmark(new AverageBenchmarkResults());
        new LongStdDevBenchmark(localQueryRunner).runBenchmark(new AverageBenchmarkResults());
        new LongStdDevPopBenchmark(localQueryRunner).runBenchmark(new AverageBenchmarkResults());
        new DoubleStdDevBenchmark(localQueryRunner).runBenchmark(new AverageBenchmarkResults());
        new DoubleStdDevPopBenchmark(localQueryRunner).runBenchmark(new AverageBenchmarkResults());
    }

    public static class LongVarianceBenchmark
            extends AbstractSqlBenchmark
    {
        public LongVarianceBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "stat_long_variance", 25, 150, "select var_samp(orderkey) from orders");
        }
    }

    public static class LongVariancePopBenchmark
            extends AbstractSqlBenchmark
    {
        public LongVariancePopBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "stat_long_variance_pop", 25, 150, "select var_pop(orderkey) from orders");
        }
    }

    public static class DoubleVarianceBenchmark
            extends AbstractSqlBenchmark
    {
        public DoubleVarianceBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "stat_double_variance", 25, 150, "select var_samp(totalprice) from orders");
        }
    }

    public static class DoubleVariancePopBenchmark
            extends AbstractSqlBenchmark
    {
        public DoubleVariancePopBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "stat_double_variance_pop", 25, 150, "select var_pop(totalprice) from orders");
        }
    }

    public static class LongStdDevBenchmark
            extends AbstractSqlBenchmark
    {
        public LongStdDevBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "stat_long_stddev", 25, 150, "select stddev_samp(orderkey) from orders");
        }
    }

    public static class LongStdDevPopBenchmark
            extends AbstractSqlBenchmark
    {
        public LongStdDevPopBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "stat_long_stddev_pop", 25, 150, "select stddev_pop(orderkey) from orders");
        }
    }

    public static class DoubleStdDevBenchmark
            extends AbstractSqlBenchmark
    {
        public DoubleStdDevBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "stat_double_stddev", 25, 150, "select stddev_samp(totalprice) from orders");
        }
    }

    public static class DoubleStdDevPopBenchmark
            extends AbstractSqlBenchmark
    {
        public DoubleStdDevPopBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "stat_double_stddev_pop", 25, 150, "select stddev_pop(totalprice) from orders");
        }
    }
}
