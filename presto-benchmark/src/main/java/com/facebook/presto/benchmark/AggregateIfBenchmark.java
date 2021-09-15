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

import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.SystemSessionProperties.AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY;
import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;

public abstract class AggregateIfBenchmark
{
    public static void main(String... args)
    {
        LocalQueryRunner localQueryRunnerWithRewrite = createLocalQueryRunner(ImmutableMap.of(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if"));
        LocalQueryRunner localQueryRunnerWithoutRewrite = createLocalQueryRunner(ImmutableMap.of(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "disabled"));
        new SumIfBenchmark(localQueryRunnerWithoutRewrite, false).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new SumIfBenchmark(localQueryRunnerWithRewrite, true).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new SumFilterBenchmark(localQueryRunnerWithRewrite).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }

    public static class SumIfBenchmark
            extends AbstractSqlBenchmark
    {
        public SumIfBenchmark(LocalQueryRunner localQueryRunner, boolean enableRewrite)
        {
            super(
                    localQueryRunner,
                    "sum_if_" + (enableRewrite ? "with_rewrite" : "without_rewrite"),
                    5,
                    50,
                    "SELECT SUM(IF(orderstatus = 'F', orderkey)) FROM tpch.sf1.orders");
        }
    }

    public static class SumFilterBenchmark
            extends AbstractSqlBenchmark
    {
        public SumFilterBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(
                    localQueryRunner,
                    "sum_filter",
                    5,
                    50,
                    "SELECT SUM(orderkey) FILTER(WHERE orderstatus = 'F') FROM tpch.sf1.orders");
        }
    }
}
