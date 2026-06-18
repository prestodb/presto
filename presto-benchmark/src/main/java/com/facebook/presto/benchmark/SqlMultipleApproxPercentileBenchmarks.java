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

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static java.lang.String.format;

public final class SqlMultipleApproxPercentileBenchmarks
{
    private SqlMultipleApproxPercentileBenchmarks() {}

    public static class SqlMultipleApproxPercentileBenchmark
            extends AbstractSqlBenchmark
    {
        public SqlMultipleApproxPercentileBenchmark(LocalQueryRunner localQueryRunner, String sql)
        {
            super(localQueryRunner, "sql_multiple_approx_percentile", 10, 30, sql);
        }
    }

    public static String getSql(int approxPercentileNumber)
    {
        List<String> list = new ArrayList<>();
        double increment = 1.0 / (approxPercentileNumber + 1);
        for (int i = 0; i < approxPercentileNumber; ++i) {
            list.add(format(" approx_percentile(extendedprice, %f)", (i + 1) * increment));
        }
        String sql = format("select %s from lineitem", String.join(", ", list));
        return sql;
    }

    public static void main(String[] args)
    {
        for (int i = 1; i < 20; ++i) {
            System.out.println(format("%d approx_percentile functions", i));
            System.out.println("Without optimization");
            new SqlMultipleApproxPercentileBenchmark(createLocalQueryRunner(ImmutableMap.of("optimize_multiple_approx_percentile_on_same_field", "false")), getSql(i)).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
            System.out.println("With optimization");
            new SqlMultipleApproxPercentileBenchmark(createLocalQueryRunner(), getSql(i)).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        }
    }
}
