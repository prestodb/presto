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

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;

public class SqlRewriteConstantArrayContainsToInExpressionBenchmarks
{
    private SqlRewriteConstantArrayContainsToInExpressionBenchmarks() {}

    public static void main(String[] args)
    {
        String sql = "";

        // This is a dummy run, as I found that the first run always shows worse performance no matter which query is put here
        sql = "select orderkey from lineitem cross join unnest(array[1, 2, 3, 4])t(idx) where contains(array[1], orderkey)";
        System.out.println("The first run is a dummy run, ignore this run");
        runQuery(sql);

        sql = "select orderkey from lineitem cross join unnest(array[1, 2, 3, 4])t(idx) where contains(array[1], orderkey)";
        runQuery(sql);

        sql = "select orderkey from lineitem cross join unnest(array[1, 2, 3, 4])t(idx) where contains(array[1, 2, 3, 4, 5, 6, 7, 8], orderkey)";
        runQuery(sql);

        sql = "select orderkey from lineitem cross join unnest(array[1, 2, 3, 4])t(idx) where contains(array[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32], orderkey)";
        runQuery(sql);

        sql = "select orderkey from lineitem cross join unnest(array[1, 2, 3, 4])t(idx) where contains(array[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, " +
                "32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56], orderkey)";
        runQuery(sql);

        sql = "select orderkey from lineitem cross join unnest(array[1, 2, 3, 4])t(idx) where any_match(array[orderkey, partkey, suppkey, orderkey+1, partkey+1, suppkey+1], x -> contains(array[1], x))";
        runQuery(sql);

        sql = "select orderkey from lineitem cross join unnest(array[1, 2, 3, 4])t(idx) where any_match(array[orderkey, partkey, suppkey, orderkey+1, partkey+1, suppkey+1], x -> contains(array[1, 2, 3, 4, 5, 6, 7, 8], x))";
        runQuery(sql);

        sql = "select orderkey from lineitem cross join unnest(array[1, 2, 3, 4])t(idx) where any_match(array[orderkey, partkey, suppkey, orderkey+1, partkey+1, suppkey+1], x -> contains(array[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32], x))";
        runQuery(sql);

        sql = "select orderkey from lineitem cross join unnest(array[1, 2, 3, 4])t(idx) where any_match(array[orderkey, partkey, suppkey, orderkey+1, partkey+1, suppkey+1], x -> contains(array[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, " +
                "32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56], x))";
        runQuery(sql);

        sql = "select orderkey from lineitem cross join unnest(array[1, 2, 3, 4])t(idx) where any_match(array[orderkey, partkey, suppkey], x -> contains(array[1, 2, 3, 4, 5, 6, 7, 8], x))";
        runQuery(sql);

        sql = "select orderkey from lineitem cross join unnest(array[1, 2, 3, 4])t(idx) where any_match(array[orderkey, partkey, suppkey, orderkey+1, partkey+1, suppkey+1], x -> contains(array[1, 2, 3, 4, 5, 6, 7, 8], x))";
        runQuery(sql);

        sql = "select orderkey from lineitem cross join unnest(array[1, 2, 3, 4])t(idx) where any_match(array[orderkey, partkey, suppkey, orderkey+1, partkey+1, suppkey+1, orderkey+2, partkey+2, suppkey+2, orderkey+3, partkey+3, suppkey+3], x -> contains(array[1, 2, 3, 4, 5, 6, 7, 8], x))";
        runQuery(sql);
    }

    private static void runQuery(String sql)
    {
        System.out.println(sql);
        System.out.println("Without optimization");
        new SqlRewriteConstantArrayContainsToInExpressionBenchmark(
                createLocalQueryRunner(ImmutableMap.of("rewrite_constant_array_contains_to_in_expression", "false")), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        System.out.println("With optimization");
        new SqlRewriteConstantArrayContainsToInExpressionBenchmark(
                createLocalQueryRunner(ImmutableMap.of("rewrite_constant_array_contains_to_in_expression", "true")), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }

    public static class SqlRewriteConstantArrayContainsToInExpressionBenchmark
            extends AbstractSqlBenchmark
    {
        public SqlRewriteConstantArrayContainsToInExpressionBenchmark(LocalQueryRunner localQueryRunner, String sql)
        {
            super(localQueryRunner, "sql_rewrite_constant_array_contains_to_in_expression", 10, 20, sql);
        }
    }
}
