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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.LocalQueryRunner;

import static io.prestosql.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static java.lang.String.format;

public class SqlTopNRowNumberBenchmark
        extends AbstractSqlBenchmark
{
    public SqlTopNRowNumberBenchmark(LocalQueryRunner localQueryRunner, String function, String partitions, int topN)
    {
        super(localQueryRunner,
                format("sql_%s_partition_by_(%s)_top_%s", function, partitions, topN),
                4,
                5,
                format("WITH t AS (" +
                        "  SELECT *, %s() OVER (PARTITION BY %s ORDER BY shipdate DESC) AS rnk" +
                        "  FROM lineitem" +
                        ")" +
                        "SELECT * FROM t WHERE rnk <= %s", function, partitions, topN));
    }

    public static void main(String[] args)
    {
        LocalQueryRunner localQueryRunner = createLocalQueryRunner(ImmutableMap.of("resource_overcommit", "true"));
        for (String function : ImmutableList.of("row_number", "rank")) {
            for (String partitions : ImmutableList.of("orderkey, partkey", "partkey", "linestatus")) {
                for (int topN : ImmutableList.of(1, 100, 10_000)) {
                    new SqlTopNRowNumberBenchmark(localQueryRunner, function, partitions, topN).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
                }
            }
        }
    }
}
