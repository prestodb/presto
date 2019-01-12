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

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.LocalQueryRunner;

import static io.prestosql.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static java.lang.String.format;

public class SqlTopNBenchmark
        extends AbstractSqlBenchmark
{
    public SqlTopNBenchmark(LocalQueryRunner localQueryRunner, int topN)
    {
        super(localQueryRunner,
                format("sql_top_%s", topN),
                4,
                5,
                format("select * from tpch.sf1.orders order by orderdate desc, totalprice, orderstatus, orderpriority desc limit %s", topN));
    }

    public static void main(String[] args)
    {
        LocalQueryRunner localQueryRunner = createLocalQueryRunner(ImmutableMap.of("resource_overcommit", "true"));
        for (int i = 0; i < 11; i++) {
            new SqlTopNBenchmark(localQueryRunner, (int) Math.pow(4, i)).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        }
    }
}
