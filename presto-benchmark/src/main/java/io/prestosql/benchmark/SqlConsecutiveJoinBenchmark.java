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

public class SqlConsecutiveJoinBenchmark
        extends AbstractSqlBenchmark
{
    public SqlConsecutiveJoinBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner,
                "sql_consecutive_join",
                4,
                5,
                "SELECT * FROM " +
                        "( " +
                        "  SELECT orders.orderkey as key, * FROM " +
                        "  customer JOIN orders " +
                        "  ON customer.custkey = orders.custkey " +
                        ") t1 " +
                        "JOIN " +
                        "( " +
                        "  SELECT lineitem.orderkey as key, * FROM " +
                        "  part JOIN lineitem " +
                        "  ON part.partkey = lineitem.partkey " +
                        ") t2 " +
                        "ON t1.key = t2.key");
    }

    public static void main(String[] args)
    {
        try (LocalQueryRunner queryRunner = createLocalQueryRunner(ImmutableMap.of("reorder_joins", "false"))) {
            new SqlConsecutiveJoinBenchmark(queryRunner).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        }
    }
}
