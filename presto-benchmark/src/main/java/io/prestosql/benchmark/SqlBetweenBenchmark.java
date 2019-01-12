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

public class SqlBetweenBenchmark
        extends AbstractSqlBenchmark
{
    public SqlBetweenBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "sql_between_long", 10, 30, "SELECT COUNT(*) FROM orders WHERE custkey BETWEEN 10000 AND 20000 OR custkey BETWEEN 30000 AND 35000 OR custkey BETWEEN 50000 AND 51000");
    }

    public static void main(String[] args)
    {
        new SqlBetweenBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
