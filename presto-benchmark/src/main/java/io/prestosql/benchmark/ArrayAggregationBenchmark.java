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

public class ArrayAggregationBenchmark
        extends AbstractSqlBenchmark
{
    public ArrayAggregationBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "sql_double_array_agg", 10, 100, "select array_agg(totalprice) from orders group by orderkey");
    }

    public static void main(String[] args)
    {
        new ArrayAggregationBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
