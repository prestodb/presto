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

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;

public class SqlSetAggBenchmark
        extends AbstractSqlBenchmark
{
    public SqlSetAggBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "sql_set_agg_long", 10, 30, "select set_agg(comment) from orders group by custkey");
    }

    public static void main(String[] args)
    {
        new SqlSetAggBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
