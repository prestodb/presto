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

public class CastJsonParseBenchmark
        extends AbstractSqlBenchmark
{
    public CastJsonParseBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(
                localQueryRunner,
                "sql_cast_json_parse",
                10,
                100,
                "select cast(json_parse('[' || array_join(repeat(totalprice, 100), ',') || ']') as array(real)) from orders");
    }

    public static void main(String[] args)
    {
        new CastJsonParseBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
