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

public class SqlStringConcatBenchmark
        extends AbstractSqlBenchmark
{
    public SqlStringConcatBenchmark(LocalQueryRunner localQueryRunner, String query, String name)
    {
        super(localQueryRunner, name, 4, 5, query);
    }

    public static void main(String[] args)
    {
        new SqlArrayConcatBenchmark(createLocalQueryRunner(), "SELECT CONCAT(a[random(x) % 5 + 1], CONCAT(a[2], CONCAT(CONCAT(a[3], concat(a[4], concat(a[5],a[6]))), a[7])), a[8]) FROM (select x, set_agg(name) a from nation CROSS JOIN UNNEST(SEQUENCE(1, 1000)) AS T(x) group by x)", "sql_concat_1k").runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
