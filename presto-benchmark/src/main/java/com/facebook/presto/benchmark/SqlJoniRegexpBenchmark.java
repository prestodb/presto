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

public class SqlJoniRegexpBenchmark
        extends AbstractSqlBenchmark
{
    public SqlJoniRegexpBenchmark(LocalQueryRunner localQueryRunner, String query, String name)
    {
        super(localQueryRunner, name, 4, 5, query);
    }

    public static void main(String[] args)
    {
        new SqlJoniRegexpBenchmark(createLocalQueryRunner(), "SELECT array_agg(regexp_extract_all(comment||cast(random() as varchar), '[a-z]* ')) FROM orders cross join unnest(sequence(1, 10))", "sql_regexp_extract_alll").runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new SqlJoniRegexpBenchmark(createLocalQueryRunner(), "SELECT array_agg(regexp_replace(comment||cast(random() as varchar), '[a-z]* ', cast(random() as varchar))) FROM orders cross join unnest(sequence(1, 10))", "sql_regexp_replace").runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
