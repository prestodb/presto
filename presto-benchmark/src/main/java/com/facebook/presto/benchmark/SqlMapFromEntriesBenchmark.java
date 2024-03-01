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
import org.intellij.lang.annotations.Language;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;

public class SqlMapFromEntriesBenchmark
        extends AbstractSqlBenchmark
{
    public SqlMapFromEntriesBenchmark(LocalQueryRunner localQueryRunner, String query, String name)
    {
        super(localQueryRunner, name, 10, 20, query);
    }

    public static void main(String[] args)
    {
        @Language("SQL") String query = "SELECT MAP_FROM_ENTRIES(ZIP(SEQUENCE(1, 1000), shuffle(SEQUENCE(1, 1000)))) AS x FROM (SELECT 1) CROSS JOIN UNNEST(SEQUENCE(1, 10000)) T(x)";
        new SqlMapFromEntriesBenchmark(createLocalQueryRunner(), query, "sql_map_entries_1k").runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
