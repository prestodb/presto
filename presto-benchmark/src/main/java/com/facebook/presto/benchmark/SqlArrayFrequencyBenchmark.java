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

public class SqlArrayFrequencyBenchmark
        extends AbstractSqlBenchmark
{
    public SqlArrayFrequencyBenchmark(LocalQueryRunner localQueryRunner, String query, String name)
    {
        super(localQueryRunner, name, 10, 10, query);
    }

    public static void main(String[] args)
    {
        new SqlArrayTransformBenchmark(createLocalQueryRunner(), "SELECT ARRAY_FREQUENCY(SHUFFLE(SEQUENCE(1, 10000)))", "sql_array_frequencey_all1_10k").runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new SqlArrayTransformBenchmark(createLocalQueryRunner(), "SELECT ARRAY_FREQUENCY(TRANSFORM(SEQUENCE(1, 10000), x->random(100)))", "sql_array_frequencey_10k").runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
