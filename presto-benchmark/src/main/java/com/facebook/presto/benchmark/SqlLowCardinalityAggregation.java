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
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;

public class SqlLowCardinalityAggregation
        extends AbstractSqlBenchmark
{
    public SqlLowCardinalityAggregation(LocalQueryRunner localQueryRunner, String sql)
    {
        super(localQueryRunner, "sql_low_cardinality_aggregation", 5, 5, sql);
    }

    public static void main(String[] args)
    {
        String sqlNoBlock = "with t as (select '2023-03-20' as ds, * from lineitem) select ds, sum(linenumber) from t group by ds";
        String sqlBlock = "with t as (select '2023-03-20' as ds, * from lineitem) select ds, sum_block(linenumber) from t group by ds";

        System.out.println("No block sum, hash generation on");
        new SqlLowCardinalityAggregation(createLocalQueryRunner(ImmutableMap.of("optimize_hash_generation", "true")), sqlNoBlock).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));

        System.out.println("No block sum, hash generation off");
        new SqlLowCardinalityAggregation(createLocalQueryRunner(ImmutableMap.of("optimize_hash_generation", "false")), sqlNoBlock).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));

        System.out.println("Block sum, hash generation on");
        new SqlLowCardinalityAggregation(createLocalQueryRunner(ImmutableMap.of("optimize_hash_generation", "true")), sqlBlock).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));

        System.out.println("Block sum, hash generation off");
        new SqlLowCardinalityAggregation(createLocalQueryRunner(ImmutableMap.of("optimize_hash_generation", "false")), sqlBlock).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
