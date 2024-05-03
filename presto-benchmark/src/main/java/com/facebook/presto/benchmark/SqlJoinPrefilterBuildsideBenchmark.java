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

public class SqlJoinPrefilterBuildsideBenchmark
        extends AbstractSqlBenchmark
{
    public SqlJoinPrefilterBuildsideBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner,
                "join_prefilter_build_side",
                4,
                5,
                "select t1.*, partkey from orders t1 left join lineitem t2 using (orderkey) where custkey < 10");
    }

    public static void main(String[] args)
    {
        new SqlJoinPrefilterBuildsideBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new SqlJoinPrefilterBuildsideBenchmark(createLocalQueryRunner(ImmutableMap.of("join_prefilter_build_side", "true"))).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
