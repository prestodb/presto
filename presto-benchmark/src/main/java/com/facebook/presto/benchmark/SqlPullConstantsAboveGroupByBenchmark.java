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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;

import java.util.Map;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;

public class SqlPullConstantsAboveGroupByBenchmark
        extends AbstractSqlBenchmark
{
    private static final Logger LOGGER = Logger.get(SqlRewriteConditionalAggregationBenchmarks.class);

    public SqlPullConstantsAboveGroupByBenchmark(LocalQueryRunner localQueryRunner, @Language("SQL") String sql)
    {
        super(localQueryRunner,
                "pull_constants_above_group_by",
                10,
                20,
                sql);
    }

    public static void main(String[] args)
    {
        Map<String, String> disableOptimization = ImmutableMap.of("optimize_constant_grouping_keys", "false");
        String sql = "SELECT * FROM (SELECT regionkey, col, count(*) FROM (SELECT regionkey, 'bla' as col FROM nation) GROUP BY regionkey, col)";
        LOGGER.info("Without optimization");
        new SqlPullConstantsAboveGroupByBenchmark(createLocalQueryRunner(disableOptimization), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        LOGGER.info("With optimization");
        new SqlPullConstantsAboveGroupByBenchmark(createLocalQueryRunner(), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
