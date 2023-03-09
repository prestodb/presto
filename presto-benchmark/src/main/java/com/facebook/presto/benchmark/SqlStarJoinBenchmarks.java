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

public class SqlStarJoinBenchmarks
        extends AbstractSqlBenchmark
{
    private static final Logger LOGGER = Logger.get(SqlStarJoinBenchmarks.class);

    public SqlStarJoinBenchmarks(LocalQueryRunner localQueryRunner, @Language("SQL") String sql)
    {
        super(localQueryRunner, "sql_star_join", 2, 4, sql);
    }

    // The improvement from this optimization depends on two aspects: 1) how many aggregation computations we skip by pushing the condition to aggregation
    // 2) how expensive the aggregation is. In this benchmark, I include three queries which varies the two variants in benchmark.
    public static void main(String[] args)
    {
        String sql = "with l as (select partkey, tax from lineitem union all select partkey, tax from lineitem union all select partkey, tax from lineitem union all select partkey, tax from lineitem), ps as (select partkey, availqty from partsupp), ps2 as (select partkey, supplycost from partsupp), p as (select partkey, name from part), p2 as (select partkey, type from part) select l.partkey, l.tax, ps.availqty, p.name, ps2.supplycost, p2.type from l left join ps on l.partkey = ps.partkey left join ps2 on l.partkey = ps2.partkey left join p on l.partkey = p.partkey left join p2 on l.partkey = p2.partkey";
        Map<String, String> defaultSession = ImmutableMap.of("star_join_enabled", "false");
        Map<String, String> starjoin = ImmutableMap.of("star_join_enabled", "true");
        Map<String, String> starjoinBlock = ImmutableMap.of("star_join_enabled", "true", "star_join_probe_type", "block");
        Map<String, String> starjoinBlockTrack = ImmutableMap.of("star_join_enabled", "true", "star_join_probe_type", "blocktrack");
        Map<String, String> starjoinNolist = ImmutableMap.of("star_join_enabled", "true", "star_join_probe_type", "nolist");
        LOGGER.info("Without optimization");
        new SqlStarJoinBenchmarks(createLocalQueryRunner(defaultSession), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        LOGGER.info("starjoin");
        new SqlStarJoinBenchmarks(createLocalQueryRunner(starjoin), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        LOGGER.info("starjoinBlock");
        new SqlStarJoinBenchmarks(createLocalQueryRunner(starjoinBlock), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        LOGGER.info("starjoinBlockTrack");
        new SqlStarJoinBenchmarks(createLocalQueryRunner(starjoinBlockTrack), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        LOGGER.info("starjoinNolist");
        new SqlStarJoinBenchmarks(createLocalQueryRunner(starjoinNolist), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
