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

import static com.facebook.presto.SystemSessionProperties.EXPLOIT_CONSTRAINTS;
import static com.facebook.presto.SystemSessionProperties.IN_PREDICATES_AS_INNER_JOINS_ENABLED;
import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_BELOW_JOIN_BYTE_REDUCTION_THRESHOLD;

public class SqlEarlyOutJoinsBenchmarks
        extends AbstractSqlBenchmark
{
    private static final Logger LOGGER = Logger.get(SqlEarlyOutJoinsBenchmarks.class);

    private static Map<String, String> disableOptimization = ImmutableMap.of(IN_PREDICATES_AS_INNER_JOINS_ENABLED, Boolean.toString(false),
            EXPLOIT_CONSTRAINTS, Boolean.toString(true));
    private static Map<String, String> enableOptimization = ImmutableMap.of(IN_PREDICATES_AS_INNER_JOINS_ENABLED, Boolean.toString(true),
            EXPLOIT_CONSTRAINTS, Boolean.toString(true));

    public SqlEarlyOutJoinsBenchmarks(LocalQueryRunner localQueryRunner, @Language("SQL") String sql)
    {
        super(localQueryRunner, "early_out_joins", 10, 10, sql);
    }

    public static void main(String[] args)
    {
        benchmarkTransformDistinctInnerJoinToLeftEarlyOutJoin();
        benchmarkTransformDistinctInnerJoinToRightEarlyOutJoin();
        benchmarkRewriteOfInPredicateToDistinctInnerJoin();
    }

    private static void benchmarkTransformDistinctInnerJoinToLeftEarlyOutJoin()
    {
        LOGGER.info("benchmarkTransformDistinctInnerJoinToLeftEarlyOutJoin");
        String sql = "select distinct orderkey from lineitem, nation where orderkey=nationkey";
        LOGGER.info("Without optimization");
        new SqlEarlyOutJoinsBenchmarks(BenchmarkQueryRunner.createLocalQueryRunner(disableOptimization), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        LOGGER.info("With optimization");
        new SqlEarlyOutJoinsBenchmarks(BenchmarkQueryRunner.createLocalQueryRunner(enableOptimization), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }

    private static void benchmarkTransformDistinctInnerJoinToRightEarlyOutJoin()
    {
        LOGGER.info("benchmarkTransformDistinctInnerJoinToRightEarlyOutJoin");
        String sql = "select distinct l.orderkey, l.comment from lineitem l, orders o where l.orderkey = o.orderkey";
        LOGGER.info("Without optimization");
        new SqlEarlyOutJoinsBenchmarks(BenchmarkQueryRunner.createLocalQueryRunner(disableOptimization), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        LOGGER.info("With optimization");
        new SqlEarlyOutJoinsBenchmarks(BenchmarkQueryRunner.createLocalQueryRunner(enableOptimization), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }

    private static void benchmarkRewriteOfInPredicateToDistinctInnerJoin()
    {
        LOGGER.info("benchmarkInPredicateToDistinctInnerJoin");
        LOGGER.info("Case 1: Rewrite IN predicate to distinct + inner join");
        String sql = " explain select * from region where regionkey in (select orderkey from lineitem)";
        LOGGER.info("Without optimization");
        new SqlEarlyOutJoinsBenchmarks(BenchmarkQueryRunner.createLocalQueryRunner(disableOptimization), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        LOGGER.info("With optimization: case 1");
        new SqlEarlyOutJoinsBenchmarks(BenchmarkQueryRunner.createLocalQueryRunner(enableOptimization), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));

        LOGGER.info("Case 2: Rewrite IN predicate to distinct + inner join and then push aggregation down into the probe of the join");
        //Use same query as previous and change the byte reduction threshold
        LOGGER.info("With optimization: case 2");
        Map<String, String> alteredByteReductionThreshold = ImmutableMap.<String, String>builder()
                .putAll(enableOptimization)
                .put(PUSH_AGGREGATION_BELOW_JOIN_BYTE_REDUCTION_THRESHOLD, "0.001")
                .build();
        new SqlEarlyOutJoinsBenchmarks(BenchmarkQueryRunner.createLocalQueryRunner(enableOptimization), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
