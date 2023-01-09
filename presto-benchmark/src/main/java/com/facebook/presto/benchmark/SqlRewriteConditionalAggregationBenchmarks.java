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

public class SqlRewriteConditionalAggregationBenchmarks
        extends AbstractSqlBenchmark
{
    private static final Logger LOGGER = Logger.get(SqlRewriteConditionalAggregationBenchmarks.class);

    public SqlRewriteConditionalAggregationBenchmarks(LocalQueryRunner localQueryRunner, @Language("SQL") String sql)
    {
        super(localQueryRunner, "sql_rewrite_conditional_aggregation", 5, 10, sql);
    }

    // The improvement from this optimization depends on two aspects: 1) how many aggregation computations we skip by pushing the condition to aggregation
    // 2) how expensive the aggregation is. In this benchmark, I include three queries which varies the two variants in benchmark.
    public static void main(String[] args)
    {
        Map<String, String> enableOptimization = ImmutableMap.of("optimize_conditional_aggregation_enabled", "true");
        LOGGER.info("SQL query with more grouping sets");
        String sqlWithCube = "SELECT IF(GROUPING(suppkey, orderkey, partkey) = FROM_BASE('011', 2), APPROX_DISTINCT(tax)),"
                + " IF(GROUPING(suppkey, orderkey, partkey) = FROM_BASE('011', 2), APPROX_DISTINCT(extendedprice)),"
                + " IF(GROUPING(suppkey, orderkey, partkey) = FROM_BASE('011', 2), APPROX_DISTINCT(discount))"
                + " FROM lineitem GROUP BY CUBE(suppkey, orderkey, partkey)";
        LOGGER.info("Without optimization");
        new SqlRewriteConditionalAggregationBenchmarks(createLocalQueryRunner(), sqlWithCube).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        LOGGER.info("With optimization");
        new SqlRewriteConditionalAggregationBenchmarks(createLocalQueryRunner(enableOptimization), sqlWithCube).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));

        LOGGER.info("SQL query with fewer grouping sets");
        String sqlNoCube = "SELECT IF(GROUPING(suppkey, orderkey, partkey) = FROM_BASE('011', 2), APPROX_DISTINCT(tax)),"
                + " IF(GROUPING(suppkey, orderkey, partkey) = FROM_BASE('101', 2), APPROX_DISTINCT(extendedprice)),"
                + " IF(GROUPING(suppkey, orderkey, partkey) = FROM_BASE('110', 2), APPROX_DISTINCT(discount))"
                + " FROM lineitem GROUP BY GROUPING SETS ((suppkey), (orderkey), (partkey))";
        LOGGER.info("Without optimization");
        new SqlRewriteConditionalAggregationBenchmarks(createLocalQueryRunner(), sqlNoCube).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        LOGGER.info("With optimization");
        new SqlRewriteConditionalAggregationBenchmarks(createLocalQueryRunner(enableOptimization), sqlNoCube).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));

        LOGGER.info("SQL query with cheaper aggregation functions");
        String sqlWithSum = "SELECT IF(GROUPING(suppkey, orderkey, partkey) = FROM_BASE('011', 2), SUM(tax)),"
                + " IF(GROUPING(suppkey, orderkey, partkey) = FROM_BASE('011', 2), SUM(extendedprice)),"
                + " IF(GROUPING(suppkey, orderkey, partkey) = FROM_BASE('011', 2), SUM(discount))"
                + " FROM lineitem GROUP BY CUBE(suppkey, orderkey, partkey)";
        LOGGER.info("Without optimization");
        new SqlRewriteConditionalAggregationBenchmarks(createLocalQueryRunner(), sqlWithSum).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        LOGGER.info("With optimization");
        new SqlRewriteConditionalAggregationBenchmarks(createLocalQueryRunner(enableOptimization), sqlWithSum).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
