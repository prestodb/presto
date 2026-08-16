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
package com.facebook.presto.hive.benchmark;

import org.testng.annotations.Test;

/**
 * Benchmarks the PreAggregateBeforeGroupId optimization against a baseline
 * and the existing AddExchangesBelowPartialAggregationOverGroupIdRuleSet.
 *
 * <p>Uses CROSS JOIN UNNEST to amplify lineitem rows without triggering
 * PushPartialAggregationThroughExchange (which would happen with UNION ALL).
 *
 * <p>Run via:
 * <pre>
 * mvn test -pl presto-hive \
 *   -Dtest=BenchmarkGroupingSetsPreAggregation \
 *   -DfailIfNoTests=false
 * </pre>
 */
public final class BenchmarkGroupingSetsPreAggregation
{
    private static final String QUERY =
            "SELECT yr, mo, dy, shipmode, returnflag, " +
            "sum(quantity), count(*), min(extendedprice), max(extendedprice) " +
            "FROM (" +
            "  SELECT year(shipdate) AS yr, month(shipdate) AS mo, day(shipdate) AS dy, " +
            "         shipmode, returnflag, quantity, extendedprice " +
            "  FROM lineitem " +
            "  CROSS JOIN UNNEST(ARRAY[1,2,3,4,5]) AS t(x)" +
            ") t " +
            "GROUP BY CUBE (yr, mo, dy, shipmode, returnflag)";

    @Test
    public void benchmark()
            throws Exception
    {
        try (HiveDistributedBenchmarkRunner runner =
                     new HiveDistributedBenchmarkRunner(3, 5)) {
            runner.addScenario("baseline", builder -> {
                builder.setSystemProperty("pre_aggregate_before_grouping_sets", "false");
                builder.setSystemProperty("add_exchange_below_partial_aggregation_over_group_id", "false");
            });

            runner.addScenario("pre_aggregate_before_groupid", builder -> {
                builder.setSystemProperty("pre_aggregate_before_grouping_sets", "true");
                builder.setSystemProperty("add_exchange_below_partial_aggregation_over_group_id", "false");
            });

            runner.addScenario("add_exchange_below_agg", builder -> {
                builder.setSystemProperty("pre_aggregate_before_grouping_sets", "false");
                builder.setSystemProperty("add_exchange_below_partial_aggregation_over_group_id", "true");
            });

            runner.runWithVerification(QUERY);
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        new BenchmarkGroupingSetsPreAggregation().benchmark();
    }
}
