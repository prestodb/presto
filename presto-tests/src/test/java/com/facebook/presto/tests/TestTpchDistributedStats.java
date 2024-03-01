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
package com.facebook.presto.tests;

import com.facebook.presto.tests.statistics.StatisticsAssertion;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.facebook.presto.tpch.ColumnNaming;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PREFER_PARTIAL_AGGREGATION;
import static com.facebook.presto.SystemSessionProperties.PRINT_STATS_FOR_NON_JOIN_QUERY;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.absoluteError;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.defaultTolerance;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.noError;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.relativeError;
import static com.facebook.presto.tests.statistics.Metrics.OUTPUT_ROW_COUNT;
import static com.facebook.presto.tests.statistics.Metrics.distinctValuesCount;
import static com.facebook.presto.tpch.TpchConnectorFactory.TPCH_COLUMN_NAMING_PROPERTY;

public class TestTpchDistributedStats
{
    private StatisticsAssertion statisticsAssertion;

    @BeforeClass
    public void setup()
            throws Exception
    {
        DistributedQueryRunner runner = TpchQueryRunnerBuilder.builder()
                // We are not able to calculate stats for PARTIAL aggregations
                .amendSession(builder -> builder.setSystemProperty(PREFER_PARTIAL_AGGREGATION, "false"))
                .amendSession(builder -> builder.setSystemProperty(PRINT_STATS_FOR_NON_JOIN_QUERY, "true"))
                .buildWithoutCatalogs();
        runner.createCatalog(
                "tpch",
                "tpch",
                ImmutableMap.of(TPCH_COLUMN_NAMING_PROPERTY, ColumnNaming.STANDARD.name()));
        statisticsAssertion = new StatisticsAssertion(runner);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        statisticsAssertion.close();
        statisticsAssertion = null;
    }

    @Test
    public void testTableScanStats()
    {
        TpchTable.getTables()
                .forEach(table -> statisticsAssertion.check("SELECT * FROM " + table.getTableName(),
                        checks -> checks.estimate(OUTPUT_ROW_COUNT, noError())));
    }

    @Test
    public void testFilter()
    {
        statisticsAssertion.check("SELECT * FROM lineitem WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test
    public void testJoin()
    {
        statisticsAssertion.check("SELECT * FROM  part, partsupp WHERE p_partkey = ps_partkey",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test
    public void testUnion()
    {
        statisticsAssertion.check("SELECT * FROM nation UNION SELECT * FROM nation",
                // real count is 25, estimation cannot know all rows are duplicate.
                checks -> checks.estimate(OUTPUT_ROW_COUNT, relativeError(1, 1)));

        statisticsAssertion.check("SELECT * FROM nation UNION ALL SELECT * FROM nation",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, noError()));

        statisticsAssertion.check("SELECT * FROM orders WHERE o_custkey < 755 OR o_orderstatus = '0' UNION SELECT * FROM orders WHERE o_custkey > 755 OR o_orderstatus = 'F'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, relativeError(.3, .35)));

        statisticsAssertion.check("SELECT * FROM orders WHERE o_custkey < 755 OR o_orderstatus = '0' UNION ALL SELECT * FROM orders WHERE o_custkey > 755 OR o_orderstatus = 'F'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));

        statisticsAssertion.check("SELECT * FROM orders WHERE o_custkey < 900 UNION SELECT * FROM orders WHERE o_custkey > 600",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, relativeError(.15, .25)));

        statisticsAssertion.check("SELECT * FROM orders WHERE o_custkey < 900 UNION ALL SELECT * FROM orders WHERE o_custkey > 600",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test
    public void testIntersect()
    {
        statisticsAssertion.check("SELECT * FROM nation INTERSECT SELECT * FROM nation",
                checks -> checks.noEstimate(OUTPUT_ROW_COUNT));

        statisticsAssertion.check("SELECT * FROM orders WHERE o_custkey < 900 INTERSECT SELECT * FROM orders WHERE o_custkey > 600",
                checks -> checks.noEstimate(OUTPUT_ROW_COUNT));
    }

    @Test
    public void testExcept()
    {
        statisticsAssertion.check("SELECT * FROM nation EXCEPT SELECT * FROM nation",
                checks -> checks.noEstimate(OUTPUT_ROW_COUNT));

        statisticsAssertion.check("SELECT * FROM orders WHERE o_custkey < 900 EXCEPT SELECT * FROM orders WHERE o_custkey > 600",
                checks -> checks.noEstimate(OUTPUT_ROW_COUNT));
    }

    @Test
    public void testEnforceSingleRow()
    {
        statisticsAssertion.check("SELECT (SELECT n_regionkey FROM nation WHERE n_name = 'nosuchvalue') AS sub",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, noError()));

        statisticsAssertion.check("SELECT (SELECT n_regionkey FROM nation WHERE n_name = 'GERMANY') AS sub",
                checks -> checks
                        .estimate(distinctValuesCount("sub"), noError())
                        .estimate(OUTPUT_ROW_COUNT, noError()));
    }

    @Test
    public void testValues()
    {
        statisticsAssertion.check("VALUES 1",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, noError()));
    }

    @Test
    public void testSemiJoin()
    {
        statisticsAssertion.check("SELECT * FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region)",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, noError()));

        statisticsAssertion.check("SELECT * FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_regionkey % 3 = 0)",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, absoluteError(15.)));
    }

    @Test
    public void testLimit()
    {
        statisticsAssertion.check("SELECT * FROM nation LIMIT 10",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, noError()));
    }

    @Test
    public void testGroupBy()
    {
        statisticsAssertion.check("SELECT l_returnflag, l_linestatus FROM lineitem GROUP BY l_returnflag, l_linestatus",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, absoluteError(2))); // real row count is 4
    }

    @Test
    public void testSort()
    {
        statisticsAssertion.check("SELECT * FROM nation ORDER BY n_nationkey",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, noError()));
    }
}
