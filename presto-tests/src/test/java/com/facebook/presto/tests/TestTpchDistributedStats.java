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
import com.facebook.presto.tpch.ColumnNaming;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.statistics.Metric.OUTPUT_ROW_COUNT;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.absoluteError;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.defaultTolerance;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.noError;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.relativeError;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunnerWithoutCatalogs;
import static java.util.Collections.emptyMap;

public class TestTpchDistributedStats
{
    private final StatisticsAssertion statisticsAssertion;

    public TestTpchDistributedStats()
            throws Exception
    {
        DistributedQueryRunner runner = createQueryRunnerWithoutCatalogs(emptyMap(), emptyMap());
        runner.createCatalog(
                "tpch",
                "tpch",
                ImmutableMap.of("tpch.column-naming", ColumnNaming.STANDARD.name()));
        statisticsAssertion = new StatisticsAssertion(runner);
    }

    @Test
    void testTableScanStats()
    {
        TpchTable.getTables()
                .forEach(table -> statisticsAssertion.check("SELECT * FROM " + table.getTableName())
                        // TODO use noError
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test
    void testFilter()
    {
        String query = "SELECT * FROM lineitem" +
                " WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY";
        statisticsAssertion.check(query)
                .estimate(OUTPUT_ROW_COUNT, relativeError(Range.closed(-.55, -.45)));
    }

    @Test
    void testJoin()
    {
        statisticsAssertion.check("SELECT * FROM  part, partsupp WHERE p_partkey = ps_partkey")
                .estimate(OUTPUT_ROW_COUNT, relativeError(Range.closed(.95, 1.05)));
    }

    @Test
    void testSetOperations()
    {
        statisticsAssertion.check("SELECT * FROM nation UNION SELECT * FROM nation")
                .noEstimate(OUTPUT_ROW_COUNT);

        statisticsAssertion.check("SELECT * FROM nation INTERSECT SELECT * FROM nation")
                .noEstimate(OUTPUT_ROW_COUNT);

        statisticsAssertion.check("SELECT * FROM nation EXCEPT SELECT * FROM nation")
                .noEstimate(OUTPUT_ROW_COUNT);
    }

    @Test
    void testEnforceSingleRow()
    {
        statisticsAssertion.check("SELECT (SELECT n_regionkey FROM nation WHERE n_name = 'Germany')")
                .estimate(OUTPUT_ROW_COUNT, noError());
    }

    @Test
    void testValues()
    {
        statisticsAssertion.check("VALUES 1")
                .estimate(OUTPUT_ROW_COUNT, noError());
    }

    @Test
    void testSemiJoin()
    {
        statisticsAssertion.check("SELECT * FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region)")
                .estimate(OUTPUT_ROW_COUNT, noError());
        statisticsAssertion.check("SELECT * FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_regionkey % 3 = 0)")
                .estimate(OUTPUT_ROW_COUNT, absoluteError(Range.singleton(15.)));
    }

    @Test
    void testLimit()
    {
        statisticsAssertion.check("SELECT * FROM nation LIMIT 10")
                .estimate(OUTPUT_ROW_COUNT, noError());

        statisticsAssertion.check("SELECT * FROM nation LIMIT 30")
                .estimate(OUTPUT_ROW_COUNT, noError());
    }

    @Test
    void testGroupBy()
    {
        String query = "SELECT l_returnflag, l_linestatus FROM lineitem" +
                " GROUP BY l_returnflag, l_linestatus";
        statisticsAssertion.check(query)
                .noEstimate(OUTPUT_ROW_COUNT);
    }
}
