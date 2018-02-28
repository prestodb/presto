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

import com.facebook.presto.Session;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.statistics.StatisticsAssertion;
import com.facebook.presto.tpch.ColumnNaming;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.ENABLE_NEW_STATS_CALCULATOR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.defaultTolerance;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.noError;
import static com.facebook.presto.tests.statistics.Metrics.OUTPUT_ROW_COUNT;
import static com.facebook.presto.tpch.TpchConnectorFactory.TPCH_COLUMN_NAMING_PROPERTY;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestTpchLocalStats
{
    private LocalQueryRunner queryRunner;
    private StatisticsAssertion statisticsAssertion;

    @BeforeClass
    public void setUp()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(ENABLE_NEW_STATS_CALCULATOR, "true")
                .build();

        queryRunner = new LocalQueryRunner(defaultSession);
        queryRunner.createCatalog(
                "tpch",
                new TpchConnectorFactory(1),
                ImmutableMap.of(TPCH_COLUMN_NAMING_PROPERTY, ColumnNaming.STANDARD.name()));
        statisticsAssertion = new StatisticsAssertion(queryRunner);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        statisticsAssertion = null;
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Test
    public void testTableScanStats()
    {
        statisticsAssertion.check("SELECT * FROM nation",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("n_regionkey")
                        .verifyExactColumnStatistics("n_name"));
    }

    @Test
    public void testDateComparisons()
    {
        statisticsAssertion.check("SELECT * FROM orders WHERE o_orderdate >= DATE '1993-10-01'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));

        statisticsAssertion.check("SELECT * FROM orders WHERE o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));

        statisticsAssertion.check("SELECT * FROM orders WHERE o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));

        statisticsAssertion.check("SELECT * FROM orders WHERE o_orderdate >= DATE '1993-10-01' OR o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));

        statisticsAssertion.check("SELECT * FROM orders WHERE NOT (o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH)",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test
    public void testLimit()
    {
        // TODO merge with TestTpchDistributedStats.testLimit once that class tests new calculator

        statisticsAssertion.check("SELECT * FROM nation LIMIT 10",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, noError()));
    }

    @Test
    public void testEnforceSingleRow()
    {
        // TODO merge with TestTpchDistributedStats.testEnforceSingleRow once that class tests new calculator

        statisticsAssertion.check("SELECT (SELECT n_regionkey FROM nation WHERE n_name = 'GERMANY') AS sub",
                checks -> checks
                        // TODO .estimate(distinctValuesCount("sub"), defaultTolerance())
                        .estimate(OUTPUT_ROW_COUNT, noError()));
    }

    @Test
    public void testVarcharComparisons()
    {
        statisticsAssertion.check("SELECT * FROM orders WHERE o_comment = 'requests above the furiously even instructions use alw'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));

        statisticsAssertion.check("SELECT * FROM orders WHERE 'this is always ...' = '... false'",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, noError()));
    }
}
