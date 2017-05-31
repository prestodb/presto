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
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.SystemSessionProperties.USE_NEW_STATS_CALCULATOR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.defaultTolerance;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.relativeError;
import static com.facebook.presto.tests.statistics.Metrics.OUTPUT_ROW_COUNT;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestTpchLocalStats
{
    private final StatisticsAssertion statisticsAssertion;

    public TestTpchLocalStats()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .setSystemProperty(USE_NEW_STATS_CALCULATOR, "true")
                .build();

        LocalQueryRunner runner = new LocalQueryRunner(defaultSession);
        runner.createCatalog("tpch", new TpchConnectorFactory(1),
                ImmutableMap.of("tpch.column-naming", ColumnNaming.STANDARD.name()
                ));
        statisticsAssertion = new StatisticsAssertion(runner);
    }

    @Test
    void testTableScanStats()
    {
        statisticsAssertion.check("SELECT * FROM nation",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("n_regionkey")
                        .verifyExactColumnStatistics("n_name")
        );
    }

    @Test
    void testInnerJoinStats()
    {
        // cross join
        statisticsAssertion.check("SELECT * FROM supplier, nation",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("s_suppkey"));
        statisticsAssertion.check("SELECT * FROM supplier, nation WHERE n_nationkey <= 12",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyColumnStatistics("n_nationkey", relativeError(0.10))
                        .verifyExactColumnStatistics("s_suppkey"));

        // simple equi joins
        statisticsAssertion.check("SELECT * FROM supplier, nation WHERE s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("s_suppkey"));
        statisticsAssertion.check("SELECT * FROM supplier, nation WHERE s_nationkey = n_nationkey AND n_nationkey <= 12",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.15))
                        .verifyColumnStatistics("s_nationkey", relativeError(0.15))
                        .verifyColumnStatistics("n_nationkey", relativeError(0.15)));

        // two joins on different keys
        statisticsAssertion.check("SELECT * FROM nation, supplier, partsupp WHERE n_nationkey = s_nationkey AND s_suppkey = ps_suppkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("ps_partkey")
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("n_name"));
        statisticsAssertion.check("SELECT * FROM nation, supplier, partsupp WHERE n_nationkey = s_nationkey AND s_suppkey = ps_suppkey AND n_nationkey <= 12",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.15))
                        .verifyColumnStatistics("ps_partkey", relativeError(0.15))
                        .verifyColumnStatistics("n_nationkey", relativeError(0.15))
                        .verifyColumnStatistics("s_nationkey", relativeError(0.15)));
    }

    @Test
    void testLeftJoinStats()
    {
        // simple equi join
        statisticsAssertion.check("SELECT * FROM supplier left join nation on s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("s_suppkey"));
        statisticsAssertion.check("SELECT * FROM supplier left join nation on s_nationkey = n_nationkey AND n_nationkey <= 12",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyColumnStatistics("n_nationkey", relativeError(0.10))
                        .verifyExactColumnStatistics("s_suppkey"));
        statisticsAssertion.check("SELECT * FROM (SELECT * FROM supplier WHERE s_nationkey <= 12) left join nation on s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.15))
                        .verifyColumnStatistics("s_nationkey", relativeError(0.15))
                        .verifyColumnStatistics("n_nationkey", relativeError(0.10)));
    }

    @Test
    void testRightJoinStats()
    {
        // simple equi join
        statisticsAssertion.check("SELECT * FROM nation right join supplier on s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("s_suppkey"));
        statisticsAssertion.check("SELECT * FROM nation right join supplier on s_nationkey = n_nationkey AND n_nationkey <= 12",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyColumnStatistics("n_nationkey", relativeError(0.10))
                        .verifyExactColumnStatistics("s_suppkey"));
        statisticsAssertion.check("SELECT * FROM nation right JOIN (SELECT * FROM supplier WHERE s_nationkey <= 12) on s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.15))
                        .verifyColumnStatistics("s_nationkey", relativeError(0.15))
                        .verifyColumnStatistics("n_nationkey", relativeError(0.10)));
    }

    @Test
    void testFullJoinStats()
    {
        // simple equi join
        statisticsAssertion.check("SELECT * FROM nation full join supplier on s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("s_suppkey"));
        statisticsAssertion.check("SELECT * FROM (SELECT * FROM nation WHERE n_nationkey <= 12) full join supplier on s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyColumnStatistics("n_nationkey", relativeError(0.10))
                        .verifyExactColumnStatistics("s_suppkey"));
        statisticsAssertion.check("SELECT * FROM nation full join (SELECT * FROM supplier WHERE s_nationkey <= 12) on s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.15))
                        .verifyColumnStatistics("s_nationkey", relativeError(0.15))
                        .verifyColumnStatistics("n_nationkey", relativeError(0.10)));
    }
}
