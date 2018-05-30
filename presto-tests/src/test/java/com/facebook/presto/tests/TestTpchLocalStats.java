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

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.absoluteError;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.defaultTolerance;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.noError;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.relativeError;
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

    @Test
    public void testInnerJoinStats()
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

        // simple equi join, different ranges
        statisticsAssertion.check("SELECT n1.n_nationkey FROM nation n1, nation n2 WHERE n1.n_nationkey + 1 = n2.n_nationkey - 1 AND n1.n_nationkey > 5 AND n2.n_nationkey < 20",
                // Join is over expressions so that predicate push down doesn't unify ranges of n_nationkey coming from n1 and n2. This, however, makes symbols
                // stats inaccurate (rules can't update them), so we don't verify them.
                checks -> checks.estimate(OUTPUT_ROW_COUNT, absoluteError(3)));

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

        // join with two keys
        statisticsAssertion.check("SELECT * FROM partsupp, lineitem WHERE ps_partkey = l_partkey AND ps_suppkey = l_suppkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(4.0))
                        .verifyExactColumnStatistics("ps_partkey")
                        .verifyExactColumnStatistics("l_partkey")
                        .verifyExactColumnStatistics("ps_suppkey")
                        .verifyExactColumnStatistics("l_suppkey")
                        .verifyExactColumnStatistics("l_orderkey"));
    }

    @Test
    public void testLeftJoinStats()
    {
        // non equi predicates
        statisticsAssertion.check("SELECT * FROM supplier left join nation on true",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("s_suppkey"));
        statisticsAssertion.check("SELECT * FROM supplier left join nation on false",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("s_suppkey"));
        // simple equi join
        statisticsAssertion.check("SELECT * FROM supplier left join nation on s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("n_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("s_suppkey", absoluteError(0.40)));
        statisticsAssertion.check("SELECT * FROM supplier left join nation on s_nationkey = n_nationkey AND n_nationkey <= 12",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("n_nationkey", relativeError(0.40))
                        .verifyColumnStatistics("s_suppkey", absoluteError(0.40)));
        statisticsAssertion.check("SELECT * FROM (SELECT * FROM supplier WHERE s_nationkey <= 12) left join nation on s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(2.0))
                        .verifyColumnStatistics("n_nationkey", absoluteError(2.0)));

        // join with two keys
        statisticsAssertion.check("SELECT * FROM partsupp left join lineitem on ps_partkey = l_partkey AND ps_suppkey = l_suppkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(4.0))
                        .verifyExactColumnStatistics("ps_partkey")
                        .verifyColumnStatistics("l_partkey", absoluteError(6.0))
                        .verifyExactColumnStatistics("ps_suppkey")
                        .verifyColumnStatistics("l_suppkey", absoluteError(6.0))
                        .verifyColumnStatistics("l_orderkey", absoluteError(6.0)));
    }

    @Test
    public void testRightJoinStats()
    {
        // non equi predicates
        statisticsAssertion.check("SELECT * FROM nation right join supplier on true",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("s_suppkey"));
        statisticsAssertion.check("SELECT * FROM nation right join supplier on false",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("s_suppkey"));
        // simple equi join
        statisticsAssertion.check("SELECT * FROM nation right join supplier on s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("n_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("s_suppkey", absoluteError(0.40)));
        statisticsAssertion.check("SELECT * FROM nation right join supplier on s_nationkey = n_nationkey AND n_nationkey <= 12",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("n_nationkey", relativeError(0.40))
                        .verifyColumnStatistics("s_suppkey", absoluteError(0.40)));
        statisticsAssertion.check("SELECT * FROM nation right JOIN (SELECT * FROM supplier WHERE s_nationkey <= 12) on s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(2.0))
                        .verifyColumnStatistics("n_nationkey", absoluteError(2.0)));

        // join with two keys
        statisticsAssertion.check("SELECT * FROM lineitem right join partsupp on ps_partkey = l_partkey AND ps_suppkey = l_suppkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(4.0))
                        .verifyExactColumnStatistics("ps_partkey")
                        .verifyColumnStatistics("l_partkey", absoluteError(6.0))
                        .verifyExactColumnStatistics("ps_suppkey")
                        .verifyColumnStatistics("l_suppkey", absoluteError(6.0))
                        .verifyColumnStatistics("l_orderkey", absoluteError(6.0)));
    }

    @Test
    public void testFullJoinStats()
    {
        // non equi predicates
        statisticsAssertion.check("SELECT * FROM supplier full join nation on true",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("s_suppkey"));
        // simple equi join
        statisticsAssertion.check("SELECT * FROM nation full join supplier on s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("n_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("s_suppkey", absoluteError(0.40)));
        statisticsAssertion.check("SELECT * FROM (SELECT * FROM nation WHERE n_nationkey <= 12) full join supplier on s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("n_nationkey", relativeError(0.40))
                        .verifyColumnStatistics("s_suppkey", absoluteError(0.40)));
        statisticsAssertion.check("SELECT * FROM nation full join (SELECT * FROM supplier WHERE s_nationkey <= 12) on s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", relativeError(0.40))
                        .verifyColumnStatistics("n_nationkey", relativeError(0.40)));

        // join with two keys
        statisticsAssertion.check("SELECT * FROM lineitem full join partsupp on ps_partkey = l_partkey AND ps_suppkey = l_suppkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(4.0))
                        .verifyColumnStatistics("ps_partkey", absoluteError(6.0))
                        .verifyColumnStatistics("l_partkey", absoluteError(6.0))
                        .verifyColumnStatistics("ps_suppkey", absoluteError(6.0))
                        .verifyColumnStatistics("l_suppkey", absoluteError(6.0))
                        .verifyColumnStatistics("l_orderkey", absoluteError(6.0)));
    }
}
