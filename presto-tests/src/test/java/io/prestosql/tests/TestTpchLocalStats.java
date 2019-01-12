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
package io.prestosql.tests;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.ColumnNaming;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.tests.statistics.StatisticsAssertion;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.SystemSessionProperties.PREFER_PARTIAL_AGGREGATION;
import static io.prestosql.plugin.tpch.TpchConnectorFactory.TPCH_COLUMN_NAMING_PROPERTY;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.tests.statistics.MetricComparisonStrategies.absoluteError;
import static io.prestosql.tests.statistics.MetricComparisonStrategies.defaultTolerance;
import static io.prestosql.tests.statistics.MetricComparisonStrategies.noError;
import static io.prestosql.tests.statistics.MetricComparisonStrategies.relativeError;
import static io.prestosql.tests.statistics.Metrics.OUTPUT_ROW_COUNT;
import static io.prestosql.tests.statistics.Metrics.distinctValuesCount;
import static io.prestosql.tests.statistics.Metrics.highValue;
import static io.prestosql.tests.statistics.Metrics.lowValue;
import static io.prestosql.tests.statistics.Metrics.nullsFraction;

public class TestTpchLocalStats
{
    private StatisticsAssertion statisticsAssertion;

    @BeforeClass
    public void setUp()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                // We are not able to calculate stats for PARTIAL aggregations
                .setSystemProperty(PREFER_PARTIAL_AGGREGATION, "false")
                .build();

        LocalQueryRunner queryRunner = new LocalQueryRunner(defaultSession);
        queryRunner.createCatalog(
                "tpch",
                new TpchConnectorFactory(1),
                ImmutableMap.of(TPCH_COLUMN_NAMING_PROPERTY, ColumnNaming.STANDARD.name()));
        statisticsAssertion = new StatisticsAssertion(queryRunner);
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
                checks -> checks.estimate(OUTPUT_ROW_COUNT, absoluteError(8)));

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
        statisticsAssertion.check("SELECT * FROM supplier LEFT JOIN nation ON true",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("s_suppkey"));
        statisticsAssertion.check("SELECT * FROM supplier LEFT JOIN nation ON false",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("s_suppkey"));
        // simple equi join
        statisticsAssertion.check("SELECT * FROM supplier LEFT JOIN nation ON s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("n_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("s_suppkey", absoluteError(0.40)));
        statisticsAssertion.check("SELECT * FROM supplier LEFT JOIN nation ON s_nationkey = n_nationkey AND n_nationkey <= 12",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("n_nationkey", relativeError(0.40))
                        .verifyColumnStatistics("s_suppkey", absoluteError(0.40)));
        statisticsAssertion.check("SELECT * FROM (SELECT * FROM supplier WHERE s_nationkey <= 12) LEFT JOIN nation ON s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(2.0))
                        .verifyColumnStatistics("n_nationkey", absoluteError(2.0)));

        // join with two keys
        statisticsAssertion.check("SELECT * FROM partsupp LEFT JOIN lineitem ON ps_partkey = l_partkey AND ps_suppkey = l_suppkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(4.0))
                        .verifyExactColumnStatistics("ps_partkey")
                        .verifyColumnStatistics("l_partkey", absoluteError(6.0))
                        .verifyExactColumnStatistics("ps_suppkey")
                        .verifyColumnStatistics("l_suppkey", absoluteError(6.0))
                        .verifyColumnStatistics("l_orderkey", absoluteError(6.0)));

        // simple non-equi join
        statisticsAssertion.check("SELECT * FROM partsupp LEFT JOIN lineitem ON ps_partkey = l_partkey AND ps_suppkey < l_suppkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(4.0))
                        .verifyExactColumnStatistics("ps_partkey")
                        .verifyColumnStatistics("l_partkey", relativeError(0.10))
                        .verifyExactColumnStatistics("ps_suppkey")
                        .verifyColumnStatistics("l_suppkey", relativeError(1.0))
                        .verifyColumnStatistics("l_orderkey", relativeError(0.10)));
    }

    @Test
    public void testRightJoinStats()
    {
        // non equi predicates
        statisticsAssertion.check("SELECT * FROM nation RIGHT JOIN supplier ON true",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("s_suppkey"));
        statisticsAssertion.check("SELECT * FROM nation RIGHT JOIN supplier ON false",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("s_suppkey"));
        // simple equi join
        statisticsAssertion.check("SELECT * FROM nation RIGHT JOIN supplier ON s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("n_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("s_suppkey", absoluteError(0.40)));
        statisticsAssertion.check("SELECT * FROM nation RIGHT JOIN supplier ON s_nationkey = n_nationkey AND n_nationkey <= 12",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("n_nationkey", relativeError(0.40))
                        .verifyColumnStatistics("s_suppkey", absoluteError(0.40)));
        statisticsAssertion.check("SELECT * FROM nation RIGHT JOIN (SELECT * FROM supplier WHERE s_nationkey <= 12) ON s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(2.0))
                        .verifyColumnStatistics("n_nationkey", absoluteError(2.0)));

        // join with two keys
        statisticsAssertion.check("SELECT * FROM lineitem RIGHT JOIN partsupp ON ps_partkey = l_partkey AND ps_suppkey = l_suppkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(4.0))
                        .verifyExactColumnStatistics("ps_partkey")
                        .verifyColumnStatistics("l_partkey", absoluteError(6.0))
                        .verifyExactColumnStatistics("ps_suppkey")
                        .verifyColumnStatistics("l_suppkey", absoluteError(6.0))
                        .verifyColumnStatistics("l_orderkey", absoluteError(6.0)));

        // simple non-equi join
        statisticsAssertion.check("SELECT * FROM lineitem RIGHT JOIN partsupp ON ps_partkey = l_partkey AND ps_suppkey < l_suppkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(4.0))
                        .verifyExactColumnStatistics("ps_partkey")
                        .verifyColumnStatistics("l_partkey", relativeError(0.10))
                        .verifyExactColumnStatistics("ps_suppkey")
                        .verifyColumnStatistics("l_suppkey", relativeError(1.0))
                        .verifyColumnStatistics("l_orderkey", relativeError(0.10)));
    }

    @Test
    public void testFullJoinStats()
    {
        // non equi predicates
        statisticsAssertion.check("SELECT * FROM supplier FULL JOIN nation ON true",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("s_nationkey")
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("s_suppkey"));
        // simple equi join
        statisticsAssertion.check("SELECT * FROM nation FULL JOIN supplier ON s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("n_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("s_suppkey", absoluteError(0.40)));
        statisticsAssertion.check("SELECT * FROM (SELECT * FROM nation WHERE n_nationkey <= 12) FULL JOIN supplier ON s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", absoluteError(0.40))
                        .verifyColumnStatistics("n_nationkey", relativeError(0.40))
                        .verifyColumnStatistics("s_suppkey", absoluteError(0.40)));
        statisticsAssertion.check("SELECT * FROM nation FULL JOIN (SELECT * FROM supplier WHERE s_nationkey <= 12) ON s_nationkey = n_nationkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.70))
                        .verifyColumnStatistics("s_nationkey", relativeError(0.40))
                        .verifyColumnStatistics("n_nationkey", relativeError(0.40)));

        // join with two keys
        statisticsAssertion.check("SELECT * FROM lineitem FULL JOIN partsupp ON ps_partkey = l_partkey AND ps_suppkey = l_suppkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(4.0))
                        .verifyColumnStatistics("ps_partkey", absoluteError(6.0))
                        .verifyColumnStatistics("l_partkey", absoluteError(6.0))
                        .verifyColumnStatistics("ps_suppkey", absoluteError(6.0))
                        .verifyColumnStatistics("l_suppkey", absoluteError(6.0))
                        .verifyColumnStatistics("l_orderkey", absoluteError(6.0)));

        // simple non-equi join
        statisticsAssertion.check("SELECT * FROM lineitem FULL JOIN partsupp ON ps_partkey = l_partkey AND ps_suppkey < l_suppkey",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(4.0))
                        .verifyColumnStatistics("ps_partkey", relativeError(0.10))
                        .verifyColumnStatistics("l_partkey", relativeError(0.10))
                        .verifyColumnStatistics("ps_suppkey", relativeError(0.10))
                        .verifyColumnStatistics("l_suppkey", relativeError(1.0))
                        .verifyColumnStatistics("l_orderkey", relativeError(0.10)));
    }

    @Test
    public void testAggregation()
    {
        statisticsAssertion.check("SELECT count() AS count FROM nation",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyNoColumnStatistics("count"));

        statisticsAssertion.check("SELECT n_name, count() AS count FROM nation GROUP BY n_name",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyNoColumnStatistics("count")
                        .verifyExactColumnStatistics("n_name"));

        statisticsAssertion.check("SELECT n_name, count() AS count FROM nation, region GROUP BY n_name",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyNoColumnStatistics("count")
                        .verifyExactColumnStatistics("n_name"));
    }

    @Test
    public void testUnion()
    {
        statisticsAssertion.check("SELECT * FROM nation UNION SELECT * FROM nation",
                // real count is 25, estimation cannot know all rows are duplicate.
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(1, 1))
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("n_regionkey"));

        statisticsAssertion.check("SELECT * FROM nation UNION ALL SELECT * FROM nation",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, noError())
                        .verifyExactColumnStatistics("n_nationkey")
                        .verifyExactColumnStatistics("n_regionkey"));

        statisticsAssertion.check("SELECT * FROM orders WHERE o_custkey < 755 OR o_orderstatus = '0' UNION SELECT * FROM orders WHERE o_custkey > 755 OR o_orderstatus = 'F'",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(.3, .35))
                        .estimate(distinctValuesCount("o_orderkey"), relativeError(-.4, -.3))
                        .estimate(nullsFraction("o_orderkey"), relativeError(.3, .35))
                        .estimate(lowValue("o_orderkey"), noError())
                        .estimate(highValue("o_orderkey"), noError())
                        .estimate(distinctValuesCount("o_custkey"), relativeError(0.5))
                        .estimate(nullsFraction("o_custkey"), relativeError(.45, .55))
                        .estimate(lowValue("o_custkey"), noError())
                        .estimate(highValue("o_custkey"), noError())
                        .estimate(distinctValuesCount("o_orderstatus"), relativeError(0.5))
                        .estimate(nullsFraction("o_orderstatus"), noError())
                        .estimate(lowValue("o_orderstatus"), noError())
                        .estimate(highValue("o_orderstatus"), noError()));

        statisticsAssertion.check("SELECT * FROM orders WHERE o_custkey < 755 OR o_orderstatus = '0' UNION ALL SELECT * FROM orders WHERE o_custkey > 755 OR o_orderstatus = 'F'",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .estimate(distinctValuesCount("o_orderkey"), relativeError(-.4, -.3))
                        .estimate(nullsFraction("o_orderkey"), relativeError(.3, .35))
                        .estimate(lowValue("o_orderkey"), noError())
                        .estimate(highValue("o_orderkey"), noError())
                        .estimate(distinctValuesCount("o_custkey"), relativeError(0.5))
                        .estimate(nullsFraction("o_custkey"), relativeError(.45, .55))
                        .estimate(lowValue("o_custkey"), noError())
                        .estimate(highValue("o_custkey"), noError())
                        .estimate(distinctValuesCount("o_orderstatus"), relativeError(0.5))
                        .estimate(nullsFraction("o_orderstatus"), noError())
                        .estimate(lowValue("o_orderstatus"), noError())
                        .estimate(highValue("o_orderstatus"), noError()));

        statisticsAssertion.check("SELECT * FROM orders WHERE o_custkey < 900 UNION SELECT * FROM orders WHERE o_custkey > 600",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(.15, .25))
                        .estimate(distinctValuesCount("o_orderkey"), relativeError(-.4, -.3))
                        .estimate(nullsFraction("o_orderkey"), relativeError(.15, .25))
                        .estimate(lowValue("o_orderkey"), noError())
                        .estimate(highValue("o_orderkey"), noError())
                        .estimate(distinctValuesCount("o_custkey"), relativeError(-.4, -.3))
                        .estimate(nullsFraction("o_custkey"), relativeError(.15, .25))
                        .estimate(lowValue("o_custkey"), noError())
                        .estimate(highValue("o_custkey"), noError()));

        statisticsAssertion.check("SELECT * FROM orders WHERE o_custkey < 900 UNION ALL SELECT * FROM orders WHERE o_custkey > 600",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .estimate(distinctValuesCount("o_orderkey"), relativeError(-.4, -.3))
                        .estimate(nullsFraction("o_orderkey"), relativeError(-.4, -.3))
                        .estimate(lowValue("o_orderkey"), noError())
                        .estimate(highValue("o_orderkey"), noError())
                        .estimate(distinctValuesCount("o_custkey"), relativeError(-.4, -.3))
                        .estimate(nullsFraction("o_custkey"), relativeError(.15, .25))
                        .estimate(lowValue("o_custkey"), noError())
                        .estimate(highValue("o_custkey"), noError()));
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
    public void testInSubquery()
    {
        statisticsAssertion.check("select * from lineitem where l_orderkey in (select o_orderkey from orders where o_orderdate >= DATE '1993-10-01')",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test
    public void testNotInSubquery()
    {
        statisticsAssertion.check("select * from lineitem where l_orderkey not in (select o_orderkey from orders where o_orderdate >= DATE '1993-10-01')",
                // we allow overestimating here. That is because safety heuristic for antijoin which enforces that not more that 50%
                // of values are filtered out.
                checks -> checks.estimate(OUTPUT_ROW_COUNT, relativeError(0.0, 1.0)));
    }

    @Test
    public void testCorrelatedSubquery()
    {
        statisticsAssertion.check("SELECT (SELECT count(*) FROM nation n1 WHERE n1.n_nationkey = n2.n_nationkey AND n1.n_regionkey > n2.n_regionkey) FROM nation n2",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(0.5)));
    }
}
