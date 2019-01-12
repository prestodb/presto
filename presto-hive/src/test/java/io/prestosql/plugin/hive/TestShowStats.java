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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.plugin.hive.HiveQueryRunner.createQueryRunner;

public class TestShowStats
        extends AbstractTestQueryFramework
{
    public TestShowStats()
    {
        super(() -> createQueryRunner(ImmutableList.of()));
    }

    @BeforeClass
    public void setUp()
    {
        assertUpdate("CREATE TABLE nation_partitioned(nationkey BIGINT, name VARCHAR, comment VARCHAR, regionkey BIGINT) WITH (partitioned_by = ARRAY['regionkey'])");
        assertUpdate("INSERT INTO nation_partitioned SELECT nationkey, name, comment, regionkey from tpch.tiny.nation", 25);
        assertUpdate("CREATE TABLE region(regionkey BIGINT, name VARCHAR, comment VARCHAR)");
        assertUpdate("CREATE TABLE orders(orderkey BIGINT, custkey BIGINT, totalprice DOUBLE, orderdate  DATE, " +
                "orderpriority VARCHAR, clerk VARCHAR, shippriority VARCHAR, comment VARCHAR, orderstatus VARCHAR)");
    }

    @Test
    public void testShowStats()
    {
        assertQuery("SHOW STATS FOR nation_partitioned",
                "SELECT * FROM (VALUES " +
                        "   ('regionkey', null, 5.0, 0.0, null, 0, 4), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 0, 24), " +
                        "   ('name', 177.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 1857.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 25.0, null, null))");

        assertQuery("SHOW STATS FOR (SELECT * FROM nation_partitioned)",
                "SELECT * FROM (VALUES " +
                        "   ('regionkey', null, 5.0, 0.0, null, 0, 4), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 0, 24), " +
                        "   ('name', 177.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 1857.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 25.0, null, null))");

        assertQuery("SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey IS NOT NULL)",
                "SELECT * FROM (VALUES " +
                        "   ('regionkey', null, 5.0, 0.0, null, 0, 4), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 0, 24), " +
                        "   ('name', 177.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 1857.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 25.0, null, null))");

        assertQuery("SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey IS NULL)",
                "SELECT * FROM (VALUES " +
                        "   ('regionkey', null, 0.0, 0.0, null, null, null), " +
                        "   ('nationkey', null, 0.0, 0.0, null, null, null), " +
                        "   ('name', 0.0, 0.0, 0.0, null, null, null), " +
                        "   ('comment', 0.0, 0.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 0.0, null, null))");

        assertQuery("SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey = 1)",
                "SELECT * FROM (VALUES " +
                        "   ('regionkey', null, 1.0, 0.0, null, 1, 1), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 1, 24), " +
                        "   ('name', 38.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 500.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 5.0, null, null))");

        assertQuery("SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey IN (1, 3))",
                "SELECT * FROM (VALUES " +
                        "   ('regionkey', null, 2.0, 0.0, null, 1, 3), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 1, 24), " +
                        "   ('name', 78.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 847.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 10.0, null, null))");

        assertQuery("SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey BETWEEN 1 AND 1 + 2)",
                "SELECT * FROM (VALUES " +
                        "   ('regionkey', null, 3.0, 0.0, null, 1, 3), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 1, 24), " +
                        "   ('name', 109.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 1199.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 15.0, null, null))");

        assertQuery("SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey > 3)",
                "SELECT * FROM (VALUES " +
                        "   ('regionkey', null, 1.0, 0.0, null, 4, 4), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 4, 20), " +
                        "   ('name', 31.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 348.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 5.0, null, null))");

        assertQuery("SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey < 1)",
                "SELECT * FROM (VALUES " +
                        "   ('regionkey', null, 1.0, 0.0, null, 0, 0), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 0, 16), " +
                        "   ('name', 37.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 310.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 5.0, null, null))");

        assertQuery("SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey > 0 and regionkey < 4)",
                "SELECT * FROM (VALUES " +
                        "   ('regionkey', null, 3.0, 0.0, null, 1, 3), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 1, 24), " +
                        "   ('name', 109.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 1199.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 15.0, null, null))");

        assertQuery("SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey > 10 or regionkey < 0)",
                "SELECT * FROM (VALUES " +
                        "   ('regionkey', null, 0.0, 0.0, null, null, null), " +
                        "   ('nationkey', null, 0.0, 0.0, null, null, null), " +
                        "   ('name', 0.0, 0.0, 0.0, null, null, null), " +
                        "   ('comment', 0.0, 0.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 0.0, null, null))");
    }

    @Test
    public void testShowStatsWithoutFromFails()
    {
        assertQueryFails("SHOW STATS FOR (SELECT 1)", ".*There must be exactly one table in query passed to SHOW STATS SELECT clause");
    }

    @Test
    public void testShowStatsWithMultipleFromFails()
    {
        assertQueryFails("SHOW STATS FOR (SELECT * FROM nation_partitioned, region)", ".*There must be exactly one table in query passed to SHOW STATS SELECT clause");
    }

    @Test
    public void testShowStatsWithGroupByFails()
    {
        assertQueryFails("SHOW STATS FOR (SELECT avg(totalprice) FROM orders GROUP BY orderkey)", ".*GROUP BY is not supported in SHOW STATS SELECT clause");
    }

    @Test
    public void testShowStatsWithHavingFails()
    {
        assertQueryFails("SHOW STATS FOR (SELECT count(nationkey) FROM nation_partitioned GROUP BY regionkey HAVING regionkey > 0)", ".*HAVING is not supported in SHOW STATS SELECT clause");
    }

    @Test
    public void testShowStatsSelectNonStarFails()
    {
        assertQueryFails("SHOW STATS FOR (SELECT orderkey FROM orders)", ".*Only SELECT \\* is supported in SHOW STATS SELECT clause");
        assertQueryFails("SHOW STATS FOR (SELECT orderkey, custkey FROM orders)", ".*Only SELECT \\* is supported in SHOW STATS SELECT clause");
        assertQueryFails("SHOW STATS FOR (SELECT *, * FROM orders)", ".*Only SELECT \\* is supported in SHOW STATS SELECT clause");
    }

    @Test
    public void testShowStatsWithSelectDistinctFails()
    {
        assertQueryFails("SHOW STATS FOR (SELECT DISTINCT * FROM orders)", ".*DISTINCT is not supported by SHOW STATS SELECT clause");
    }

    @Test
    public void testShowStatsWithSelectFunctionCallFails()
    {
        assertQueryFails("SHOW STATS FOR (SELECT sin(orderkey) FROM orders)", ".*Only SELECT \\* is supported in SHOW STATS SELECT clause");
        assertQueryFails("SHOW STATS FOR (SELECT count(*) FROM orders)", ".*Only SELECT \\* is supported in SHOW STATS SELECT clause");
    }

    @Test
    public void testShowStatsWithNonPushDownFilterFails()
    {
        assertQueryFails("SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey + 100 < 200)", ".*Only predicates that can be pushed down are supported in the SHOW STATS WHERE clause");
        assertQueryFails("SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey > 0 and nationkey > 0)", ".*Only predicates that can be pushed down are supported in the SHOW STATS WHERE clause");
        assertQueryFails("SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE nationkey = 1 and name is not null)", ".*Only predicates that can be pushed down are supported in the SHOW STATS WHERE clause");
        assertQueryFails("SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE sin(regionkey) > 0)", ".*Only predicates that can be pushed down are supported in the SHOW STATS WHERE clause");
    }
}
