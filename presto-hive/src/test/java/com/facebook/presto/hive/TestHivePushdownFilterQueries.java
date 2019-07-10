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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static io.airlift.tpch.TpchTable.getTables;

public class TestHivePushdownFilterQueries
        extends AbstractTestQueryFramework
{
    private static final String WITH_LINEITEM_EX = "WITH lineitem_ex AS (" +
            "SELECT linenumber, orderkey, " +
            "   CASE WHEN linenumber % 7 = 0 THEN null ELSE shipmode = 'AIR' END AS ship_by_air, " +
            "   CASE WHEN linenumber % 5 = 0 THEN null ELSE returnflag = 'R' END AS is_returned " +
            "FROM lineitem)";

    protected TestHivePushdownFilterQueries()
    {
        super(TestHivePushdownFilterQueries::createQueryRunner);
    }

    private static QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.createQueryRunner(getTables(),
                ImmutableMap.of(),
                "sql-standard",
                ImmutableMap.of("hive.pushdown-filter-enabled", "true"),
                Optional.empty());

        queryRunner.execute(noPushdownFilter(queryRunner.getDefaultSession()),
                "CREATE TABLE lineitem_ex (linenumber, orderkey, ship_by_air, is_returned) AS " +
                        "SELECT linenumber, " +
                        "   orderkey, " +
                        "   IF (linenumber % 7 = 0, null, shipmode = 'AIR') AS ship_by_air, " +
                        "   IF (linenumber % 5 = 0, null, returnflag = 'R') AS is_returned " +
                        "FROM lineitem");

        return queryRunner;
    }

    @Test
    public void testBooleans()
    {
        // Single boolean column
        assertQueryUsingH2Cte("SELECT is_returned FROM lineitem_ex");

        assertQueryUsingH2Cte("SELECT is_returned FROM lineitem_ex WHERE is_returned = true");

        assertQueryUsingH2Cte("SELECT count(*) FROM lineitem_ex WHERE is_returned is not null");

        assertQueryUsingH2Cte("SELECT count(*) FROM lineitem_ex WHERE is_returned = false");

        // Two boolean columns
        assertQueryUsingH2Cte("SELECT ship_by_air, is_returned FROM lineitem_ex");

        assertQueryUsingH2Cte("SELECT ship_by_air, is_returned FROM lineitem_ex WHERE ship_by_air = true");

        assertQueryUsingH2Cte("SELECT ship_by_air, is_returned FROM lineitem_ex WHERE ship_by_air = true AND is_returned = false");

        assertQueryUsingH2Cte("SELECT COUNT(*) FROM lineitem_ex WHERE ship_by_air is null");

        assertQueryUsingH2Cte("SELECT COUNT(*) FROM lineitem_ex WHERE ship_by_air is not null AND is_returned = true");
    }

    @Test
    public void testNumeric()
    {
        assertQuery("SELECT orderkey, custkey, orderdate, shippriority FROM orders");

        assertQuery("SELECT count(*) FROM orders WHERE orderkey BETWEEN 100 AND 1000 AND custkey BETWEEN 500 AND 800");

        assertQuery("SELECT custkey, orderdate, shippriority FROM orders WHERE orderkey BETWEEN 100 AND 1000 AND custkey BETWEEN 500 AND 800");

        assertQuery("SELECT orderkey, orderdate FROM orders WHERE orderdate BETWEEN date '1994-01-01' AND date '1997-03-30'");

        assertQueryUsingH2Cte("SELECT count(*) FROM lineitem_ex WHERE orderkey < 30000 AND ship_by_air = true");

        assertQueryUsingH2Cte("SELECT linenumber, orderkey, ship_by_air, is_returned FROM lineitem_ex WHERE orderkey < 30000 AND ship_by_air = true");

        assertQueryUsingH2Cte("SELECT linenumber, ship_by_air, is_returned FROM lineitem_ex WHERE orderkey < 30000 AND ship_by_air = true");
    }

    @Test
    public void testFilterFunctions()
    {
        // filter function on orderkey; orderkey is projected out
        assertQuery("SELECT custkey, orderkey, orderdate FROM orders WHERE orderkey % 5 = 0");

        // filter function on orderkey; orderkey is not projected out
        assertQuery("SELECT custkey, orderdate FROM orders WHERE orderkey % 5 = 0");

        // filter function and range predicate on orderkey
        assertQuery("SELECT custkey, orderdate FROM orders WHERE orderkey % 5 = 0 AND orderkey > 100");

        // multiple filter functions
        assertQuery("SELECT custkey, orderdate FROM orders WHERE orderkey % 5 = 0 AND custkey % 7 = 0");

        // multi-column filter functions
        assertQuery("SELECT custkey, orderdate FROM orders WHERE (orderkey + custkey) % 5 = 0");

        // filter function with an error
        assertQueryFails("SELECT custkey, orderdate FROM orders WHERE array[1, 2, 3][orderkey % 5 + custkey % 7 + 1] > 0", "Array subscript out of bounds");

        // filter function with "recoverable" error
        assertQuery("SELECT custkey, orderdate FROM orders WHERE array[1, 2, 3][orderkey % 5 + custkey %7 + 1] > 0 AND orderkey % 5 = 1 AND custkey % 7 = 0", "SELECT custkey, orderdate FROM orders WHERE orderkey % 5 = 1 AND custkey % 7 = 0");

        // filter function on numeric and boolean columnss
        assertQuery("SELECT linenumber FROM lineitem_ex WHERE if(is_returned, linenumber, orderkey) % 5 = 0", WITH_LINEITEM_EX + "SELECT linenumber FROM lineitem_ex WHERE CASE WHEN is_returned THEN linenumber ELSE orderkey END % 5 = 0");
    }

    @Test
    public void testPartitionColumns()
    {
        assertUpdate("CREATE TABLE test_partition_columns WITH (partitioned_by = ARRAY['p']) AS\n" +
                "SELECT * FROM (VALUES (1, 'abc'), (2, 'abc')) as t(x, p)", 2);

        assertQuery("SELECT * FROM test_partition_columns", "SELECT 1, 'abc' UNION ALL SELECT 2, 'abc'");

        assertQuery("SELECT * FROM test_partition_columns WHERE p = 'abc'", "SELECT 1, 'abc' UNION ALL SELECT 2, 'abc'");

        assertQuery("SELECT * FROM test_partition_columns WHERE p LIKE 'a%'", "SELECT 1, 'abc' UNION ALL SELECT 2, 'abc'");

        assertQuery("SELECT * FROM test_partition_columns WHERE substr(p, x, 1) = 'a'", "SELECT 1, 'abc'");

        assertQueryReturnsEmptyResult("SELECT * FROM test_partition_columns WHERE p = 'xxx'");

        assertUpdate("DROP TABLE test_partition_columns");
    }

    @Test
    public void testBucketColumn()
    {
        getQueryRunner().execute("CREATE TABLE test_bucket_column WITH (bucketed_by = ARRAY['orderkey'], bucket_count = 11) AS " +
                "SELECT linenumber, orderkey FROM lineitem");

        assertQuery("SELECT linenumber, \"$bucket\" FROM test_bucket_column", "SELECT linenumber, orderkey % 11 FROM lineitem");
        assertQuery("SELECT linenumber, \"$bucket\" FROM test_bucket_column WHERE (\"$bucket\" + linenumber) % 2 = 1", "SELECT linenumber, orderkey % 11 FROM lineitem WHERE (orderkey % 11 + linenumber) % 2 = 1");

        assertUpdate("DROP TABLE test_bucket_column");
    }

    @Test
    public void testPathColumn()
    {
        Session session = getQueryRunner().getDefaultSession();
        assertQuerySucceeds(session, "SELECT linenumber, \"$path\" FROM lineitem");
        assertQuerySucceeds(session, "SELECT linenumber, \"$path\" FROM lineitem WHERE length(\"$path\") % 2 = linenumber % 2");
    }

    private void assertQueryUsingH2Cte(String query)
    {
        assertQuery(query, WITH_LINEITEM_EX + query);
    }

    private static Session noPushdownFilter(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "false")
                .build();
    }
}
