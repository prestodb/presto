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
package com.facebook.presto.nativetests.iceberg;

import org.testng.annotations.Test;

/**
 * Query coverage over native-written V3 tables, plus native reads of V2 merge-on-read tables that
 * carry position deletes. Each query runs on native workers and on Java workers over the same data.
 * The deletes and updates are applied with Java workers (see {@link AbstractTestNativeIcebergV3Dml}).
 */
public abstract class AbstractTestNativeIcebergV3Dql
        extends AbstractNativeIcebergV3Test
{
    private static final String ORDERS_DELETED_EXPECTED = "(SELECT * FROM orders WHERE orderkey % 3 <> 0 AND custkey % 7 <> 1)";

    protected AbstractTestNativeIcebergV3Dql(String storageFormat)
    {
        super(storageFormat);
    }

    @Override
    protected void createTables()
    {
        super.createTables();
        createTpchTables("nation", "region", "customer", "orders", "lineitem");

        // Copies of orders carrying position deletes and updated rows, read in the tests below.
        createV2MergeOnReadTableAs("orders_deleted", "SELECT * FROM orders", 15000);
        assertJavaUpdate("DELETE FROM orders_deleted WHERE orderkey % 3 = 0", scalarLong("SELECT count(*) FROM orders WHERE orderkey % 3 = 0"));
        assertJavaUpdate("DELETE FROM orders_deleted WHERE custkey % 7 = 1", scalarLong("SELECT count(*) FROM orders WHERE orderkey % 3 <> 0 AND custkey % 7 = 1"));

        createV2MergeOnReadTableAs("orders_updated", "SELECT * FROM orders", 15000);
        assertJavaUpdate("UPDATE orders_updated SET totalprice = totalprice * 2, orderpriority = 'X-UPDATED' WHERE orderstatus = 'P'",
                scalarLong("SELECT count(*) FROM orders WHERE orderstatus = 'P'"));
    }

    @Test
    public void testProjectionAndFilter()
    {
        assertQuery("SELECT nationkey, name FROM nation WHERE regionkey = 1");
        assertQuery("SELECT name, nationkey * 10 AS x FROM nation");
        assertQuery("SELECT * FROM orders WHERE orderkey < 100 AND orderstatus = 'O'");
        assertQuery("SELECT nationkey FROM nation WHERE nationkey BETWEEN 5 AND 10");
        assertQuery("SELECT coalesce(nullif(name, 'FRANCE'), 'UNKNOWN') FROM nation");
        assertQuery("SELECT abs(nationkey - 12), greatest(nationkey, regionkey), least(nationkey, regionkey) FROM nation");
    }

    @Test
    public void testAggregations()
    {
        assertQuery("SELECT count(*) FROM orders");
        assertQuery("SELECT sum(totalprice), avg(totalprice), min(totalprice), max(totalprice) FROM orders");
        assertQuery("SELECT orderstatus, count(*), sum(totalprice) FROM orders GROUP BY orderstatus");
        assertQuery("SELECT orderpriority, count(*) c FROM orders GROUP BY orderpriority HAVING count(*) > 1");
        assertQuery("SELECT count(DISTINCT custkey), count(DISTINCT orderstatus), count(DISTINCT orderpriority) FROM orders");
        assertQuery("SELECT orderstatus, count(DISTINCT custkey), sum(DISTINCT custkey) FROM orders GROUP BY orderstatus");
        assertQuery("SELECT count(*) FILTER (WHERE orderstatus = 'O'), count(*) FILTER (WHERE orderstatus = 'F'), count(*) FROM orders");
        assertQuery("SELECT sum(quantity), avg(discount), min(extendedprice), max(extendedprice) FROM lineitem");
        // Approximate aggregates may differ between engines: only check that they run natively.
        assertQuerySucceeds("SELECT approx_distinct(custkey), stddev(totalprice), variance(totalprice), approx_percentile(totalprice, 0.5) FROM orders");
    }

    @Test
    public void testGroupingSets()
    {
        assertQuery("SELECT orderstatus, orderpriority, count(*) FROM orders GROUP BY GROUPING SETS ((orderstatus), (orderpriority), ())");
        assertQuery("SELECT orderstatus, count(*) FROM orders GROUP BY ROLLUP (orderstatus)");
        assertQuery("SELECT orderstatus, orderpriority, count(*) FROM orders GROUP BY CUBE (orderstatus, orderpriority)");
    }

    @Test
    public void testJoins()
    {
        assertQuery("SELECT n.name, r.name FROM nation n JOIN region r ON n.regionkey = r.regionkey");
        assertQuery("SELECT n.name, r.name FROM nation n LEFT JOIN region r ON n.regionkey = r.regionkey");
        assertQuery("SELECT n.name, r.name FROM nation n RIGHT JOIN region r ON n.regionkey = r.regionkey");
        assertQuery("SELECT n.name, r.name FROM nation n FULL OUTER JOIN region r ON n.regionkey = r.regionkey");
        assertQuery("SELECT c.name, n.name, r.name FROM customer c JOIN nation n ON c.nationkey = n.nationkey "
                + "JOIN region r ON n.regionkey = r.regionkey WHERE c.custkey < 20");
        assertQuery("SELECT r.name, sum(o.totalprice) FROM orders o JOIN customer c ON o.custkey = c.custkey "
                + "JOIN nation n ON c.nationkey = n.nationkey JOIN region r ON n.regionkey = r.regionkey GROUP BY r.name");
        assertQuery("SELECT count(*) FROM nation CROSS JOIN region");
        assertQuery("SELECT count(*) FROM nation n1 JOIN nation n2 ON n1.nationkey < n2.nationkey");
    }

    @Test
    public void testSemiAntiJoinsAndSubqueries()
    {
        assertQuery("SELECT count(*) FROM orders WHERE custkey IN (SELECT custkey FROM customer WHERE nationkey = 1)");
        assertQuery("SELECT count(*) FROM orders WHERE custkey NOT IN (SELECT custkey FROM customer WHERE nationkey = 1)");
        assertQuery("SELECT count(*) FROM customer c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.custkey = c.custkey)");
        assertQuery("SELECT name FROM customer c WHERE acctbal > (SELECT avg(acctbal) FROM customer c2 WHERE c2.nationkey = c.nationkey)");
        assertQuery("WITH high AS (SELECT custkey FROM orders WHERE totalprice > 100000) SELECT count(*) FROM high");
    }

    @Test
    public void testOrderByLimitDistinctSetOperations()
    {
        assertQueryOrdered("SELECT nationkey FROM nation ORDER BY nationkey LIMIT 5");
        assertQueryOrdered("SELECT orderkey FROM orders ORDER BY orderkey DESC LIMIT 10");
        assertQueryOrdered("SELECT nationkey FROM nation ORDER BY nationkey OFFSET 20 ROWS");
        assertQuery("SELECT DISTINCT orderstatus FROM orders");
        assertQuery("SELECT nationkey FROM nation WHERE regionkey = 0 UNION ALL SELECT nationkey FROM nation WHERE regionkey = 1");
        assertQuery("SELECT regionkey FROM nation UNION SELECT regionkey FROM region");
        assertQuery("SELECT regionkey FROM nation INTERSECT SELECT regionkey FROM region WHERE regionkey < 3");
        assertQuery("SELECT regionkey FROM nation EXCEPT SELECT regionkey FROM region WHERE regionkey < 3");
    }

    @Test
    public void testWindowFunctions()
    {
        assertQuery("SELECT name, regionkey, row_number() OVER (PARTITION BY regionkey ORDER BY name) FROM nation");
        assertQuery("SELECT orderstatus, orderkey, rank() OVER (PARTITION BY orderstatus ORDER BY totalprice DESC, orderkey) FROM orders");
        assertQuery("SELECT custkey, round(sum(totalprice) OVER (PARTITION BY custkey), 2) FROM orders");
    }

    @Test
    public void testScalarFunctions()
    {
        assertQuery("SELECT upper(name), lower(name), length(name), substr(name, 1, 3), replace(name, 'A', 'a') FROM nation");
        assertQuery("SELECT name FROM nation WHERE name LIKE 'A%'");
        assertQuery("SELECT CASE WHEN regionkey = 0 THEN 'A' ELSE 'B' END, CAST(nationkey AS varchar) FROM nation");
        assertQuery("SELECT year(orderdate), month(orderdate), day(orderdate) FROM orders WHERE orderkey < 200");
        assertQuery("SELECT count(*) FROM orders WHERE orderdate >= DATE '1995-01-01'");
    }

    @Test
    public void testReadSkipsDeletedRows()
    {
        assertQuery("SELECT count(*) FROM orders_deleted", "SELECT count(*) FROM " + ORDERS_DELETED_EXPECTED);
        assertQuery("SELECT * FROM orders_deleted", "SELECT * FROM " + ORDERS_DELETED_EXPECTED);
        assertQuery("SELECT count(*) FROM orders_deleted WHERE orderkey % 3 = 0", "VALUES 0");
        assertQuery("SELECT orderstatus, count(*), sum(totalprice) FROM orders_deleted GROUP BY orderstatus",
                "SELECT orderstatus, count(*), sum(totalprice) FROM " + ORDERS_DELETED_EXPECTED + " GROUP BY orderstatus");
        assertQuery("SELECT orderkey FROM orders_deleted WHERE orderkey BETWEEN 1000 AND 1100",
                "SELECT orderkey FROM " + ORDERS_DELETED_EXPECTED + " WHERE orderkey BETWEEN 1000 AND 1100");
    }

    @Test
    public void testJoinOverDeletedRows()
    {
        assertQuery("SELECT c.nationkey, count(*) FROM orders_deleted o JOIN customer c ON o.custkey = c.custkey GROUP BY c.nationkey",
                "SELECT c.nationkey, count(*) FROM " + ORDERS_DELETED_EXPECTED + " o JOIN customer c ON o.custkey = c.custkey GROUP BY c.nationkey");
        assertQuery("SELECT count(*) FROM orders o WHERE NOT EXISTS (SELECT 1 FROM orders_deleted d WHERE d.orderkey = o.orderkey)",
                "SELECT count(*) FROM orders WHERE orderkey % 3 = 0 OR custkey % 7 = 1");
    }

    @Test
    public void testReadAfterUpdate()
    {
        assertQuery("SELECT count(*) FROM orders_updated", "SELECT count(*) FROM orders");
        assertQuery("SELECT orderkey, orderpriority, totalprice FROM orders_updated",
                "SELECT orderkey, CASE WHEN orderstatus = 'P' THEN 'X-UPDATED' ELSE orderpriority END, "
                        + "CASE WHEN orderstatus = 'P' THEN totalprice * 2 ELSE totalprice END FROM orders");
    }

    @Test
    public void testHiddenColumns()
    {
        assertQuery("SELECT count(DISTINCT \"$path\") > 0 FROM orders", "VALUES true");
        assertQuery("SELECT count(*) FROM orders WHERE \"$data_sequence_number\" >= 1", "SELECT count(*) FROM orders");
    }
}
