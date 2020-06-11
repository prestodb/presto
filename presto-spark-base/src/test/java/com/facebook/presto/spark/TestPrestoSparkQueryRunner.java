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
package com.facebook.presto.spark;

import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

public class TestPrestoSparkQueryRunner
        extends AbstractTestQueryFramework
{
    public TestPrestoSparkQueryRunner()
    {
        super(PrestoSparkQueryRunner::createHivePrestoSparkQueryRunner);
    }

    @Test
    public void testTableWrite()
    {
        assertUpdate(
                "CREATE TABLE hive.hive_test.hive_orders AS " +
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                "FROM orders",
                15000);

        assertUpdate(
                "INSERT INTO hive.hive_test.hive_orders " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders " +
                        "UNION ALL " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders",
                30000);

        assertQuery(
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM hive.hive_test.hive_orders",
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders " +
                        "UNION ALL " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders " +
                        "UNION ALL " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders");
    }

    @Test
    public void testAggregation()
    {
        assertQuery("select partkey, count(*) c from lineitem where partkey % 10 = 1 group by partkey having count(*) = 42");
    }

    @Test
    public void testJoin()
    {
        assertQuery("SELECT l.orderkey, l.linenumber, o.orderstatus " +
                "FROM lineitem l " +
                "JOIN orders o " +
                "ON l.orderkey = o.orderkey " +
                "WHERE l.orderkey % 223 = 42 AND l.linenumber = 4 and o.orderstatus = 'O'");
    }

    @Test
    public void testCrossJoin()
    {
        assertQuery("" +
                "SELECT o.custkey, l.orderkey " +
                "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                "CROSS JOIN (SELECT * FROM orders WHERE orderkey = 5) o");
        assertQuery("" +
                "SELECT o.custkey, l.orderkey " +
                "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                "CROSS JOIN (" +
                "   SELECT * FROM orders WHERE orderkey = 5 " +
                "   UNION ALL " +
                "   SELECT * FROM orders WHERE orderkey = 5 " +
                ") o");
    }

    @Test
    public void testNWayJoin()
    {
        assertQuery("SELECT l.orderkey, l.linenumber, o1.orderstatus, o2.orderstatus, o3.orderstatus, o4.orderstatus, o5.orderstatus, o6.orderstatus " +
                "FROM lineitem l, orders o1, orders o2, orders o3, orders o4, orders o5, orders o6 " +
                "WHERE l.orderkey = o1.orderkey " +
                "AND l.orderkey = o2.orderkey " +
                "AND l.orderkey = o3.orderkey " +
                "AND l.orderkey = o4.orderkey " +
                "AND l.orderkey = o5.orderkey " +
                "AND l.orderkey = o6.orderkey");
    }

    @Test
    public void testUnionAll()
    {
        assertQuery("SELECT * FROM orders UNION ALL SELECT * FROM orders");
    }

    @Test
    public void testValues()
    {
        assertQuery("SELECT a, b " +
                "FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')) t1 (a, b) ");
    }

    @Test
    public void testUnionWithAggregationAndJoin()
    {
        assertQuery(
                "SELECT * FROM ( " +
                        "SELECT orderkey, count(*) FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION ALL " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "GROUP BY orderkey) t " +
                        "JOIN orders o " +
                        "ON (o.orderkey = t.orderkey)");
    }

    @Test
    public void testFailures()
    {
        assertQueryFails("SELECT * FROM orders WHERE custkey / (orderkey - orderkey) = 0", "/ by zero");
        assertQueryFails(
                "CREATE TABLE hive.hive_test.hive_orders AS " +
                        "(SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders) " +
                        "UNION ALL " +
                        "(SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders " +
                        "WHERE custkey / (orderkey - orderkey) = 0 )",
                "/ by zero");
    }

    @Test
    public void testSelectFromEmptyTable()
    {
        assertUpdate(
                "CREATE TABLE hive.hive_test.empty_orders AS " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders " +
                        "WITH NO DATA",
                0);

        assertQuery(
                "SELECT count(*) FROM hive.hive_test.empty_orders",
                "SELECT 0");
    }
}
