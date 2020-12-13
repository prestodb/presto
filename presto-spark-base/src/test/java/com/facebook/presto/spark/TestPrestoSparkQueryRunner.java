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

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

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
        // some basic tests
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

        // 3-way union all with potentially non-flattened plan
        // See https://github.com/prestodb/presto/issues/12625
        //
        // CreateTable is not supported yet, use CreateTableAsSelect
        assertUpdate(
                "CREATE TABLE hive.hive_test.test_table_write_with_union AS " +
                        "SELECT orderkey, 'dummy' AS dummy " +
                        "FROM orders",
                15000);
        assertUpdate(
                "INSERT INTO hive.hive_test.test_table_write_with_union " +
                        "SELECT orderkey, dummy " +
                        "FROM (" +
                        "   SELECT orderkey, 'a' AS dummy FROM orders " +
                        "UNION ALL" +
                        "   SELECT orderkey, 'bb' AS dummy FROM orders " +
                        "UNION ALL" +
                        "   SELECT orderkey, 'ccc' AS dummy FROM orders " +
                        ")",
                45000);
    }

    @Test
    public void testBucketedTableWrite()
    {
        // create from bucketed table
        assertUpdate(
                "CREATE TABLE hive.hive_test.hive_orders_bucketed_1 WITH (bucketed_by=array['orderkey'], bucket_count=11) AS " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders_bucketed",
                15000);
        assertQuery(
                "SELECT count(*) " +
                        "FROM hive.hive_test.hive_orders_bucketed_1 " +
                        "WHERE \"$bucket\" = 1",
                "SELECT 1365");

        // create from non bucketed table
        assertUpdate(
                "CREATE TABLE hive.hive_test.hive_orders_bucketed_2 WITH (bucketed_by=array['orderkey'], bucket_count=11) AS " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders",
                15000);
        assertQuery(
                "SELECT count(*) " +
                        "FROM hive.hive_test.hive_orders_bucketed_2 " +
                        "WHERE \"$bucket\" = 1",
                "SELECT 1365");
    }

    @Test
    public void testAggregation()
    {
        assertQuery("select partkey, count(*) c from lineitem where partkey % 10 = 1 group by partkey having count(*) = 42");
    }

    @Test
    public void testBucketedAggregation()
    {
        assertBucketedQuery("SELECT orderkey, count(*) c FROM lineitem_bucketed WHERE partkey % 10 = 1 GROUP BY orderkey");
    }

    @Test
    public void testJoin()
    {
        assertQuery("SELECT l.orderkey, l.linenumber, p.brand " +
                "FROM lineitem l, part p " +
                "WHERE l.partkey = p.partkey");
    }

    @Test
    public void testBucketedJoin()
    {
        // both tables are bucketed
        assertBucketedQuery("SELECT l.orderkey, l.linenumber, o.orderstatus " +
                "FROM lineitem_bucketed l " +
                "JOIN orders_bucketed o " +
                "ON l.orderkey = o.orderkey " +
                "WHERE l.orderkey % 223 = 42 AND l.linenumber = 4 and o.orderstatus = 'O'");

        // only probe side table is bucketed
        assertBucketedQuery("SELECT l.orderkey, l.linenumber, o.orderstatus " +
                "FROM lineitem_bucketed l " +
                "JOIN orders o " +
                "ON l.orderkey = o.orderkey " +
                "WHERE l.orderkey % 223 = 42 AND l.linenumber = 4 and o.orderstatus = 'O'");

        // only build side table is bucketed
        assertBucketedQuery("SELECT l.orderkey, l.linenumber, o.orderstatus " +
                "FROM lineitem l " +
                "JOIN orders_bucketed o " +
                "ON l.orderkey = o.orderkey " +
                "WHERE l.orderkey % 223 = 42 AND l.linenumber = 4 and o.orderstatus = 'O'");
    }

    @Test
    public void testJoinUnderUnionALL()
    {
        assertUpdate("create table if not exists hive.hive_test.partitioned_nation_10 " +
                        "WITH (bucket_count = 10, bucketed_by = ARRAY['nationkey']) as select * from nation",
                25);
        assertUpdate("create table if not exists hive.hive_test.partitioned_nation_20 " +
                        "WITH (bucket_count = 20, bucketed_by = ARRAY['nationkey']) as select * from nation",
                25);
        assertUpdate("create table if not exists hive.hive_test.partitioned_nation_30 " +
                        "WITH (bucket_count = 30, bucketed_by = ARRAY['nationkey']) as select * from nation",
                25);

        assertQuery("SELECT hive.hive_test.partitioned_nation_10.nationkey " +
                        "FROM hive.hive_test.partitioned_nation_10 " +
                        "JOIN hive.hive_test.partitioned_nation_20 " +
                        "  ON hive.hive_test.partitioned_nation_10.nationkey = hive.hive_test.partitioned_nation_20.nationkey " +
                        "UNION ALL " +
                        "SELECT hive.hive_test.partitioned_nation_10.nationkey " +
                        "FROM hive.hive_test.partitioned_nation_10 " +
                        "JOIN hive.hive_test.partitioned_nation_30 " +
                        "  ON hive.hive_test.partitioned_nation_10.nationkey = hive.hive_test.partitioned_nation_30.nationkey ",
                "SELECT m.nationkey " +
                        "FROM nation m " +
                        "JOIN nation n " +
                        "  ON m.nationkey = n.nationkey " +
                        "UNION ALL " +
                        "SELECT m.nationkey " +
                        "FROM nation m " +
                        "JOIN nation n " +
                        "  ON m.nationkey = n.nationkey");

        assertQuery("SELECT nationkey " +
                        "FROM nation " +
                        "UNION ALL " +
                        "SELECT hive.hive_test.partitioned_nation_10.nationkey " +
                        "FROM hive.hive_test.partitioned_nation_10 " +
                        "JOIN hive.hive_test.partitioned_nation_30 " +
                        "  ON hive.hive_test.partitioned_nation_10.nationkey = hive.hive_test.partitioned_nation_30.nationkey ",
                "SELECT nationkey " +
                        "FROM nation " +
                        "UNION ALL " +
                        "SELECT m.nationkey " +
                        "FROM nation m " +
                        "JOIN nation n " +
                        "  ON m.nationkey = n.nationkey");
    }

    @Test
    public void testAggregationUnderUnionAll()
    {
        assertQuery("SELECT orderkey, 1 FROM orders UNION ALL SELECT orderkey, count(*) FROM orders GROUP BY 1",
                "SELECT orderkey, 1 FROM orders UNION ALL SELECT orderkey, count(*) FROM orders GROUP BY orderkey");

        assertQuery("SELECT " +
                        "   o.regionkey, " +
                        "   l.orderkey " +
                        "FROM (" +
                        "   SELECT " +
                        "       * " +
                        "   FROM lineitem " +
                        "   WHERE" +
                        "       linenumber = 4" +
                        ") l " +
                        "CROSS JOIN (" +
                        "   SELECT" +
                        "       regionkey," +
                        "       1 " +
                        "   FROM nation " +
                        "   UNION ALL " +
                        "   SELECT" +
                        "       regionkey," +
                        "       count(*) " +
                        "   FROM nation " +
                        "       GROUP BY regionkey" +
                        ") o",
                "SELECT " +
                        "   o.regionkey, " +
                        "   l.orderkey " +
                        "FROM (" +
                        "   SELECT " +
                        "       * " +
                        "   FROM lineitem " +
                        "   WHERE" +
                        "       linenumber = 4" +
                        ") l " +
                        "CROSS JOIN (" +
                        "   SELECT" +
                        "       regionkey," +
                        "       1 " +
                        "   FROM nation " +
                        "   UNION ALL " +
                        "   SELECT" +
                        "       regionkey," +
                        "       count(*) " +
                        "   FROM nation " +
                        "       GROUP BY regionkey" +
                        ") o");
    }

    @Test
    public void testCrossJoin()
    {
        assertQuery("" +
                "SELECT o.custkey, l.orderkey " +
                "FROM (SELECT * FROM lineitem WHERE linenumber = 4) l " +
                "CROSS JOIN (SELECT * FROM orders WHERE orderkey = 5) o");
        assertQuery("" +
                "SELECT o.custkey, l.orderkey " +
                "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                "CROSS JOIN (" +
                "   SELECT * FROM orders WHERE orderkey = 5 " +
                "   UNION ALL " +
                "   SELECT * FROM orders WHERE orderkey = 5 " +
                ") o");

        assertUpdate("create table if not exists hive.hive_test.partitioned_nation_11 " +
                        "WITH (bucket_count = 11, bucketed_by = ARRAY['nationkey']) as select * from nation",
                25);
        assertUpdate("create table if not exists hive.hive_test.partitioned_nation_22 " +
                        "WITH (bucket_count = 22, bucketed_by = ARRAY['nationkey']) as select * from nation",
                25);
        assertUpdate("create table if not exists hive.hive_test.partitioned_nation_33 " +
                        "WITH (bucket_count = 33, bucketed_by = ARRAY['nationkey']) as select * from nation",
                25);

        // UNION ALL over aggregation
        assertQuery("SELECT o.orderkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT orderkey, 1 FROM orders WHERE orderkey = 5 " +
                        "   UNION ALL " +
                        "   SELECT orderkey, count(*) " +
                        "       FROM orders WHERE orderkey = 5 " +
                        "   GROUP BY 1 " +
                        "   ) o",
                "SELECT o.orderkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT orderkey, 1 FROM orders WHERE orderkey = 5 " +
                        "   UNION ALL " +
                        "   SELECT orderkey, count(*) " +
                        "       FROM orders WHERE orderkey = 5 " +
                        "   GROUP BY orderkey " +
                        "   ) o");

        // 22 buckets UNION ALL 11 buckets
        assertQuery("SELECT o.regionkey, l.orderkey " +
                "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                "CROSS JOIN (" +
                "   SELECT * FROM hive.hive_test.partitioned_nation_22 " +
                "   UNION ALL " +
                "   SELECT * FROM hive.hive_test.partitioned_nation_11 " +
                ") o",
                "SELECT o.regionkey, l.orderkey " +
                "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                "CROSS JOIN (" +
                "   SELECT * FROM nation " +
                "   UNION ALL " +
                "   SELECT * FROM nation" +
                ") o");

        // 11 buckets UNION ALL 22 buckets
        assertQuery("SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM hive.hive_test.partitioned_nation_11 " +
                        "   UNION ALL " +
                        "   SELECT * FROM hive.hive_test.partitioned_nation_22 " +
                        ") o",
                "SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM nation" +
                        ") o");

        // bucketed UNION ALL non-bucketed
        assertQuery("SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM hive.hive_test.partitioned_nation_11 " +
                        "   UNION ALL " +
                        "   SELECT * FROM nation " +
                        ") o",
                "SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM nation" +
                        ") o");

        // non-bucketed UNION ALL bucketed
        assertQuery("SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM hive.hive_test.partitioned_nation_11 " +
                        ") o",
                "SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM nation" +
                        ") o");

        // 11 buckets UNION ALL non-bucketed UNION ALL 22 buckets
        assertQuery("SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM hive.hive_test.partitioned_nation_11 " +
                        "   UNION ALL " +
                        "   SELECT * FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM hive.hive_test.partitioned_nation_22 " +
                        ") o",
                "SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM nation" +
                        "   UNION ALL " +
                        "   SELECT * FROM nation" +
                        ") o");
    }

    @Test
    public void testNWayJoin()
    {
        assertQuery("SELECT l.orderkey, l.linenumber, p1.brand, p2.brand, p3.brand, p4.brand, p5.brand, p6.brand " +
                "FROM lineitem l, part p1, part p2, part p3, part p4, part p5, part p6 " +
                "WHERE l.partkey = p1.partkey " +
                "AND l.partkey = p2.partkey " +
                "AND l.partkey = p3.partkey " +
                "AND l.partkey = p4.partkey " +
                "AND l.partkey = p5.partkey " +
                "AND l.partkey = p6.partkey");
    }

    @Test
    public void testBucketedNWayJoin()
    {
        // all tables are bucketed
        assertBucketedQuery("SELECT l.orderkey, l.linenumber, o1.orderstatus, o2.orderstatus, o3.orderstatus, o4.orderstatus, o5.orderstatus, o6.orderstatus " +
                "FROM lineitem_bucketed l, orders_bucketed o1, orders_bucketed o2, orders_bucketed o3, orders_bucketed o4, orders_bucketed o5, orders_bucketed o6 " +
                "WHERE l.orderkey = o1.orderkey " +
                "AND l.orderkey = o2.orderkey " +
                "AND l.orderkey = o3.orderkey " +
                "AND l.orderkey = o4.orderkey " +
                "AND l.orderkey = o5.orderkey " +
                "AND l.orderkey = o6.orderkey");

        // some tables are bucketed
        assertBucketedQuery("SELECT l.orderkey, l.linenumber, o1.orderstatus, o2.orderstatus, o3.orderstatus, o4.orderstatus, o5.orderstatus, o6.orderstatus " +
                "FROM lineitem_bucketed l, orders o1, orders_bucketed o2, orders o3, orders_bucketed o4, orders o5, orders_bucketed o6 " +
                "WHERE l.orderkey = o1.orderkey " +
                "AND l.orderkey = o2.orderkey " +
                "AND l.orderkey = o3.orderkey " +
                "AND l.orderkey = o4.orderkey " +
                "AND l.orderkey = o5.orderkey " +
                "AND l.orderkey = o6.orderkey");
        assertBucketedQuery("SELECT l.orderkey, l.linenumber, o1.orderstatus, o2.orderstatus, o3.orderstatus, o4.orderstatus, o5.orderstatus, o6.orderstatus " +
                "FROM lineitem l, orders o1, orders_bucketed o2, orders o3, orders_bucketed o4, orders o5, orders_bucketed o6 " +
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

        assertBucketedQuery("SELECT * FROM lineitem_bucketed UNION ALL SELECT * FROM lineitem_bucketed");

        assertBucketedQuery("SELECT * FROM lineitem UNION ALL SELECT * FROM lineitem_bucketed");

        assertBucketedQuery("SELECT * FROM lineitem_bucketed UNION ALL SELECT * FROM lineitem");
    }

    @Test
    public void testBucketedUnionAll()
    {
        // all tables bucketed
        assertBucketedQuery("" +
                "SELECT orderkey, count(*) c " +
                "FROM (" +
                "   SELECT * FROM lineitem_bucketed " +
                "   UNION ALL " +
                "   SELECT * FROM lineitem_bucketed" +
                "   UNION ALL " +
                "   SELECT * FROM lineitem_bucketed" +
                ") GROUP BY orderkey");

        // some tables bucketed
        assertBucketedQuery("" +
                "SELECT orderkey, count(*) c " +
                "FROM (" +
                "   SELECT * FROM lineitem_bucketed " +
                "   UNION ALL " +
                "   SELECT * FROM lineitem" +
                "   UNION ALL " +
                "   SELECT * FROM lineitem_bucketed" +
                ") GROUP BY orderkey");
        assertBucketedQuery("" +
                "SELECT orderkey, count(*) c " +
                "FROM (" +
                "   SELECT * FROM lineitem " +
                "   UNION ALL " +
                "   SELECT * FROM lineitem_bucketed" +
                "   UNION ALL " +
                "   SELECT * FROM lineitem" +
                ") GROUP BY orderkey");
        assertBucketedQuery("" +
                "SELECT orderkey, count(*) c " +
                "FROM (" +
                "   SELECT * FROM lineitem " +
                "   UNION ALL " +
                "   SELECT * FROM lineitem_bucketed" +
                ") GROUP BY orderkey");
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
                "CREATE TABLE hive.hive_test.hive_orders_test_failures AS " +
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

    @Test
    public void testSelectFromEmptyBucketedTable()
    {
        assertUpdate(
                "CREATE TABLE hive.hive_test.empty_orders_bucketed WITH (bucketed_by=array['orderkey'], bucket_count=11) AS " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders " +
                        "WITH NO DATA",
                0);

        assertQuery(
                "SELECT count(*) FROM (SELECT orderkey, count(*) FROM hive.hive_test.empty_orders_bucketed GROUP BY orderkey)",
                "SELECT 0");
    }

    @Test
    public void testLimit()
    {
        MaterializedResult actual = computeActual("SELECT * FROM orders LIMIT 10");
        assertEquals(actual.getRowCount(), 10);
        actual = computeActual("SELECT 'a' FROM orders LIMIT 10");
        assertEquals(actual.getRowCount(), 10);
    }

    @Test
    public void testTableSampleSystem()
    {
        long totalRows = (Long) computeActual("SELECT count(*) FROM orders").getOnlyValue();
        long sampledRows = (Long) computeActual("SELECT count(*) FROM orders TABLESAMPLE SYSTEM (1)").getOnlyValue();
        assertThat(sampledRows).isLessThan(totalRows);
    }

    @Test
    public void testTimeouts()
    {
        // Expected run time for this query is ~30s
        String longRunningCrossJoin = "SELECT count(l1.orderkey), count(l2.orderkey) FROM lineitem l1, lineitem l2";

        Session queryMaxRunTimeLimitSession = Session.builder(getSession())
                .setSystemProperty("query_max_run_time", "2s")
                .build();
        assertQueryFails(queryMaxRunTimeLimitSession, longRunningCrossJoin, "Query exceeded maximum time limit of 2.00s");

        Session queryMaxExecutionTimeLimitSession = Session.builder(getSession())
                .setSystemProperty("query_max_run_time", "3s")
                .setSystemProperty("query_max_execution_time", "2s")
                .build();
        assertQueryFails(queryMaxExecutionTimeLimitSession, longRunningCrossJoin, "Query exceeded maximum time limit of 2.00s");
    }

    private void assertBucketedQuery(String sql)
    {
        assertQuery(sql, sql.replaceAll("_bucketed", ""));
    }
}
