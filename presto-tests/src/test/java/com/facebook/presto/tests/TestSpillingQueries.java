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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestSpillingQueries
        extends AbstractTestQueryFramework
{
    public TestSpillingQueries()
    {
        super(TestSpillingQueries::createQueryRunner);
    }

    private static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(SystemSessionProperties.TASK_CONCURRENCY, "2")
                .setSystemProperty(SystemSessionProperties.SPILL_ENABLED, "true")
                .setSystemProperty(SystemSessionProperties.AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT, "128kB")
                .build();

        ImmutableMap<String, String> extraProperties = ImmutableMap.<String, String>builder()
                .put("experimental.spiller-spill-path", Paths.get(System.getProperty("java.io.tmpdir"), "presto", "spills").toString())
                .put("experimental.spiller-max-used-space-threshold", "1.0")
                .put("experimental.memory-revoking-threshold", "0.0") // revoke always
                .put("experimental.memory-revoking-target", "0.0")
                .build();

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(defaultSession, 2, extraProperties, ImmutableMap.of(), new SqlParserOptions());

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    @Test
    public void testJoinWithMultiFieldGroupBy()
    {
        assertQuery("SELECT orderstatus FROM lineitem JOIN (SELECT DISTINCT orderkey, orderstatus FROM ORDERS) T on lineitem.orderkey = T.orderkey");
    }

    @Test
    public void testGroupByRepeatedField()
    {
        assertQuery("SELECT sum(custkey) FROM orders GROUP BY orderstatus, orderstatus");
        assertQuery("SELECT count(*) FROM (select orderstatus a, orderstatus b FROM orders) GROUP BY a, b");
    }

    @Test
    public void testGroupByMultipleFieldsWithPredicateOnAggregationArgument()
    {
        assertQuery("SELECT custkey, orderstatus, MAX(orderkey) FROM ORDERS WHERE orderkey = 1 GROUP BY custkey, orderstatus");
    }

    @Test
    public void testGroupAggregationOverNestedGroupByAggregation()
    {
        assertQuery("SELECT sum(custkey), max(orderstatus), min(c) FROM (SELECT orderstatus, custkey, COUNT(*) c FROM ORDERS GROUP BY orderstatus, custkey) T");
    }

    @Test
    public void test15WayGroupBy()
    {
        // Among other things, this test verifies we are not getting for overflow in the distributed HashPagePartitionFunction
        assertQuery("" +
                "SELECT " +
                "    orderkey + 1, orderkey + 2, orderkey + 3, orderkey + 4, orderkey + 5, " +
                "    orderkey + 6, orderkey + 7, orderkey + 8, orderkey + 9, orderkey + 10, " +
                "    count(*) " +
                "FROM orders " +
                "GROUP BY " +
                "    orderkey + 1, orderkey + 2, orderkey + 3, orderkey + 4, orderkey + 5, " +
                "    orderkey + 6, orderkey + 7, orderkey + 8, orderkey + 9, orderkey + 10");
    }

    @Test
    public void testDistinctMultipleFields()
    {
        assertQuery("SELECT DISTINCT custkey, orderstatus FROM ORDERS");
    }

    @Test
    public void testDistinctJoin()
    {
        assertQuery("SELECT COUNT(DISTINCT CAST(b.quantity AS BIGINT)), a.orderstatus " +
                "FROM orders a " +
                "JOIN lineitem b " +
                "ON a.orderkey = b.orderkey " +
                "GROUP BY a.orderstatus");
    }

    @Test
    public void testDistinctGroupBy()
    {
        assertQuery("SELECT COUNT(DISTINCT clerk) as count, orderdate FROM orders GROUP BY orderdate ORDER BY count, orderdate");
    }

    @Test
    public void testSingleDistinctOptimizer()
    {
        assertQuery("SELECT custkey, orderstatus, COUNT(DISTINCT orderkey) FROM orders GROUP BY custkey, orderstatus");
        assertQuery("SELECT custkey, orderstatus, COUNT(DISTINCT orderkey), SUM(DISTINCT orderkey) FROM orders GROUP BY custkey, orderstatus");
        assertQuery("" +
                "SELECT custkey, COUNT(DISTINCT orderstatus) FROM (" +
                "   SELECT orders.custkey AS custkey, orders.orderstatus AS orderstatus " +
                "   FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey " +
                "   GROUP BY orders.custkey, orders.orderstatus" +
                ") " +
                "GROUP BY custkey");
        assertQuery("SELECT custkey, COUNT(DISTINCT orderkey), COUNT(DISTINCT orderstatus) FROM orders GROUP BY custkey");

        assertQuery("SELECT SUM(DISTINCT x) FROM (SELECT custkey, COUNT(DISTINCT orderstatus) x FROM orders GROUP BY custkey) t");
    }

    @Test
    public void testExtractDistinctAggregationOptimizer()
    {
        assertQuery("SELECT max(orderstatus), COUNT(orderkey), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT custkey, orderstatus, avg(shippriority), SUM(DISTINCT orderkey) FROM orders GROUP BY custkey, orderstatus");

        assertQuery("SELECT s, MAX(custkey), SUM(a) FROM (" +
                "    SELECT custkey, avg(shippriority) as a, SUM(DISTINCT orderkey) as s FROM orders GROUP BY custkey, orderstatus" +
                ") " +
                "GROUP BY s");

        assertQuery("SELECT max(orderstatus), COUNT(distinct orderkey), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT max(orderstatus), COUNT(distinct shippriority), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT COUNT(tan(shippriority)), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT count(DISTINCT a), max(b) FROM (VALUES (row(1, 2), 3)) t(a, b)", "VALUES (1, 3)");

        // Test overlap between GroupBy columns and aggregation columns
        assertQuery("SELECT shippriority, MAX(orderstatus), SUM(DISTINCT shippriority) FROM orders GROUP BY shippriority");

        assertQuery("SELECT shippriority, COUNT(shippriority), SUM(DISTINCT orderkey) FROM orders GROUP BY shippriority");

        assertQuery("SELECT shippriority, COUNT(shippriority), SUM(DISTINCT shippriority) FROM orders GROUP BY shippriority");

        assertQuery("SELECT clerk, shippriority, MAX(orderstatus), SUM(DISTINCT shippriority) FROM orders GROUP BY clerk, shippriority");

        assertQuery("SELECT clerk, shippriority, COUNT(shippriority), SUM(DISTINCT orderkey) FROM orders GROUP BY clerk, shippriority");

        assertQuery("SELECT clerk, shippriority, COUNT(shippriority), SUM(DISTINCT shippriority) FROM orders GROUP BY clerk, shippriority");
    }

    @Test
    public void testGroupByOrderByLimit()
    {
        assertQueryOrdered("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey ORDER BY SUM(totalprice) DESC LIMIT 10");
    }

    @Test
    public void testDistinctHaving()
    {
        assertQuery("SELECT COUNT(DISTINCT clerk) AS count " +
                "FROM orders " +
                "GROUP BY orderdate " +
                "HAVING COUNT(DISTINCT clerk) > 1");
    }

    @Test
    public void testRepeatedAggregations()
    {
        assertQuery("SELECT SUM(orderkey), SUM(orderkey) FROM ORDERS");
    }

    @Test
    public void testGroupingSets()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY suppkey");
    }

    @Test
    public void testIntersect()
    {
        assertQuery(
                "SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT select regionkey FROM nation WHERE nationkey > 21");
        assertQuery(
                "SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT DISTINCT SELECT regionkey FROM nation WHERE nationkey > 21",
                "VALUES 1, 3");
        assertQuery(
                "WITH wnation AS (SELECT nationkey, regionkey FROM nation) " +
                        "SELECT regionkey FROM wnation WHERE nationkey < 7 " +
                        "INTERSECT SELECT regionkey FROM wnation WHERE nationkey > 21", "VALUES 1, 3");
        assertQuery(
                "SELECT num FROM (SELECT 1 as num FROM nation WHERE nationkey=10 " +
                        "INTERSECT SELECT 1 FROM nation WHERE nationkey=20) T");
        assertQuery(
                "SELECT nationkey, nationkey / 2 FROM (SELECT nationkey FROM nation WHERE nationkey < 10 " +
                        "INTERSECT SELECT nationkey FROM nation WHERE nationkey > 4) T WHERE nationkey % 2 = 0");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "UNION SELECT 4");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "UNION SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "INTERSECT SELECT 1");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "UNION ALL SELECT 3");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "UNION ALL SELECT 3");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) " +
                        "INTERSECT SELECT * FROM (VALUES 1.0, 2)",
                "VALUES 1.0, 2.0");
        assertQuery("SELECT NULL, NULL INTERSECT SELECT NULL, NULL FROM nation");

        MaterializedResult emptyResult = computeActual("SELECT 100 INTERSECT (SELECT regionkey FROM nation WHERE nationkey <10)");
        assertEquals(emptyResult.getMaterializedRows().size(), 0);
    }

    @Test
    public void testIntersectWithAggregation()
    {
        assertQuery("SELECT COUNT(*) FROM nation INTERSECT SELECT COUNT(regionkey) FROM nation HAVING SUM(regionkey) IS NOT NULL");
        assertQuery("SELECT SUM(nationkey), COUNT(name) FROM (SELECT nationkey,name FROM nation INTERSECT SELECT regionkey, name FROM nation) n");
        assertQuery("SELECT COUNT(*) * 2 FROM nation INTERSECT (SELECT SUM(nationkey) FROM nation GROUP BY regionkey ORDER BY 1 LIMIT 2)");
        assertQuery("SELECT COUNT(a) FROM (SELECT nationkey AS a FROM (SELECT nationkey FROM nation INTERSECT SELECT regionkey FROM nation) n1 INTERSECT SELECT regionkey FROM nation) n2");
        assertQuery("SELECT COUNT(*), SUM(2), regionkey FROM (SELECT nationkey, regionkey FROM nation INTERSECT SELECT regionkey, regionkey FROM nation) n GROUP BY regionkey");
        assertQuery("SELECT COUNT(*) FROM (SELECT nationkey FROM nation INTERSECT SELECT 2) n1 INTERSECT SELECT regionkey FROM nation");
    }

    @Test
    public void testIntersectAllFails()
    {
        assertQueryFails("SELECT * FROM (VALUES 1, 2, 3, 4) INTERSECT ALL SELECT * FROM (VALUES 3, 4)", "line 1:35: INTERSECT ALL not yet implemented");
    }

    @Test
    public void testExcept()
    {
        assertQuery(
                "SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "EXCEPT select regionkey FROM nation WHERE nationkey > 21");
        assertQuery(
                "SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "EXCEPT DISTINCT SELECT regionkey FROM nation WHERE nationkey > 21",
                "VALUES 0, 4");
        assertQuery(
                "WITH wnation AS (SELECT nationkey, regionkey FROM nation) " +
                        "SELECT regionkey FROM wnation WHERE nationkey < 7 " +
                        "EXCEPT SELECT regionkey FROM wnation WHERE nationkey > 21",
                "VALUES 0, 4");
        assertQuery(
                "SELECT num FROM (SELECT 1 as num FROM nation WHERE nationkey=10 " +
                        "EXCEPT SELECT 2 FROM nation WHERE nationkey=20) T");
        assertQuery(
                "SELECT nationkey, nationkey / 2 FROM (SELECT nationkey FROM nation WHERE nationkey < 10 " +
                        "EXCEPT SELECT nationkey FROM nation WHERE nationkey > 4) T WHERE nationkey % 2 = 0");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "EXCEPT SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "UNION SELECT 3");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "UNION SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "EXCEPT SELECT 1");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "EXCEPT SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "UNION ALL SELECT 4");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) " +
                        "EXCEPT SELECT * FROM (VALUES 3.0, 2)");
        assertQuery("SELECT NULL, NULL EXCEPT SELECT NULL, NULL FROM nation");

        assertQuery(
                "(SELECT * FROM (VALUES 1) EXCEPT SELECT * FROM (VALUES 0))" +
                        "EXCEPT (SELECT * FROM (VALUES 1) EXCEPT SELECT * FROM (VALUES 1))");

        MaterializedResult emptyResult = computeActual("SELECT 0 EXCEPT (SELECT regionkey FROM nation WHERE nationkey <10)");
        assertEquals(emptyResult.getMaterializedRows().size(), 0);
    }

    @Test
    public void testExceptWithAggregation()
    {
        assertQuery("SELECT COUNT(*) FROM nation EXCEPT SELECT COUNT(regionkey) FROM nation where regionkey < 3 HAVING SUM(regionkey) IS NOT NULL");
        assertQuery("SELECT SUM(nationkey), COUNT(name) FROM (SELECT nationkey, name FROM nation where nationkey < 6 EXCEPT SELECT regionkey, name FROM nation) n");
        assertQuery("(SELECT SUM(nationkey) FROM nation GROUP BY regionkey ORDER BY 1 LIMIT 2) EXCEPT SELECT COUNT(*) * 2 FROM nation");
        assertQuery("SELECT COUNT(a) FROM (SELECT nationkey AS a FROM (SELECT nationkey FROM nation EXCEPT SELECT regionkey FROM nation) n1 EXCEPT SELECT regionkey FROM nation) n2");
        assertQuery("SELECT COUNT(*), SUM(2), regionkey FROM (SELECT nationkey, regionkey FROM nation EXCEPT SELECT regionkey, regionkey FROM nation) n GROUP BY regionkey HAVING regionkey < 3");
        assertQuery("SELECT COUNT(*) FROM (SELECT nationkey FROM nation EXCEPT SELECT 10) n1 EXCEPT SELECT regionkey FROM nation");
    }

    @Test
    public void testJoinWithLessThanInJoinClause()
    {
        assertQuery("SELECT n.nationkey, r.regionkey FROM region r JOIN nation n ON n.regionkey = r.regionkey AND n.name < r.name");
        assertQuery("SELECT l.suppkey, n.nationkey, l.partkey, n.regionkey FROM nation n JOIN lineitem l ON l.suppkey = n.nationkey AND l.partkey < n.regionkey");
        // test with single null value in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, CAST(-1 AS BIGINT)), (0, NULL), (0, CAST(0 AS BIGINT))) t(a, b) WHERE n.regionkey - 100 < t.b AND n.nationkey = t.a",
                "VALUES -1, 0");
        // test with single (first) null value in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, NULL), (0, CAST(-1 AS BIGINT)), (0, CAST(0 AS BIGINT))) t(a, b) WHERE n.regionkey - 100 < t.b AND n.nationkey = t.a",
                "VALUES -1, 0");
        // test with multiple null values in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, NULL), (0, NULL), (0, CAST(-1 AS BIGINT)), (0, NULL)) t(a, b) WHERE n.regionkey - 100 < t.b AND n.nationkey = t.a",
                "VALUES -1");
        // test with only null value in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, NULL)) t(a, b) WHERE n.regionkey - 100 < t.b AND n.nationkey = t.a", "SELECT 1 WHERE FALSE");
        // test with function predicate in ON clause
        assertQuery("SELECT n.nationkey, r.regionkey FROM nation n JOIN region r ON n.regionkey = r.regionkey AND length(n.name) < length(substr(r.name, 5))");
    }

    @Test
    public void testJoinWithGreaterThanInJoinClause()
    {
        assertQuery("SELECT n.nationkey, r.regionkey FROM region r JOIN nation n ON n.regionkey = r.regionkey AND n.name > r.name AND r.regionkey = 0");
        assertQuery("SELECT l.suppkey, n.nationkey, l.partkey, n.regionkey FROM nation n JOIN lineitem l ON l.suppkey = n.nationkey AND l.partkey > n.regionkey");
        // test with single null value in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, CAST(-1 AS BIGINT)), (0, NULL), (0, CAST(0 AS BIGINT))) t(a, b) WHERE n.regionkey + 100 > t.b AND n.nationkey = t.a",
                "VALUES -1, 0");
        // test with single (first) null value in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, NULL), (0, CAST(-1 AS BIGINT)), (0, CAST(0 AS BIGINT))) t(a, b) WHERE n.regionkey + 100 > t.b AND n.nationkey = t.a",
                "VALUES -1, 0");
        // test with multiple null values in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, NULL), (0, NULL), (0, CAST(-1 AS BIGINT)), (0, NULL)) t(a, b) WHERE n.regionkey + 100 > t.b AND n.nationkey = t.a",
                "VALUES -1");
        // test with only null value in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, NULL)) t(a, b) WHERE n.regionkey + 100 > t.b AND n.nationkey = t.a", "SELECT 1 WHERE FALSE");
        /// test with function predicate in ON clause
        assertQuery("SELECT n.nationkey, r.regionkey FROM nation n JOIN region r ON n.regionkey = r.regionkey AND length(n.name) > length(substr(r.name, 5))");
    }

    @Test
    public void testJoinWithRangePredicatesinJoinClause()
    {
        assertQuery("SELECT COUNT(*) " +
                "FROM (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem " +
                "JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders " +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8 AND lineitem.linenumber % 2 = 0 " +
                "AND orders.custkey % 8 < 7 AND lineitem.suppkey % 10 < orders.custkey % 7 AND lineitem.suppkey % 7 > orders.custkey % 7");

        assertQuery("SELECT COUNT(*) " +
                "FROM (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem " +
                "JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders " +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8 AND lineitem.linenumber % 2 = 0 " +
                "AND orders.custkey % 8 < lineitem.linenumber % 2 AND lineitem.suppkey % 10 < orders.custkey % 7 AND lineitem.suppkey % 7 > orders.custkey % 7");
    }

    @Test
    public void testJoinWithMultipleLessThanPredicatesDifferentOrders()
    {
        // test that fast inequality join is not sensitive to order of search conjuncts.
        assertQuery("SELECT count(*) FROM lineitem l JOIN nation n ON l.suppkey % 5 = n.nationkey % 5 AND l.partkey % 3 < n.regionkey AND l.partkey % 3 + 1 < n.regionkey AND l.partkey % 3 + 2 < n.regionkey");
        assertQuery("SELECT count(*) FROM lineitem l JOIN nation n ON l.suppkey % 5 = n.nationkey % 5 AND l.partkey % 3 + 2 < n.regionkey AND l.partkey % 3 + 1 < n.regionkey AND l.partkey % 3 < n.regionkey");
        assertQuery("SELECT count(*) FROM lineitem l JOIN nation n ON l.suppkey % 5 = n.nationkey % 5 AND l.partkey % 3 > n.regionkey AND l.partkey % 3 + 1 > n.regionkey AND l.partkey % 3 + 2 > n.regionkey");
        assertQuery("SELECT count(*) FROM lineitem l JOIN nation n ON l.suppkey % 5 = n.nationkey % 5 AND l.partkey % 3 + 2 > n.regionkey AND l.partkey % 3 + 1 > n.regionkey AND l.partkey % 3 > n.regionkey");
    }

    @Test
    public void testJoinWithLessThanOnDatesInJoinClause()
    {
        assertQuery(
                "SELECT o.orderkey, o.orderdate, l.shipdate FROM orders o JOIN lineitem l ON l.orderkey = o.orderkey AND l.shipdate < o.orderdate + INTERVAL '10' DAY",
                "SELECT o.orderkey, o.orderdate, l.shipdate FROM orders o JOIN lineitem l ON l.orderkey = o.orderkey AND l.shipdate < DATEADD('DAY', 10, o.orderdate)");
        assertQuery(
                "SELECT o.orderkey, o.orderdate, l.shipdate FROM lineitem l JOIN orders o ON l.orderkey = o.orderkey AND l.shipdate < DATE_ADD('DAY', 10, o.orderdate)",
                "SELECT o.orderkey, o.orderdate, l.shipdate FROM orders o JOIN lineitem l ON l.orderkey = o.orderkey AND l.shipdate < DATEADD('DAY', 10, o.orderdate)");
        assertQuery(
                "SELECT o.orderkey, o.orderdate, l.shipdate FROM orders o JOIN lineitem l ON o.orderkey=l.orderkey AND o.orderdate + INTERVAL '2' DAY <= l.shipdate AND l.shipdate < o.orderdate + INTERVAL '7' DAY",
                "SELECT o.orderkey, o.orderdate, l.shipdate FROM orders o JOIN lineitem l ON o.orderkey=l.orderkey AND DATEADD('DAY', 2, o.orderdate) <= l.shipdate AND l.shipdate < DATEADD('DAY', 7, o.orderdate)");
    }

    @Test
    public void testJoinWithNonDeterministicLessThan()
    {
        MaterializedRow actualRow = getOnlyElement(computeActual(
                "SELECT count(*) FROM " +
                        "customer c1 JOIN customer c2 ON c1.nationkey=c2.nationkey " +
                        "WHERE c1.custkey - RANDOM(CAST(c1.custkey AS BIGINT)) < c2.custkey").getMaterializedRows());
        assertEquals(actualRow.getFieldCount(), 1);
        long actualCount = (Long) actualRow.getField(0); // this should be around ~69000

        MaterializedRow expectedAtLeastRow = getOnlyElement(computeActual(
                "SELECT count(*) FROM " +
                        "customer c1 JOIN customer c2 ON c1.nationkey=c2.nationkey " +
                        "WHERE c1.custkey < c2.custkey").getMaterializedRows());
        assertEquals(expectedAtLeastRow.getFieldCount(), 1);
        long expectedAtLeastCount = (Long) expectedAtLeastRow.getField(0); // this is exactly 45022

        // Technically non-deterministic unit test but has hopefully a next to impossible chance of a false positive
        assertTrue(actualCount > expectedAtLeastCount);
    }

    @Test
    public void testNonEqualityJoin()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity + length(orders.comment) > 7");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND NOT lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON NOT NOT lineitem.orderkey = orders.orderkey AND NOT NOT lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND NOT NOT NOT lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity <= 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity != 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.shipdate > orders.orderdate");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderdate < lineitem.shipdate");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment LIKE '%forges%'");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment LIKE lineitem.comment");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.comment LIKE '%forges%'");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.comment LIKE orders.comment");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment NOT LIKE '%forges%'");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment NOT LIKE lineitem.comment");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND NOT (orders.comment LIKE '%forges%')");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND NOT (orders.comment LIKE lineitem.comment)");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity + length(orders.comment) > 7");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND NULL");
    }

    @Test
    public void testNonEqualityLeftJoin()
    {
        assertQuery("SELECT COUNT(*) FROM " +
                "      (SELECT * FROM lineitem ORDER BY orderkey,linenumber LIMIT 5) l " +
                "         LEFT OUTER JOIN " +
                "      (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o " +
                "         ON " +
                "      o.custkey != 1000 WHERE o.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > 1000 WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > 1000.0 WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > orders.totalprice WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > lineitem.quantity WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 WHERE orders.orderkey IS NULL");
    }

    @Test
    public void testLeftJoinWithEmptyInnerTable()
    {
        // Use orderkey = rand() to create an empty relation
        assertQuery("SELECT * FROM lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON a.orderkey = b.orderkey");
        assertQuery("SELECT * FROM lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON a.orderkey > b.orderkey");
        assertQuery("SELECT * FROM lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON 1 = 1");
        assertQuery("SELECT * FROM lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON b.orderkey > 1");
        assertQuery("SELECT * FROM lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON b.orderkey > b.totalprice");
    }

    @Test
    public void testRightJoinWithEmptyInnerTable()
    {
        // Use orderkey = rand() to create an empty relation
        assertQuery("SELECT * FROM orders b RIGHT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON a.orderkey = b.orderkey");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON a.orderkey > b.orderkey");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON 1 = 1");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON b.orderkey > 1");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON b.orderkey > b.totalprice");
    }

    @Test
    public void testNonEqualityRightJoin()
    {
        assertQuery("SELECT COUNT(*) FROM " +
                "      (SELECT * FROM lineitem ORDER BY orderkey,linenumber LIMIT 5) l " +
                "         RIGHT OUTER JOIN " +
                "      (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o " +
                "         ON " +
                "      l.quantity != 5 WHERE l.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 WHERE lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5.0 WHERE lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > lineitem.suppkey WHERE lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity*1000 > orders.totalprice WHERE lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice > 1000 WHERE lineitem.orderkey IS NULL");
    }

    @Test
    public void testNonEqualityFullJoin()
    {
        assertQuery(
                "SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 WHERE lineitem.orderkey IS NULL OR orders.orderkey IS NULL",
                "SELECT COUNT(*) FROM " +
                        "(SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 " +
                        "    UNION ALL " +
                        "SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 " +
                        "    WHERE lineitem.orderkey IS NULL) " +
                        " WHERE o1 IS NULL OR o2 IS NULL");
        assertQuery(
                "SELECT COUNT(*) FROM lineitem FULL OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > 1000 WHERE lineitem.orderkey IS NULL OR orders.orderkey IS NULL",
                "SELECT COUNT(*) FROM " +
                        "(SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > 1000 " +
                        "    UNION ALL " +
                        "SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > 1000 " +
                        "    WHERE lineitem.orderkey IS NULL) " +
                        " WHERE o1 IS NULL OR o2 IS NULL");
        assertQuery(
                "SELECT COUNT(*) FROM lineitem FULL OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > lineitem.quantity WHERE lineitem.orderkey IS NULL OR orders.orderkey IS NULL",
                "SELECT COUNT(*) FROM " +
                        "(SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > lineitem.quantity " +
                        "    UNION ALL " +
                        "SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > lineitem.quantity " +
                        "    WHERE lineitem.orderkey IS NULL) " +
                        " WHERE o1 IS NULL OR o2 IS NULL");
    }

    @Test
    public void testLeftFilteredJoin()
    {
        // Test predicate move around
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM (SELECT * FROM lineitem WHERE orderkey % 2 = 0) a JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testRightFilteredJoin()
    {
        // Test predicate move around
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM lineitem JOIN (SELECT *  FROM orders WHERE orderkey % 2 = 0) a ON lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testJoinWithFullyPushedDownJoinClause()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON orders.custkey = 1 AND lineitem.orderkey = 1");
    }

    @Test
    public void testJoinPredicateMoveAround()
    {
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem\n" +
                "JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders\n" +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8 AND lineitem.linenumber % 2 = 0\n" +
                "WHERE orders.custkey % 8 < 7 AND orders.custkey % 8 = lineitem.orderkey % 8 AND lineitem.suppkey % 7 > orders.custkey % 7");
    }

    @Test
    public void testSimpleFullJoin()
    {
        assertQuery("SELECT a, b FROM (VALUES (1), (2)) t (a) FULL OUTER JOIN (VALUES (1), (3)) u (b) ON a = b",
                "SELECT * FROM (VALUES (1, 1), (2, NULL), (NULL, 3))");
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey " +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey " +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
        assertQuery("SELECT COUNT(*) FROM lineitem FULL OUTER JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey " +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey " +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");

        // The above outer join queries will produce the same result even if they are inner join.
        // The below query uses "orderkey = custkey" as join condition.
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.custkey",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.custkey " +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.custkey " +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
    }

    @Test
    public void testFullJoinNormalizedToLeft()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey WHERE lineitem.orderkey IS NOT NULL",
                "SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey WHERE lineitem.orderkey IS NOT NULL");

        // The above outer join queries will produce the same result even if they are inner join.
        // The below query uses "orderkey = custkey" as join condition.
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.custkey WHERE lineitem.orderkey IS NOT NULL",
                "SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.custkey WHERE lineitem.orderkey IS NOT NULL");
    }

    @Test
    public void testFullJoinNormalizedToRight()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey WHERE orders.orderkey IS NOT NULL",
                "SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey  WHERE orders.orderkey IS NOT NULL");

        // The above outer join queries will produce the same result even if they are inner join.
        // The below query uses "orderkey = custkey" as join condition.
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.custkey WHERE orders.custkey IS NOT NULL",
                "SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.custkey  WHERE orders.custkey IS NOT NULL");
    }

    @Test
    public void testFullJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem FULL JOIN orders ON lineitem.orderkey = 1024",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = 1024 " +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = 1024 " +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
    }

    @Test
    public void testFullJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem FULL JOIN orders ON orders.orderkey = 1024",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT OUTER JOIN orders ON orders.orderkey = 1024 " +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT OUTER JOIN orders ON orders.orderkey = 1024 " +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
    }

    @Test
    public void testSimpleFullJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2" +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2" +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
    }

    @Test
    public void testSimpleFullJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2" +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2" +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
    }

    @Test
    public void testOuterJoinWithNullsOnProbe()
    {
        assertQuery(
                "SELECT DISTINCT a.orderkey FROM " +
                        "(SELECT CASE WHEN orderkey > 10 THEN orderkey END orderkey FROM orders WHERE orderkey < 100) a " +
                        "RIGHT OUTER JOIN " +
                        "(SELECT * FROM orders WHERE orderkey < 100) b ON a.orderkey = b.orderkey");

        assertQuery(
                "SELECT DISTINCT a.orderkey FROM " +
                        "(SELECT CASE WHEN orderkey > 2 THEN orderkey END orderkey FROM orders WHERE orderkey < 100) a " +
                        "FULL OUTER JOIN " +
                        "(SELECT * FROM orders WHERE orderkey < 100) b ON a.orderkey = b.orderkey",
                "SELECT DISTINCT orderkey FROM (" +
                        "SELECT a.orderkey FROM " +
                        "(SELECT CASE WHEN orderkey > 2 THEN orderkey END orderkey FROM orders WHERE orderkey < 100) a " +
                        "RIGHT OUTER JOIN " +
                        "(SELECT * FROM orders WHERE orderkey < 100) b ON a.orderkey = b.orderkey " +
                        "UNION ALL " +
                        "SELECT a.orderkey FROM" +
                        "(SELECT CASE WHEN orderkey > 2 THEN orderkey END orderkey FROM orders WHERE orderkey < 100) a " +
                        "LEFT OUTER JOIN " +
                        "(SELECT * FROM orders WHERE orderkey < 100) b ON a.orderkey = b.orderkey " +
                        "WHERE a.orderkey IS NULL)");
    }

    @Test
    public void testOuterJoinWithCommonExpression()
    {
        MaterializedResult actual = computeActual("SELECT count(1), count(one) " +
                "FROM (values (1, 'a'), (2, 'a')) as l(k, a) " +
                "LEFT JOIN (select k, 1 one from (values 1) as r(k)) r " +
                "ON l.k = r.k GROUP BY a");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(2L, 1L) // (total rows, # of non null values)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testSimpleLeftJoin()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testLeftJoinNormalizedToInner()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey WHERE orders.orderkey IS NOT NULL");
    }

    @Test
    public void testLeftJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testLeftJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN orders ON orders.orderkey = 1024");
    }

    @Test
    public void testSimpleLeftJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleLeftJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2");
    }

    @Test
    public void testDoubleFilteredLeftJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON orders.orderkey = 1024");
    }

    @Test
    public void testDoubleFilteredLeftJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testLeftJoinDoubleClauseWithLeftOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testLeftJoinDoubleClauseWithRightOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey");
    }

    @Test
    public void testBuildFilteredLeftJoin()
    {
        assertQuery("SELECT * FROM lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 2 = 0) a ON lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testProbeFilteredLeftJoin()
    {
        assertQuery("SELECT * FROM (SELECT * FROM lineitem WHERE orderkey % 2 = 0) a LEFT JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testLeftJoinPredicateMoveAround()
    {
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem\n" +
                "LEFT JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders\n" +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8\n" +
                "WHERE (orders.custkey % 8 < 7 OR orders.custkey % 8 IS NULL) AND orders.custkey % 8 = lineitem.orderkey % 8");
    }

    @Test
    public void testLeftJoinEqualityInference()
    {
        // Test that we can infer orders.orderkey % 4 = orders.custkey % 3 on the inner side
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM lineitem WHERE orderkey % 4 = 0 AND suppkey % 2 = partkey % 2 AND linenumber % 3 = orderkey % 3) lineitem\n" +
                "LEFT JOIN (SELECT * FROM orders WHERE orderkey % 4 = 0) orders\n" +
                "ON lineitem.linenumber % 3 = orders.orderkey % 4 AND lineitem.orderkey % 3 = orders.custkey % 3\n" +
                "WHERE lineitem.suppkey % 2 = lineitem.linenumber % 3");
    }

    @Test
    public void testLeftJoinWithNullValues()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 512 = 0\n" +
                ") AS lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT CASE WHEN orderkey % 2 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders\n" +
                "  WHERE custkey % 512 = 0\n" +
                ") AS orders\n" +
                "ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testSimpleRightJoin()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey");

        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.custkey");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testRightJoinNormalizedToInner()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey WHERE lineitem.orderkey IS NOT NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.custkey WHERE lineitem.orderkey IS NOT NULL");
    }

    @Test
    public void testRightJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testRightJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN orders ON orders.orderkey = 1024");
    }

    @Test
    public void testDoubleFilteredRightJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON orders.orderkey = 1024");
    }

    @Test
    public void testDoubleFilteredRightJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testSimpleRightJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleRightJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2");
    }

    @Test
    public void testRightJoinDoubleClauseWithLeftOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testRightJoinDoubleClauseWithRightOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey");
    }

    @Test
    public void testBuildFilteredRightJoin()
    {
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM (SELECT * FROM lineitem WHERE orderkey % 2 = 0) a RIGHT JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testProbeFilteredRightJoin()
    {
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM lineitem RIGHT JOIN (SELECT *  FROM orders WHERE orderkey % 2 = 0) a ON lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testRightJoinPredicateMoveAround()
    {
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders\n" +
                "RIGHT JOIN (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem\n" +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8\n" +
                "WHERE (orders.custkey % 8 < 7 OR orders.custkey % 8 IS NULL) AND orders.custkey % 8 = lineitem.orderkey % 8");
    }

    @Test
    public void testRightJoinEqualityInference()
    {
        // Test that we can infer orders.orderkey % 4 = orders.custkey % 3 on the inner side
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM orders WHERE orderkey % 4 = 0) orders\n" +
                "RIGHT JOIN (SELECT * FROM lineitem WHERE orderkey % 4 = 0 AND suppkey % 2 = partkey % 2 AND linenumber % 3 = orderkey % 3) lineitem\n" +
                "ON lineitem.linenumber % 3 = orders.orderkey % 4 AND lineitem.orderkey % 3 = orders.custkey % 3\n" +
                "WHERE lineitem.suppkey % 2 = lineitem.linenumber % 3");
    }

    @Test
    public void testRightJoinWithNullValues()
    {
        assertQuery("" +
                "SELECT lineitem.orderkey, orders.orderkey\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 512 = 0\n" +
                ") AS lineitem \n" +
                "RIGHT JOIN (\n" +
                "  SELECT CASE WHEN orderkey % 2 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders\n" +
                "  WHERE custkey % 512 = 0\n" +
                ") AS orders\n" +
                "ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testJoinWithDuplicateRelations()
    {
        assertQuery("SELECT * FROM orders JOIN orders USING (orderkey)", "SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey = o2.orderkey");
        assertQuery("SELECT * FROM lineitem x JOIN orders x USING (orderkey)", "SELECT * FROM lineitem l JOIN orders o ON l.orderkey = o.orderkey");
    }

    @Test
    public void testGroupByAsJoinProbe()
    {
        // we join on customer key instead of order key because
        // orders is effectively distributed on order key due the
        // generated data being sorted
        assertQuery("SELECT " +
                "  b.orderkey, " +
                "  b.custkey, " +
                "  a.custkey " +
                "FROM ( " +
                "  SELECT custkey" +
                "  FROM orders " +
                "  GROUP BY custkey" +
                ") a " +
                "JOIN orders b " +
                "  ON a.custkey = b.custkey ");
    }

    @Test(enabled = false)
    public void testAssignUniqueId()
    {
        // TODO: disabled until https://github.com/prestodb/presto/issues/8926 is resolved
        //       due to long running query test created many spill files on disk.
        String unionLineitem50Times = IntStream.range(0, 50)
                .mapToObj(i -> "SELECT * FROM lineitem")
                .collect(joining(" UNION ALL "));

        assertQuery(
                "SELECT count(*) FROM (" +
                        "SELECT * FROM (" +
                        "   SELECT (SELECT count(*) WHERE c = 1) " +
                        "   FROM (SELECT CASE orderkey WHEN 1 THEN orderkey ELSE 1 END " +
                        "       FROM (" + unionLineitem50Times + ")) o(c)) result(a) " +
                        "WHERE a = 1)",
                "VALUES 3008750");
    }
}
