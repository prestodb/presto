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
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_NULLS_IN_JOINS;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.tests.QueryTemplate.parameter;
import static com.facebook.presto.tests.QueryTemplate.queryTemplate;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestJoinQueries
        extends AbstractTestQueryFramework
{
    @Test
    public void testRowFieldAccessorInJoin()
    {
        assertQuery("" +
                        "SELECT t.a.col1, custkey, orderkey FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1, 11) AS ROW(col0 integer, col1 integer))), " +
                        "ROW(CAST(ROW(2, 22) AS ROW(col0 integer, col1 integer))), " +
                        "ROW(CAST(ROW(3, 33) AS ROW(col0 integer, col1 integer)))) t(a) " +
                        "INNER JOIN orders " +
                        "ON t.a.col0 = orders.orderkey",
                "SELECT * FROM VALUES (11, 370, 1), (22, 781, 2), (33, 1234, 3)");
    }

    @Test
    public void testJoinWithMultiFieldGroupBy()
    {
        assertQuery("SELECT orderstatus FROM lineitem JOIN (SELECT DISTINCT orderkey, orderstatus FROM orders) T on lineitem.orderkey = T.orderkey");
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

    // Disable since the test is flaky
    @Test(enabled = false)
    public void testLimitWithJoin()
    {
        MaterializedResult actual = computeActual("SELECT o1.orderkey, o2.orderkey FROM orders o1 JOIN orders o2 on o1.orderkey = o2.orderkey LIMIT 10");
        MaterializedResult all = computeActual("SELECT o1.orderkey, o2.orderkey FROM orders o1 JOIN  orders o2 on o1.orderkey = o2.orderkey");

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);

        actual = computeActual("SELECT o1.orderkey, o2.orderkey FROM orders o1 LEFT OUTER JOIN orders o2 on o1.orderkey = o2.orderkey LIMIT 10");
        all = computeActual("SELECT o1.orderkey, o2.orderkey FROM orders o1 LEFT OUTER JOIN orders o2 on o1.orderkey = o2.orderkey");

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);

        actual = computeActual("SELECT o1.orderkey, o2.orderkey FROM orders o1 RIGHT OUTER JOIN orders o2 on o1.orderkey = o2.orderkey LIMIT 10");
        all = computeActual("SELECT o1.orderkey, o2.orderkey FROM orders o1 RIGHT OUTER JOIN orders o2 on o1.orderkey = o2.orderkey");

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);

        actual = computeActual("SELECT o1.orderkey, o2.orderkey FROM orders o1 FULL OUTER JOIN orders o2 on o1.orderkey = o2.orderkey LIMIT 10");
        all = computeActual("SELECT o1.orderkey, o2.orderkey FROM orders o1 FULL OUTER JOIN orders o2 on o1.orderkey = o2.orderkey");

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testJoinCoercion()
    {
        assertQuery("SELECT COUNT(*) FROM orders t JOIN (SELECT * FROM orders LIMIT 1) t2 ON sin(t2.custkey) = 0");
    }

    @Test
    public void testJoinCoercionOnEqualityComparison()
    {
        assertQuery("SELECT o.clerk, avg(o.shippriority), COUNT(l.linenumber) FROM orders o LEFT OUTER JOIN lineitem l ON o.orderkey=l.orderkey AND o.shippriority=1 GROUP BY o.clerk");
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

        assertQuery("SELECT * FROM " +
                        "(VALUES (1,1),(2,1)) t1(a,b), " +
                        "(VALUES (1,1),(1,2),(2,1)) t2(x,y) " +
                        "WHERE a=x and b<=y",
                "VALUES (1,1,1,1), (1,1,1,2), (2,1,2,1)");

        assertQuery("SELECT * FROM " +
                        "(VALUES (1,1),(2,1)) t1(a,b), " +
                        "(VALUES (1,1),(1,2),(2,1)) t2(x,y) " +
                        "WHERE a=x and b<y",
                "VALUES (1,1,1,2)");
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

        assertQuery("SELECT * FROM " +
                        "(VALUES (1,1),(2,1)) t1(a,b), " +
                        "(VALUES (1,1),(1,2),(2,1)) t2(x,y) " +
                        "WHERE a=x and b>=y",
                "VALUES (1,1,1,1), (2,1,2,1)");

        assertQuery("SELECT * FROM " +
                        "(VALUES (1,1),(2,1)) t1(a,b), " +
                        "(VALUES (1,1),(1,2),(2,1)) t2(x,y) " +
                        "WHERE a=x and b>y",
                "SELECT 1 WHERE FALSE");
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
    public void testSimpleJoin()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey");
        assertQuery("" +
                "SELECT COUNT(*) FROM " +
                "(SELECT orderkey FROM lineitem WHERE orderkey < 1000) a " +
                "JOIN " +
                "(SELECT orderkey FROM orders WHERE orderkey < 2000) b " +
                "ON NOT (a.orderkey <= b.orderkey)");
    }

    @Test
    public void testJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = 2");
    }

    @Test
    public void testJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON orders.orderkey = 2");
    }

    @Test
    public void testSimpleJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2");
    }

    @Test
    public void testJoinDoubleClauseWithLeftOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testJoinDoubleClauseWithRightOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey");
    }

    @Test
    public void testJoinWithAlias()
    {
        assertQuery("SELECT * FROM (lineitem JOIN orders ON lineitem.orderkey = orders.orderkey) x");
    }

    @Test
    public void testJoinWithConstantExpression()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND 123 = 123");
    }

    @Test
    public void testJoinWithConstantTrueExpressionWithCoercion()
    {
        // Covers #7520
        assertQuery("SELECT count(*) > 0 FROM nation JOIN region ON (cast(1.2 AS real) = CAST(1.2 AS decimal(2,1)))");
    }

    @Test
    public void testJoinWithCanonicalizedConstantTrueExpressionWithCoercion()
    {
        // Covers #7520
        assertQuery("SELECT count(*) > 0 FROM nation JOIN region ON CAST((CASE WHEN (TRUE IS NOT NULL) THEN '1.2' ELSE '1.2' END) AS real) = CAST(1.2 AS decimal(2,1))");
    }

    @Test
    public void testJoinWithConstantPredicatePushDown()
    {
        assertQuery("" +
                "SELECT\n" +
                "  a.orderstatus\n" +
                "  , a.clerk\n" +
                "FROM (\n" +
                "  SELECT DISTINCT orderstatus, clerk FROM orders\n" +
                ") a\n" +
                "INNER JOIN (\n" +
                "  SELECT DISTINCT orderstatus, clerk FROM orders\n" +
                ") b\n" +
                "ON\n" +
                "  a.orderstatus = b.orderstatus\n" +
                "  and a.clerk = b.clerk\n" +
                "where a.orderstatus = 'F'\n");
    }

    @Test
    public void testJoinWithInferredFalseJoinClause()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM orders\n" +
                "JOIN lineitem\n" +
                "ON CAST(orders.orderkey AS VARCHAR) = CAST(lineitem.orderkey AS VARCHAR)\n" +
                "WHERE orders.orderkey = 1 AND lineitem.orderkey = 2\n");
    }

    @Test
    public void testJoinUsing()
    {
        assertQuery(
                "SELECT COUNT(*) FROM lineitem JOIN orders USING (orderkey)",
                "SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testJoinCriteriaCoercion()
    {
        assertQuery(
                "SELECT * FROM (VALUES (1.0, 2.0)) x (a, b) JOIN (VALUES (1, 3)) y (a, b) ON x.a = y.a",
                "VALUES (1.0, 2.0, 1, 3)");
        assertQuery(
                "SELECT * FROM (VALUES (1, 2)) x (a, b) JOIN (VALUES (SMALLINT '1', SMALLINT '3')) y (a, b) ON x.a = y.a",
                "VALUES (1, 2, 1, 3)");

        // short decimal, long decimal
        assertQuery(
                format("SELECT * FROM " +
                        "   (VALUES (CAST(1 AS DECIMAL(%1$d,0)), 2)) x (a, b) , " +
                        "   (VALUES (CAST(0 AS DECIMAL(%1$d,0)), SMALLINT '3')) y (a, b) " +
                        " WHERE x.a = y.a + 1", Decimals.MAX_SHORT_PRECISION),
                "VALUES (1, 2, 0, 3)");
        assertQuery(
                format("SELECT * FROM " +
                        "   (VALUES (CAST(1 AS DECIMAL(%1$d,0)), 2)) x (a, b) " +
                        "   INNER JOIN " +
                        "   (VALUES (CAST(0 AS DECIMAL(%1$d,0)), SMALLINT '3')) y (a, b) " +
                        "   ON x.a = y.a + 1", Decimals.MAX_SHORT_PRECISION),
                "VALUES (1, 2, 0, 3)");
        assertQuery(
                format("SELECT * FROM " +
                        "   (VALUES (CAST(1 AS DECIMAL(%1$d,0)), 2)) x (a, b) " +
                        "   LEFT JOIN (VALUES (CAST(0 AS DECIMAL(%1$d,0)), SMALLINT '3')) y (a, b) " +
                        "   ON x.a = y.a + 1", Decimals.MAX_SHORT_PRECISION),
                "VALUES (1, 2, 0, 3)");
        assertQuery(
                format("SELECT * FROM " +
                        "   (VALUES CAST(1 AS decimal(%d,0))) t1 (a), " +
                        "   (VALUES CAST(1 AS decimal(%d,0))) t2 (b) " +
                        "   WHERE a = b", Decimals.MAX_SHORT_PRECISION, Decimals.MAX_SHORT_PRECISION + 1),
                "VALUES (1, 1)");
    }

    @Test
    public void testJoinWithReversedComparison()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON orders.orderkey = lineitem.orderkey");
    }

    @Test
    public void testJoinWithComplexExpressions()
    {
        assertQuery("SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey = CAST(orders.orderkey AS BIGINT)");
    }

    @Test
    public void testJoinWithComplexExpressions2()
    {
        assertQuery(
                "SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey = CASE WHEN orders.custkey = 1 and orders.orderstatus = 'F' THEN orders.orderkey ELSE NULL END");
    }

    @Test
    public void testJoinWithComplexExpressions3()
    {
        assertQuery(
                "SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey + 1 = orders.orderkey + 1",
                // H2 takes a million years because it can't join efficiently on a non-indexed field/expression
                "SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey ");
    }

    @Test
    public void testJoinWithNormalization()
    {
        assertQuery("SELECT COUNT(*) FROM nation a JOIN nation b on not ((a.nationkey + b.nationkey) <> b.nationkey)");
        assertQuery("SELECT COUNT(*) FROM nation a JOIN nation b on not (a.nationkey <> b.nationkey)");
        assertQuery("SELECT COUNT(*) FROM nation a JOIN nation b on not (a.nationkey = b.nationkey)");
        assertQuery("SELECT COUNT(*) FROM nation a JOIN nation b on not (not CAST(a.nationkey AS boolean))");
        assertQuery("SELECT COUNT(*) FROM nation a JOIN nation b on not not not (a.nationkey = b.nationkey)");
    }

    @Test
    public void testSelfJoin()
    {
        assertQuery("SELECT COUNT(*) FROM orders a JOIN orders b on a.orderkey = b.orderkey");
    }

    @Test
    public void testWildcardFromJoin()
    {
        assertQuery(
                "SELECT * FROM (SELECT orderkey, partkey FROM lineitem) a JOIN (SELECT orderkey, custkey FROM orders) b using (orderkey)",
                "SELECT a.orderkey, a.partkey, b.custkey FROM (SELECT orderkey, partkey FROM lineitem) a JOIN (SELECT orderkey, custkey FROM orders) b on a.orderkey = b.orderkey");
    }

    @Test
    public void testQualifiedWildcardFromJoin()
    {
        assertQuery(
                "SELECT a.*, b.* FROM (SELECT orderkey, partkey FROM lineitem) a JOIN (SELECT orderkey, custkey FROM orders) b using (orderkey)",
                "SELECT a.partkey, b.custkey FROM (SELECT orderkey, partkey FROM lineitem) a JOIN (SELECT orderkey, custkey FROM orders) b on a.orderkey = b.orderkey");
    }

    @Test
    public void testJoinAggregations()
    {
        assertQuery(
                "SELECT x + y FROM (" +
                        "   SELECT orderdate, COUNT(*) x FROM orders GROUP BY orderdate) a JOIN (" +
                        "   SELECT orderdate, COUNT(*) y FROM orders GROUP BY orderdate) b ON a.orderdate = b.orderdate");
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
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2, 11), (2, 10)");
        assertQuery(
                "SELECT COUNT(*) FROM (VALUES 1, 2) t1(a) JOIN (VALUES 10, 11) t2(b) ON a > 2",
                "VALUES (0)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) JOIN (VALUES 10, 11) t2(b) ON a+9 > b",
                "VALUES (2, 10)");
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
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > d",
                "VALUES (1, 2, 1, 1), (1, 1, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b < d",
                "VALUES (1, 1, 1, 2), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 2",
                "VALUES (1, 1, NULL,  NULL), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 2",
                "VALUES (1, 1, NULL, NULL), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND c = d",
                "VALUES (1, 1, 1, 1), (1, 2, 1, 1)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND c < d",
                "VALUES (1, 1, 1, 2), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON c = d",
                "VALUES (1, 1, 1, 1), (1, 2, 1, 1)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON c < d",
                "VALUES (1, 1, 1, 2), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON 1 = 1",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (1, NULL), (2, 11), (2, 10)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (1, 11), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON a > b",
                "VALUES (1, NULL), (2, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON a < b",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");

        assertQuery(
                "SELECT * FROM (VALUES 1) t1(a) LEFT OUTER JOIN (VALUES (1,2,2), (1,2,3), (1, 2, NULL)) t2(x,y,z) ON a=x AND y = z",
                "VALUES (1, 1, 2, 2)");

        // left join which gets converted to inner join without equality conditions.
        // all symbols pruned by original join
        assertQuery("SELECT 1 FROM (VALUES 1, 20) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON a > b WHERE b IS NOT NULL",
                "VALUES (1), (1)");
    }

    @Test
    public void testNonEqalityJoinWithScalarRequiringSessionParameter()
    {
        assertQuery("SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND from_unixtime(b) > current_timestamp",
                "VALUES (1, 1, NULL, NULL), (1, 2, NULL, NULL)");
    }

    @Test
    public void testNonEqualityJoinWithTryInFilter()
    {
        assertQuery("SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) " +
                        "             ON a=c AND TRY(1 / (b-a) != 1000)",
                "VALUES (1, 1, NULL, NULL), (1, 2, 1, 1), (1, 2, 1, 2)");

        // use of scalar requiring session parameter within try
        assertQuery("SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) " +
                        "             ON a=c AND TRY(1 / (b-a) != 1000 OR from_unixtime(b) > current_timestamp)",
                "VALUES (1, 1, NULL, NULL), (1, 2, 1, 1), (1, 2, 1, 2)");
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
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > d",
                "VALUES (1, 2, 1, 1), (NULL, NULL, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b < d",
                "VALUES (1, 1, 1, 2), (NULL, NULL, 1, 1)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 2",
                "VALUES (NULL, NULL, 1, 1), (NULL, NULL, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 2",
                "VALUES (NULL, NULL, 1, 1), (NULL, NULL, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND c = d",
                "VALUES (1, 2, 1, 1), (1, 1, 1, 1), (NULL, NULL, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND c < d",
                "VALUES (NULL, NULL, 1, 1), (1, 2, 1, 2), (1, 1, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON c = d",
                "VALUES (1, 1, 1, 1), (1, 2, 1, 1), (NULL, NULL, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON c < d",
                "VALUES (NULL, NULL, 1, 1), (1, 1, 1, 2), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON 1 = 1",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2, 11), (2, 10)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (NULL, 10), (1, 11), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON a > b",
                "VALUES (NULL, 10), (NULL, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON a < b",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");
    }

    @Test
    public void testJoinUsingSymbolsFromJustOneSideOfJoin()
    {
        assertQuery(
                "SELECT b FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (10), (11), (11)");
        assertQuery(
                "SELECT a FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2), (2)");
        assertQuery(
                "SELECT b FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (11), (11)");
        assertQuery(
                "SELECT a FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (1), (2), (2)");
        assertQuery(
                "SELECT a FROM (VALUES 1, 2) t1(a) JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2), (2)");
        assertQuery(
                "SELECT b FROM (VALUES 1, 2) t1(a) JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (11), (11)");
    }

    @Test
    public void testJoinsWithTrueJoinCondition()
    {
        // inner join
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (0, 10), (0, 11), (1, 10), (1, 11)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");

        // left join
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) LEFT JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (0, 10), (0, 11), (1, 10), (1, 11)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) LEFT JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) LEFT JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "VALUES (0, NULL), (1, NULL)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) LEFT JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");

        // right join
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) RIGHT JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (0, 10), (0, 11), (1, 10), (1, 11)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) RIGHT JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (NULL, 10), (NULL, 11)");
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) RIGHT JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) RIGHT JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");

        // full join
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) FULL JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (0, 10), (0, 11), (1, 10), (1, 11)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) FULL JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (NULL, 10), (NULL, 11)");
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) FULL JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "VALUES (0, NULL), (1, NULL)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) FULL JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");
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
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > d",
                "VALUES (1, 2, 1, 1), (NULL, NULL, 1, 2), (1, 1, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b < d",
                "VALUES (1, 1, 1, 2), (NULL, NULL, 1, 1), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 2",
                "VALUES (NULL, NULL, 1, 1), (NULL, NULL, 1, 2), (1, 1, NULL, NULL), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 2",
                "VALUES (NULL, NULL, 1, 1), (NULL, NULL, 1, 2), (1, 1, NULL, NULL), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2, 11), (2, 10), (1, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (NULL, 10), (1, 11), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON a > b",
                "VALUES (NULL, 10), (NULL, 11), (1, NULL), (2, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON a < b",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");
    }

    @Test
    public void testJoinOnMultipleFields()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.shipdate = orders.orderdate");
    }

    @Test
    public void testJoinUsingMultipleFields()
    {
        assertQuery(
                "SELECT COUNT(*) FROM lineitem JOIN (SELECT orderkey, orderdate shipdate FROM orders) T USING (orderkey, shipdate)",
                "SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.shipdate = orders.orderdate");
    }

    @Test
    public void testColocatedJoinWithLocalUnion()
    {
        assertQuery(
                "SELECT count(*) FROM ((SELECT * FROM orders) union all (SELECT * FROM orders)) JOIN orders USING (orderkey)",
                "SELECT 2 * count(*) FROM orders");
    }

    @Test
    public void testJoinWithNonJoinExpression()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey = 1");
    }

    @Test
    public void testJoinWithNullValues()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 512 = 0\n" +
                ") AS lineitem \n" +
                "JOIN (\n" +
                "  SELECT CASE WHEN orderkey % 2 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders\n" +
                "  WHERE custkey % 512 = 0\n" +
                ") AS orders\n" +
                "ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testJoinWithMultipleInSubqueryClauses()
    {
        QueryTemplate.Parameter type = parameter("type").of("");
        QueryTemplate.Parameter condition = parameter("condition").of("true");
        QueryTemplate queryTemplate = queryTemplate(
                "SELECT * FROM (VALUES 1,2,3,4) t(x) %type% JOIN (VALUES 1,2,3,5) t2(y) ON %condition%",
                type,
                condition);

        QueryTemplate.Parameter twoDuplicatedInSubqueriesCondition = condition.of(
                "(x in (VALUES 1,2,3)) = (y in (VALUES 1,2,3)) AND (x in (VALUES 1,2,4)) = (y in (VALUES 1,2,4))");
        assertQuery(
                queryTemplate.replace(twoDuplicatedInSubqueriesCondition),
                "VALUES (1,1), (1,2), (2,2), (2,1), (3,3)");
        assertQuery(
                queryTemplate.replace(condition.of("(x in (VALUES 1,2)) = (y in (VALUES 1,2)) AND (x in (VALUES 1)) = (y in (VALUES 3))")),
                "VALUES (2,2), (2,1), (3,5), (4,5)");
        assertQuery(
                queryTemplate.replace(condition.of("(x in (VALUES 1,2)) = (y in (VALUES 1,2)) AND (x in (VALUES 1)) != (y in (VALUES 3))")),
                "VALUES (1,2), (1,1), (3, 3), (4,3)");
        assertQuery(
                queryTemplate.replace(condition.of("(x in (VALUES 1)) = (y in (VALUES 1)) AND (x in (SELECT 2)) != (y in (SELECT 2))")),
                "VALUES (2,3), (2,5), (3, 2), (4,2)");

        QueryTemplate.Parameter left = type.of("left");
        QueryTemplate.Parameter right = type.of("right");
        QueryTemplate.Parameter full = type.of("full");
        for (QueryTemplate.Parameter joinType : ImmutableList.of(left, right, full)) {
            for (String joinCondition : ImmutableList.of("x IN (VALUES 1)", "y in (VALUES 1)")) {
                assertQueryFails(
                        queryTemplate.replace(joinType, condition.of(joinCondition)),
                        ".*IN with subquery predicate in join condition is not supported");
            }
        }

        assertQuery(
                queryTemplate.replace(left, twoDuplicatedInSubqueriesCondition),
                "VALUES (1,1), (1,2), (2,2), (2,1), (3,3), (4, null)");
        assertQuery(
                queryTemplate.replace(right, twoDuplicatedInSubqueriesCondition),
                "VALUES (1,1), (1,2), (2,2), (2,1), (3,3), (null, 5)");
        assertQuery(
                queryTemplate.replace(full, twoDuplicatedInSubqueriesCondition),
                "VALUES (1,1), (1,2), (2,2), (2,1), (3,3), (4, null), (null, 5)");
    }

    @Test
    public void testJoinWithInSubqueryToBeExecutedAsPostJoinFilter()
    {
        QueryTemplate.Parameter type = parameter("type").of("");
        QueryTemplate.Parameter condition = parameter("condition").of("true");
        QueryTemplate queryTemplate = queryTemplate(
                "SELECT * FROM (VALUES 1,2,3,4) t(x) %type% JOIN (VALUES 1,2,3,5) t2(y) ON %condition%",
                type,
                condition);

        assertQuery(
                queryTemplate.replace(condition.of("(x+y in (VALUES 4))")),
                "VALUES (1,3), (2,2), (3,1)");
        assertQuery(
                queryTemplate.replace(condition.of("(x+y in (VALUES 4)) AND (x*y in (VALUES 4,5))")),
                "VALUES (2,2)");
        assertQuery(
                queryTemplate.replace(condition.of("(x+y in (VALUES 4,5)) AND (x*y IN (VALUES 4,5))")),
                "VALUES (4,1), (2,2)");
        assertQuery(
                queryTemplate.replace(condition.of("(x+y in (VALUES 4,5)) AND (x in (VALUES 4,5)) != (y in (VALUES 4,5))")),
                "VALUES (4,1)");

        for (QueryTemplate.Parameter joinType : type.of("left", "right", "full")) {
            assertQueryFails(
                    queryTemplate.replace(
                            joinType,
                            condition.of("(x+y in (VALUES 4,5)) AND (x in (VALUES 4,5)) != (y in (VALUES 4,5))")),
                    ".*IN with subquery predicate in join condition is not supported");
        }
    }

    @Test
    public void testOuterJoinWithComplexCorrelatedSubquery()
    {
        QueryTemplate.Parameter type = parameter("type");
        QueryTemplate.Parameter condition = parameter("condition");
        QueryTemplate queryTemplate = queryTemplate(
                "SELECT * FROM (VALUES 1,2,3,4) t(x) %type% JOIN (VALUES 1,2,3,5) t2(y) ON %condition%",
                type,
                condition);

        queryTemplate.replaceAll(
                (query) -> assertQueryFails(query, "line .*: .* is not supported"),
                ImmutableList.of(type.of("left"), type.of("right"), type.of("full")),
                ImmutableList.of(
                        condition.of("EXISTS(SELECT 1 WHERE x = y)"),
                        condition.of("(SELECT x = y)"),
                        condition.of("true IN (SELECT x = y)")));
    }

    @Test
    public void testJoinWithMultipleScalarSubqueryClauses()
    {
        QueryTemplate.Parameter type = parameter("type").of("");
        QueryTemplate.Parameter condition = parameter("condition");
        QueryTemplate queryTemplate = queryTemplate(
                "SELECT * FROM (VALUES 1,2,3,4) t(x) %type% JOIN (VALUES 1,2,3,5) t2(y) ON %condition%",
                type,
                condition);

        QueryTemplate.Parameter multipleScalarJoinCondition =
                condition.of("(x = (VALUES 1)) AND (y = (VALUES 2)) AND (x in (VALUES 2)) = (y in (VALUES 1))");
        assertQuery(queryTemplate.replace(multipleScalarJoinCondition), "VALUES (1,2)");
        assertQuery(
                queryTemplate.replace(condition.of("(x = (VALUES 2)) = (y > (VALUES 0)) AND (x > (VALUES 1)) = (y < (VALUES 3))")),
                "VALUES (2,2), (2,1)");
        assertQuery(
                queryTemplate.replace(condition.of("(x = (VALUES 1)) = (y = (VALUES 1)) AND (x = (SELECT 2)) != (y = (SELECT 3))")),
                "VALUES (2,5), (2,2), (3,3), (4,3)");

        assertQuery(
                queryTemplate.replace(type.of("left"), multipleScalarJoinCondition),
                "VALUES (1,2), (2,null), (3, null), (4, null)");
        assertQuery(
                queryTemplate.replace(type.of("right"), multipleScalarJoinCondition),
                "VALUES (1,2), (null,1), (null, 3), (null, 5)");
        assertQuery(
                queryTemplate.replace(type.of("full"), multipleScalarJoinCondition),
                "VALUES (1,2), (2,null), (3, null), (4, null), (null,1), (null, 3), (null, 5)");
    }

    @Test
    public void testJoinWithScalarSubqueryToBeExecutedAsPostJoinFilter()
    {
        QueryTemplate.Parameter type = parameter("type").of("");
        QueryTemplate.Parameter condition = parameter("condition");
        QueryTemplate queryTemplate = queryTemplate(
                "SELECT * FROM (VALUES 1,2,3,4) t(x) %type% JOIN (VALUES 1,2,3,5) t2(y) ON %condition%",
                type,
                condition);

        QueryTemplate.Parameter xPlusYEqualsSubqueryJoinCondition = condition.of("(x+y = (SELECT 4))");
        assertQuery(
                queryTemplate.replace(xPlusYEqualsSubqueryJoinCondition),
                "VALUES (1,3), (2,2), (3,1)");
        assertQuery(queryTemplate.replace(condition.of("(x+y = (VALUES 4)) AND (x*y = (VALUES 4))")), "VALUES (2,2)");

        // all combination of duplicated subquery
        assertQuery(
                queryTemplate.replace(condition.of("x+y > (VALUES 3) AND (x = (VALUES 3)) != (y = (VALUES 3))")),
                "VALUES (3,1), (3,2), (1,3), (2,3), (4,3), (3,5)");
        assertQuery(
                queryTemplate.replace(condition.of("x+y >= (VALUES 5) AND (x = (VALUES 3)) != (y = (VALUES 3))")),
                "VALUES (3,2), (2,3), (4,3), (3,5)");
        assertQuery(
                queryTemplate.replace(condition.of("x+y >= (VALUES 3) AND (x = (VALUES 5)) != (y = (VALUES 3))")),
                "VALUES (1,3), (2,3), (3,3), (4,3)");
        assertQuery(
                queryTemplate.replace(condition.of("x+y >= (VALUES 3) AND (x = (VALUES 3)) != (y = (VALUES 5))")),
                "VALUES (3,1), (3,2), (3,3), (1,5), (2,5), (4,5)");
        assertQuery(
                queryTemplate.replace(condition.of("x+y >= (VALUES 4) AND (x = (VALUES 3)) != (y = (VALUES 5))")),
                "VALUES (3,1), (3,2), (3,3), (1,5), (2,5), (4,5)");

        // non inner joins
        assertQuery(
                queryTemplate.replace(type.of("left"), xPlusYEqualsSubqueryJoinCondition),
                "VALUES (1,3), (2,2), (3,1), (4, null)");
        assertQuery(
                queryTemplate.replace(type.of("right"), xPlusYEqualsSubqueryJoinCondition),
                "VALUES (1,3), (2,2), (3,1), (null, 5)");
        assertQuery(
                queryTemplate.replace(type.of("full"), xPlusYEqualsSubqueryJoinCondition),
                "VALUES (1,3), (2,2), (3,1), (4, null), (null, 5)");
    }

    @Test
    public void testJoinWithScalarSubqueryInOnClause()
    {
        assertQuery(
                "SELECT count() FROM nation a" +
                        " INNER JOIN nation b ON a.name = (SELECT max(name) FROM nation)" +
                        " INNER JOIN nation c ON c.name = split_part(b.name,'<',2)",
                "SELECT 0");
    }

    @Test
    public void testJoinWithScalarSubqueryToBeExecutedAsPostJoinFilterWithEmptyInnerTable()
    {
        String noOutputQuery = "SELECT 1 WHERE false";
        QueryTemplate.Parameter type = parameter("type").of("");
        QueryTemplate.Parameter condition = parameter("condition");
        QueryTemplate queryTemplate = queryTemplate(
                "SELECT * FROM (" + noOutputQuery + ") t(x) %type% JOIN (VALUES 1) t2(y) ON %condition%",
                type);

        QueryTemplate.Parameter xPlusYEqualsSubqueryJoinCondition = condition.of("(x+y = (SELECT 4))");
        assertQuery(queryTemplate.replace(xPlusYEqualsSubqueryJoinCondition), noOutputQuery);
        assertQuery(queryTemplate.replace(condition.of("(x+y = (VALUES 4)) AND (x*y = (VALUES 4))")), noOutputQuery);

        // non inner joins
        assertQuery(queryTemplate.replace(xPlusYEqualsSubqueryJoinCondition, type.of("left")), noOutputQuery);
        assertQuery(queryTemplate.replace(xPlusYEqualsSubqueryJoinCondition, type.of("right")), "VALUES (null,1)");
        assertQuery(queryTemplate.replace(xPlusYEqualsSubqueryJoinCondition, type.of("full")), "VALUES (null,1)");
    }

    @Test
    public void testJoinWithExpressionsThatMayReturnNull()
    {
        assertQuery("" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "    SELECT a, nullif(a, 1)\n" +
                        "    FROM (VALUES 1) w(a)\n" +
                        ") t(a,b)\n" +
                        "JOIN (VALUES 1) u(x) ON t.a = u.x",
                "SELECT 1, NULL, 1");

        assertQuery("" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "    SELECT a, contains(array[2, null], a)\n" +
                        "    FROM (VALUES 1) w(a)\n" +
                        ") t(a,b)\n" +
                        "JOIN (VALUES 1) u(x) ON t.a = u.x\n",
                "SELECT 1, NULL, 1");

        assertQuery("" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "    SELECT a, array[null][a]\n" +
                        "    FROM (VALUES 1) w(a)\n" +
                        ") t(a,b)\n" +
                        "JOIN (VALUES 1) u(x) ON t.a = u.x",
                "SELECT 1, NULL, 1");

        assertQuery("" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "    SELECT a, try(a / 0)\n" +
                        "    FROM (VALUES 1) w(a)\n" +
                        ") t(a,b)\n" +
                        "JOIN (VALUES 1) u(x) ON t.a = u.x",
                "SELECT 1, NULL, 1");
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
                "FROM (values (1, 'a'), (2, 'a')) AS l(k, a) " +
                "LEFT JOIN (SELECT k, 1 one FROM (values 1) AS r(k)) r " +
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

        // With null base (null row value) in dereference expression
        assertQuery(
                "SELECT x.val FROM " +
                        "(SELECT CAST(ROW(v) AS ROW(val integer)) FROM (VALUES 1, 2, 3) t(v)) ta (x) " +
                        "LEFT OUTER JOIN " +
                        "(SELECT CAST(ROW(v) AS ROW(val integer)) FROM (VALUES 1, 2, 3) t(v)) tb (y) " +
                        "ON x.val=y.val " +
                        "WHERE y.val=1",
                "SELECT 1");
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
        assertQuery(noJoinReordering(), "SELECT * FROM lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 2 = 0) a ON lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testProbeFilteredLeftJoin()
    {
        assertQuery(noJoinReordering(), "SELECT * FROM (SELECT * FROM lineitem WHERE orderkey % 2 = 0) a LEFT JOIN orders ON a.orderkey = orders.orderkey");
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
        assertQuery(noJoinReordering(), "SELECT custkey, linestatus, tax, totalprice, orderstatus FROM (SELECT * FROM lineitem WHERE orderkey % 2 = 0) a RIGHT JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testProbeFilteredRightJoin()
    {
        assertQuery(noJoinReordering(), "SELECT custkey, linestatus, tax, totalprice, orderstatus FROM lineitem RIGHT JOIN (SELECT *  FROM orders WHERE orderkey % 2 = 0) a ON lineitem.orderkey = a.orderkey");
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
    public void testJoinWithStatefulFilterFunction()
    {
        assertQuery("SELECT *\n" +
                        "FROM (VALUES 1, 2) a(id)\n" +
                        "FULL JOIN (VALUES 2, 3) b(id)\n" +
                        "ON (array_intersect(array[a.id], array[b.id]) = array[a.id])",
                "VALUES (1, null), (2, 2), (null, 3)");
    }

    @Test
    public void testJoinWithGroupByAsProbe()
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

    @Test
    public void testJoinEffectivePredicateWithNoRanges()
    {
        assertQuery("" +
                "SELECT * FROM orders a " +
                "   JOIN (SELECT * FROM orders WHERE orderkey IS NULL) b " +
                "   ON a.orderkey = b.orderkey");
    }

    @Test
    public void testRowNumberJoin()
    {
        MaterializedResult actual = computeActual("SELECT a, rn\n" +
                "FROM (\n" +
                "    SELECT a, row_number() OVER (ORDER BY a) rn\n" +
                "    FROM (VALUES (1), (2)) t (a)\n" +
                ") a\n" +
                "JOIN (VALUES (2)) b (b) ON a.a = b.b\n" +
                "LIMIT 1");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(2, 2L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        actual = computeActual("SELECT a, rn\n" +
                "FROM (\n" +
                "    SELECT a, row_number() OVER (PARTITION BY a ORDER BY a) rn\n" +
                "    FROM (VALUES (1), (2), (1), (2)) t (a)\n" +
                ") a\n" +
                "JOIN (VALUES (2)) b (b) ON a.a = b.b\n" +
                "LIMIT 2");

        expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(2, 1L)
                .row(2, 2L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testJoinUnaliasedSubqueries()
    {
        assertQuery(
                "SELECT COUNT(*) FROM (SELECT * FROM lineitem) JOIN (SELECT * FROM orders) USING (orderkey)",
                "SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testWithSelfJoin()
    {
        assertQuery("" +
                "WITH x AS (SELECT DISTINCT orderkey FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "SELECT count(*) FROM x a JOIN x b USING (orderkey)", "" +
                "SELECT count(*)\n" +
                "FROM (SELECT DISTINCT orderkey FROM orders ORDER BY orderkey LIMIT 10) a\n" +
                "JOIN (SELECT DISTINCT orderkey FROM orders ORDER BY orderkey LIMIT 10) b ON a.orderkey = b.orderkey");
    }

    @Test
    public void testInSubqueryWithCrossJoin()
    {
        assertQuery("SELECT a FROM (VALUES (1),(2)) t(a) WHERE a IN " +
                "(SELECT b FROM (VALUES (ARRAY[2])) AS t1 (a) CROSS JOIN UNNEST(a) AS t2(b))", "SELECT 2");
    }

    @Test
    public void testJoinProjectionPushDown()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM\n" +
                "  (SELECT orderkey, abs(orderkey) a FROM orders) t\n" +
                "JOIN\n" +
                "  (SELECT orderkey, abs(orderkey) a FROM orders) u\n" +
                "ON\n" +
                "  t.orderkey = u.orderkey");
    }

    @Test
    public void testUnionWithJoin()
    {
        assertQuery(
                "SELECT * FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION ALL " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "JOIN orders o ON (a.orderkey = o.orderkey)");
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
    public void testUnionWithJoinOnNonTranslateableSymbols()
    {
        assertQuery("SELECT *\n" +
                "FROM (SELECT orderdate ds, orderkey\n" +
                "      FROM orders\n" +
                "      UNION ALL\n" +
                "      SELECT shipdate ds, orderkey\n" +
                "      FROM lineitem) a\n" +
                "JOIN orders o\n" +
                "ON (substr(cast(a.ds AS VARCHAR), 6, 2) = substr(cast(o.orderdate AS VARCHAR), 6, 2) AND a.orderkey = o.orderkey)");
    }

    @Test
    public void testRandCrossJoins()
    {
        assertQuery("" +
                "SELECT COUNT(*) " +
                "FROM (SELECT * FROM orders ORDER BY rand() LIMIT 5) a " +
                "CROSS JOIN (SELECT * FROM lineitem ORDER BY rand() LIMIT 5) b");
    }

    @Test
    public void testCrossJoins()
    {
        assertQuery("" +
                "SELECT a.custkey, b.orderkey " +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a " +
                "CROSS JOIN (SELECT * FROM lineitem ORDER BY orderkey LIMIT 5) b");
    }

    @Test
    public void testCrossJoinEmptyProbePage()
    {
        assertQuery("" +
                "SELECT a.custkey, b.orderkey " +
                "FROM (SELECT * FROM orders WHERE orderkey < 0) a " +
                "CROSS JOIN (SELECT * FROM lineitem WHERE orderkey < 100) b");
    }

    @Test
    public void testCrossJoinEmptyBuildPage()
    {
        assertQuery("" +
                "SELECT a.custkey, b.orderkey " +
                "FROM (SELECT * FROM orders WHERE orderkey < 100) a " +
                "CROSS JOIN (SELECT * FROM lineitem WHERE orderkey < 0) b");
    }

    @Test
    public void testSimpleCrossJoins()
    {
        assertQuery("SELECT * FROM (SELECT 1 a) x CROSS JOIN (SELECT 2 b) y");
    }

    @Test
    public void testCrossJoinsWithWhereClause()
    {
        assertQuery("" +
                        "SELECT a, b, c, d " +
                        "FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')) t1 (a, b) " +
                        "CROSS JOIN (VALUES (1, 1.1), (3, 3.3), (5, 5.5)) t2 (c, d) " +
                        "WHERE t1.a > t2.c",
                "SELECT * FROM (VALUES  (2, 'b', 1, 1.1), (3, 'c', 1, 1.1), (4, 'd', 1, 1.1), (4, 'd', 3, 3.3))");
    }

    @Test
    public void testCrossJoinsDifferentDataTypes()
    {
        assertQuery("" +
                "SELECT * " +
                "FROM (SELECT 'AAA' a1, 11 b1, 33.3 c1, true AS d1, 21 e1) x " +
                "CROSS JOIN (SELECT 4444.4 a2, false AS b2, 'BBB' c2, 22 d2) y");
    }

    @Test
    public void testCrossJoinWithNulls()
    {
        assertQuery("SELECT a, b FROM (VALUES (1), (2)) t (a) CROSS JOIN (VALUES (1), (3)) u (b)",
                "SELECT * FROM (VALUES  (1, 1), (1, 3), (2, 1), (2, 3))");
        assertQuery("SELECT a, b FROM (VALUES (1), (2), (null)) t (a), (VALUES (11), (null), (13)) u (b)",
                "SELECT * FROM (VALUES (1, 11), (1, null), (1, 13), (2, 11), (2, null), (2, 13), (null, 11), (null, null), (null, 13))");
        assertQuery("SELECT a, b FROM (VALUES ('AA'), ('BB'), (null)) t (a), (VALUES ('111'), (null), ('333')) u (b)",
                "SELECT * FROM (VALUES ('AA', '111'), ('AA', null), ('AA', '333'), ('BB', '111'), ('BB', null), ('BB', '333'), (null, '111'), (null, null), (null, '333'))");
    }

    @Test
    public void testImplicitCrossJoin()
    {
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 3) a, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 4) b");
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 2) b");
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 5) b, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 5) c ");

        // Inner Join converted to cross join because all join conditions are pushed down.
        assertQuery("" +
                "SELECT l.orderkey, l.linenumber " +
                "FROM orders o INNER JOIN lineitem l " +
                "ON o.custkey = l.linenumber " +
                "WHERE o.custkey IN (5) AND l.orderkey IN (7522)");

        assertQuery("" +
                "SELECT o.custkey " +
                "FROM orders o INNER JOIN lineitem l " +
                "ON o.custkey = l.linenumber " +
                "WHERE o.custkey IN (5) AND l.orderkey IN (7522)");

        assertQuery("" +
                "SELECT COUNT(*) " +
                "FROM orders o INNER JOIN lineitem l " +
                "ON o.custkey = l.linenumber " +
                "WHERE o.custkey IN (5) AND l.orderkey IN (7522)");
    }

    @Test
    public void testCrossJoinUnion()
    {
        assertQuery("" +
                "SELECT t.c " +
                "FROM (SELECT 1) " +
                "CROSS JOIN (SELECT 0 AS c UNION ALL SELECT 1) t");
        assertQuery("" +
                "SELECT a, b " +
                "FROM (VALUES (1, 1)) " +
                "CROSS JOIN (SELECT 0 AS a, 0 AS b UNION ALL SELECT 1, 1) t");
    }

    @Test
    public void testCrossJoinUnnestWithUnion()
    {
        assertQuery("" +
                        "SELECT col, COUNT(*)\n" +
                        "FROM ((\n" +
                        "    SELECT ARRAY[1, 2] AS a\n" +
                        "    UNION ALL\n" +
                        "    SELECT ARRAY[1, 3] AS a)  unionresult\n" +
                        "  CROSS JOIN UNNEST(unionresult.a) t(col))\n" +
                        "GROUP BY col",
                "SELECT * FROM VALUES (1, 2), (2, 1), (3, 1)");
    }

    @Test
    public void testJoinOnConstantExpression()
    {
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a " +
                "   JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) b " +
                "   ON 123 = 123");
    }

    @Test
    public void testSemiJoin()
    {
        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber " +
                "HAVING min(orderkey) IN (SELECT orderkey FROM orders WHERE orderkey > 1)");
        //
        // constant literal versus a subquery
        assertQuery("SELECT 10 in (SELECT orderkey FROM orders)");

        // the same IN subquery used twice
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (2,2), (3, 3)) t(x, y) WHERE (x+y in (VALUES 4, 5)) AND (x*y in (VALUES 4, 5))",
                "VALUES (2,2)");

        // test multiple IN subqueries with coercions
        assertQuery("SELECT 1.0 IN (SELECT 1), 1 IN (SELECT 1)");
        assertQuery("SELECT 1 WHERE 1 IN (SELECT 1) AND 1.0 IN (SELECT 1)");
        assertQuery("SELECT 1.0 in (values (1), (2), (3))", "SELECT true");

        // test IN subqueries with supertype coercions
        assertQuery("SELECT CAST(1 AS decimal(3,2)) IN (SELECT CAST(1 AS decimal(3,1)))", "SELECT true");
        assertQuery("SELECT CAST(1 AS decimal(3,2)) IN (values (cast(1 AS decimal(3,1))), (cast (2 AS decimal(3,1))))", "SELECT true");

        // test multi level IN subqueries
        assertQuery("SELECT 1 IN (SELECT 1), 2 IN (SELECT 1) WHERE 1 IN (SELECT 1)");

        // test with subqueries on left
        assertQuery("SELECT (SELECT 1) IN (SELECT 1)");
        assertQuery("SELECT (SELECT 2) IN (1, (SELECT 2))");
        assertQuery("SELECT (2 + (SELECT 1)) IN (SELECT 1)");
        assertQuery("SELECT (1 IN (SELECT 1)) IN (SELECT TRUE)");
        assertQuery("SELECT ((SELECT 1) IN (SELECT 1)) IN (SELECT TRUE)");
        assertQuery("SELECT (EXISTS(SELECT 1)) IN (SELECT TRUE)");
        assertQuery("SELECT (1 = ANY(SELECT 1)) IN (SELECT TRUE)");

        // Throw in a bunch of IN subquery predicates
        assertQuery("" +
                "SELECT *, o2.custkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 5 = 0)\n" +
                "FROM (SELECT * FROM orders WHERE custkey % 256 = 0) o1\n" +
                "JOIN (SELECT * FROM orders WHERE custkey % 256 = 0) o2\n" +
                "  ON (o1.orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 4 = 0)) = (o2.orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 4 = 0))\n" +
                "WHERE o1.orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 4 = 0)\n" +
                "ORDER BY o1.orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 7 = 0)");
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE partkey % 4 = 0),\n" +
                "  SUM(\n" +
                "    CASE\n" +
                "      WHEN orderkey\n" +
                "        IN (\n" +
                "          SELECT orderkey\n" +
                "          FROM lineitem\n" +
                "          WHERE suppkey % 4 = 0)\n" +
                "      THEN 1\n" +
                "      ELSE 0\n" +
                "      END)\n" +
                "FROM orders\n" +
                "GROUP BY orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE partkey % 4 = 0)\n" +
                "HAVING SUM(\n" +
                "  CASE\n" +
                "    WHEN orderkey\n" +
                "      IN (\n" +
                "        SELECT orderkey\n" +
                "        FROM lineitem\n" +
                "        WHERE suppkey % 4 = 0)\n" +
                "      THEN 1\n" +
                "      ELSE 0\n" +
                "      END) > 1");
    }

    @Test
    public void testJoinConstantPropagation()
    {
        assertQuery("" +
                "SELECT x, y, COUNT(*)\n" +
                "FROM (SELECT orderkey, 0 AS x FROM orders) a \n" +
                "JOIN (SELECT orderkey, 1 AS y FROM orders) b \n" +
                "ON a.orderkey = b.orderkey\n" +
                "GROUP BY 1, 2");
    }

    @Test
    public void testAntiJoin()
    {
        assertQuery("" +
                "SELECT *, orderkey\n" +
                "  NOT IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 3 = 0)\n" +
                "FROM orders");
    }

    @Test
    public void testAntiJoinNullHandling()
    {
        assertQuery("WITH empty AS (SELECT 1 WHERE FALSE) " +
                        "SELECT 3 FROM (VALUES 1) WHERE NULL NOT IN (SELECT * FROM empty)",
                "VALUES 3");

        assertQuery("WITH empty AS (SELECT 1 WHERE FALSE) " +
                        "SELECT x FROM (VALUES NULL) t(x) WHERE x NOT IN (SELECT * FROM empty)",
                "VALUES NULL");
    }

    @Test
    public void testSemiJoinLimitPushDown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 2 = 0)\n" +
                "  FROM orders\n" +
                "  LIMIT 10)");
    }

    @Test
    public void testSemiJoinNullHandling()
    {
        assertQuery("WITH empty AS (SELECT 1 WHERE FALSE) " +
                        "SELECT 3 FROM (VALUES 1) WHERE NULL IN (SELECT * FROM empty)",
                "SELECT 0 WHERE FALSE");

        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem)\n" +
                "FROM orders");
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem)\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders)");
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem)\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 4 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders)");
    }

    @Test
    public void testSemiJoinWithGroupBy()
    {
        // using the same subquery in query
        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber");

        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber " +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        // using different subqueries
        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT sum(orderkey) FROM orders WHERE orderkey < 5)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey > 3)");

        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 5)" +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey > 3)");
    }

    @Test
    public void testSemiJoinUnionNullHandling()
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 500 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM orders\n" +
                "    WHERE orderkey % 200 = 0\n" +
                "    UNION ALL\n" +
                "    SELECT CASE WHEN orderkey % 600 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM orders\n" +
                "    WHERE orderkey % 300 = 0\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE orderkey % 100 = 0)");
    }

    @Test
    public void testSemiJoinAggregationNullHandling()
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 10 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 2 = 0\n" +
                "    GROUP BY orderkey\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 3 = 0)");
    }

    @Test
    public void testSemiJoinUnionAggregationNullHandling()
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 500 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 250 = 0\n" +
                "    UNION ALL\n" +
                "    SELECT CASE WHEN orderkey % 300 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 200 = 0\n" +
                "    GROUP BY orderkey\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 100 = 0)\n");
    }

    @Test
    public void testSemiJoinAggregationUnionNullHandling()
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM (\n" +
                "      SELECT CASE WHEN orderkey % 500 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "      FROM orders\n" +
                "      WHERE orderkey % 200 = 0\n" +
                "      UNION ALL\n" +
                "      SELECT CASE WHEN orderkey % 600 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "      FROM orders\n" +
                "      WHERE orderkey % 300 = 0\n" +
                "    )\n" +
                "    GROUP BY orderkey\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE orderkey % 100 = 0)");
    }

    @Test
    public void testJoinPredicatePushdown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM lineitem \n" +
                "JOIN (\n" +
                "  SELECT * FROM orders\n" +
                ") orders \n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey % 4 = 0\n" +
                "  AND lineitem.suppkey > orders.orderkey");
    }

    @Test
    public void testLeftJoinAsInnerPredicatePushdown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.custkey IS NULL)");
    }

    @Test
    public void testPlainLeftJoinPredicatePushdown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE lineitem.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testLeftJoinPredicatePushdownWithSelfEquality()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey = orders.orderkey\n" +
                "  AND lineitem.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testLeftJoinPredicatePushdownWithNullConstant()
    {
        assertQuery("" +
                "SELECT count(*)\n" +
                "FROM orders a\n" +
                "LEFT OUTER JOIN orders b\n" +
                "  ON a.clerk = b.clerk\n" +
                "WHERE a.orderpriority='5-LOW'\n" +
                "  AND b.orderpriority='1-URGENT'\n" +
                "  AND b.clerk is null\n" +
                "  AND a.orderkey % 4 = 0\n");
    }

    @Test
    public void testRightJoinAsInnerPredicatePushdown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders\n" +
                "RIGHT JOIN lineitem\n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.custkey IS NULL)");
    }

    @Test
    public void testPlainRightJoinPredicatePushdown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "RIGHT JOIN lineitem\n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE lineitem.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testRightJoinPredicatePushdownWithSelfEquality()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "RIGHT JOIN lineitem\n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey = orders.orderkey\n" +
                "  AND lineitem.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testPredicatePushdownJoinEqualityGroups()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT custkey custkey1, custkey%4 custkey1a, custkey%8 custkey1b, custkey%16 custkey1c\n" +
                "  FROM orders\n" +
                ") orders1 \n" +
                "JOIN (\n" +
                "  SELECT custkey custkey2, custkey%4 custkey2a, custkey%8 custkey2b\n" +
                "  FROM orders\n" +
                ") orders2 ON orders1.custkey1 = orders2.custkey2\n" +
                "WHERE custkey2a = custkey2b\n" +
                "  AND custkey1 = custkey1a\n" +
                "  AND custkey2 = custkey2a\n" +
                "  AND custkey1a = custkey1c\n" +
                "  AND custkey1b = custkey1c\n" +
                "  AND custkey1b % 2 = 0");
    }

    @Test
    public void testNonDeterministicJoinPredicatePushdown()
    {
        MaterializedResult materializedResult = computeActual("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT DISTINCT *\n" +
                "  FROM (\n" +
                "    SELECT 'abc' AS col1a, 500 AS col1b FROM lineitem LIMIT 1\n" +
                "  ) table1\n" +
                "  JOIN (\n" +
                "    SELECT 'abc' AS col2a FROM lineitem LIMIT 1000000\n" +
                "  ) table2\n" +
                "  ON table1.col1a = table2.col2a\n" +
                "  WHERE rand() * 1000 > table1.col1b\n" +
                ")");
        MaterializedRow row = getOnlyElement(materializedResult.getMaterializedRows());
        assertEquals(row.getFieldCount(), 1);
        long count = (Long) row.getField(0);
        // Technically non-deterministic unit test but has essentially a next to impossible chance of a false positive
        assertTrue(count > 0 && count < 1000000);
    }

    @Test
    public void testSemiJoinPredicateMoveAround()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM orders WHERE custkey % 2 = 0 AND orderkey % 3 = 0)\n" +
                "WHERE orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 7 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem\n" +
                "    WHERE partkey % 2 = 0)\n" +
                "  AND\n" +
                "    orderkey % 2 = 0");
    }

    @Test
    public void testExecuteUsingComplexJoinCriteria()
    {
        String query = "SELECT * FROM (VALUES 1) t(a) JOIN (VALUES 2) u(a) ON t.a + u.a < ?";
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", query)
                .build();
        assertQuery(session,
                "EXECUTE my_query USING 5",
                "VALUES (1, 2)");
    }

    @Test
    public void testExecuteUsingWithSubqueryInJoin()
    {
        String query = "SELECT * " +
                "FROM " +
                "    (VALUES ?,2,3) t(x) " +
                "  JOIN " +
                "    (VALUES 1,2,3) t2(y) " +
                "  ON " +
                "(x in (VALUES 1,2,?)) = (y in (VALUES 1,2,3)) AND (x in (VALUES 1,?)) = (y in (VALUES 1,2))";

        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", query)
                .build();
        assertQuery(session,
                "EXECUTE my_query USING 1, 3, 2",
                "VALUES (1,1), (1,2), (2,2), (2,1), (3,3)");
    }

    @Test
    public void testLateralJoin()
    {
        assertQuery(
                "SELECT name FROM nation, LATERAL (SELECT 1 WHERE false)",
                "SELECT 1 WHERE false");

        assertQuery(
                "SELECT name FROM nation, LATERAL (SELECT 1)",
                "SELECT name FROM nation");

        assertQuery(
                "SELECT name FROM nation, LATERAL (SELECT 1 WHERE name = 'ola')",
                "SELECT 1 WHERE false");

        assertQuery(
                "SELECT nationkey, a FROM nation, LATERAL (SELECT max(region.name) FROM region WHERE region.regionkey <= nation.regionkey) t(a) ORDER BY nationkey LIMIT 1",
                "VALUES (0, 'AFRICA')");

        assertQuery(
                "SELECT nationkey, a FROM nation, LATERAL (SELECT region.name || '_' FROM region WHERE region.regionkey = nation.regionkey) t(a) ORDER BY nationkey LIMIT 1",
                "VALUES (0, 'AFRICA_')");

        assertQuery(
                "SELECT nationkey, a, b, name FROM nation, LATERAL (SELECT nationkey + 2 AS a), LATERAL (SELECT a * -1 AS b) ORDER BY b LIMIT 1",
                "VALUES (24, 26, -26, 'UNITED STATES')");

        assertQuery(
                "SELECT * FROM region r, LATERAL (SELECT * FROM nation) n WHERE n.regionkey = r.regionkey",
                "SELECT * FROM region, nation WHERE nation.regionkey = region.regionkey");
        assertQuery(
                "SELECT * FROM region, LATERAL (SELECT * FROM nation WHERE nation.regionkey = region.regionkey)",
                "SELECT * FROM region, nation WHERE nation.regionkey = region.regionkey");

        assertQuery(
                "SELECT quantity, extendedprice, avg_price, low, high " +
                        "FROM lineitem, " +
                        "LATERAL (SELECT extendedprice / quantity AS avg_price) average_price, " +
                        "LATERAL (SELECT avg_price * 0.9 AS low) lower_bound, " +
                        "LATERAL (SELECT avg_price * 1.1 AS high) upper_bound " +
                        "ORDER BY extendedprice, quantity LIMIT 1",
                "VALUES (1.0, 904.0, 904.0, 813.6, 994.400)");

        assertQuery(
                "SELECT y FROM (VALUES array[2, 3]) a(x) CROSS JOIN LATERAL(SELECT x[1]) b(y)",
                "SELECT 2");
        assertQuery(
                "SELECT * FROM (VALUES 2) a(x) CROSS JOIN LATERAL(SELECT x + 1)",
                "SELECT 2, 3");
        assertQuery(
                "SELECT * FROM (VALUES 2) a(x) CROSS JOIN LATERAL(SELECT x)",
                "SELECT 2, 2");
        assertQuery(
                "SELECT * FROM (VALUES 2) a(x) CROSS JOIN LATERAL(SELECT x, x + 1)",
                "SELECT 2, 2, 3");

        assertQueryFails(
                "SELECT * FROM (VALUES array[2, 2]) a(x) LEFT OUTER JOIN LATERAL(VALUES x) ON true",
                "line .*: LATERAL on other than the right side of CROSS JOIN is not supported");
        assertQueryFails(
                "SELECT * FROM (VALUES array[2, 2]) a(x) RIGHT OUTER JOIN LATERAL(VALUES x) ON true",
                "line .*: LATERAL on other than the right side of CROSS JOIN is not supported");
        assertQueryFails(
                "SELECT * FROM (VALUES array[2, 2]) a(x) FULL OUTER JOIN LATERAL(VALUES x) ON true",
                "line .*: LATERAL on other than the right side of CROSS JOIN is not supported");
    }

    @Test
    public void testJoinsWithNulls()
    {
        Session sessionWithOptNulls = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_NULLS_IN_JOINS, "true")
                .build();
        testJoinsWithNullsInternal(getSession());
        testJoinsWithNullsInternal(sessionWithOptNulls);
    }

    private void testJoinsWithNullsInternal(Session session)
    {
        assertQuery(
                session,
                "SELECT * FROM (VALUES 2, 3, null) a(x) INNER JOIN (VALUES 3, 4, null) b(x) ON a.x = b.x",
                "SELECT * FROM VALUES (3, 3)");

        assertQuery(
                session,
                "SELECT * FROM (VALUES 2, 3, null) a(x) LEFT JOIN (VALUES 3, 4, null) b(x) ON a.x = b.x",
                "SELECT * FROM VALUES (3, 3), (2, NULL), (NULL, NULL)");

        assertQuery(
                session,
                "SELECT * FROM (VALUES 2, 3, null) a(x) RIGHT JOIN (VALUES 3, 4, null) b(x) ON a.x = b.x",
                "SELECT * FROM VALUES (3, 3), (NULL, 4), (NULL, NULL)");

        assertQuery(
                session,
                "SELECT * FROM (VALUES 2, 3, null) a(x) " +
                        "FULL OUTER JOIN (VALUES 3, 4, null) b(x) ON a.x = b.x",
                "SELECT * FROM VALUES (3, 3), (NULL, 4), (2, NULL), (NULL, NULL), (NULL, NULL)");

        assertQuery(
                session,
                "SELECT * FROM (VALUES 2, 3, null) a(x) " +
                        "FULL OUTER JOIN (VALUES 3, 4, null) b(x) ON a.x = b.x WHERE a.x IS NULL",
                "SELECT * FROM VALUES (NULL, 4), (NULL, NULL), (NULL, NULL)");
    }

    @Test
    public void testInnerJoinWithEmptyBuildSide()
    {
        MaterializedResult actual = computeActual(
                noJoinReordering(),
                "WITH small_part AS (SELECT * FROM part WHERE name = 'a') " +
                        "SELECT lineitem.orderkey FROM lineitem INNER JOIN small_part ON lineitem.partkey = small_part.partkey");

        assertEquals(actual.getRowCount(), 0);
    }

    @Test
    public void testRightJoinWithEmptyBuildSide()
    {
        assertQuery(
                noJoinReordering(),
                "WITH small_part AS (SELECT * FROM part WHERE name = 'a') SELECT lineitem.orderkey FROM lineitem RIGHT JOIN small_part ON lineitem.partkey = small_part.partkey");
    }

    @Test
    public void testLeftJoinWithEmptyBuildSide()
    {
        assertQuery(
                noJoinReordering(),
                "WITH small_part AS (SELECT * FROM part WHERE name = 'a') SELECT lineitem.orderkey FROM lineitem LEFT JOIN small_part ON lineitem.partkey = small_part.partkey");
    }

    @Test
    public void testFullJoinWithEmptyBuildSide()
    {
        assertQuery(
                noJoinReordering(),
                "WITH small_part AS (SELECT * FROM part WHERE name = 'a') SELECT lineitem.orderkey FROM lineitem FULL OUTER JOIN small_part ON lineitem.partkey = small_part.partkey",
                // H2 doesn't support FULL OUTER
                "WITH small_part AS (SELECT * FROM part WHERE name = 'a') SELECT lineitem.orderkey FROM lineitem LEFT JOIN small_part ON lineitem.partkey = small_part.partkey");
    }

    @Test
    public void testInnerJoinWithEmptyProbeSide()
    {
        assertQuery(
                noJoinReordering(),
                "WITH small_part AS (SELECT * FROM part WHERE name = 'a') SELECT lineitem.orderkey FROM small_part INNER JOIN lineitem ON small_part.partkey = lineitem.partkey");
    }

    @Test
    public void testRightJoinWithEmptyProbeSide()
    {
        assertQuery(
                noJoinReordering(),
                "WITH small_part AS (SELECT * FROM part WHERE name = 'a') SELECT lineitem.orderkey FROM small_part RIGHT JOIN lineitem ON  small_part.partkey = lineitem.partkey");
    }

    protected Session noJoinReordering()
    {
        return Session.builder(getSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, FeaturesConfig.JoinReorderingStrategy.NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.PARTITIONED.name())
                .build();
    }
}
