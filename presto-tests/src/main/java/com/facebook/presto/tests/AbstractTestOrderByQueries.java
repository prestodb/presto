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
import com.facebook.presto.testing.MaterializedResult;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_SORT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryTemplate.parameter;
import static com.facebook.presto.tests.QueryTemplate.queryTemplate;

public abstract class AbstractTestOrderByQueries
        extends AbstractTestQueryFramework
{
    @Test
    public void testOrderBy()
    {
        assertQueryOrdered("SELECT orderstatus FROM orders ORDER BY orderstatus");
        assertQueryOrdered("SELECT orderstatus FROM orders ORDER BY orderkey DESC");
    }

    @Test
    public void testOrderByLimit()
    {
        assertQueryOrdered("SELECT custkey, orderstatus FROM orders ORDER BY orderkey DESC LIMIT 10");
        assertQueryOrdered("SELECT custkey, orderstatus FROM orders ORDER BY orderkey + 1 DESC LIMIT 10");
        assertQuery("SELECT custkey, totalprice FROM orders ORDER BY orderkey LIMIT 0");
    }

    @Test
    public void testOrderByWithOutputColumnReference()
    {
        assertQueryOrdered("SELECT a*2 AS b FROM (VALUES -1, 0, 2) t(a) ORDER BY b*-1", "VALUES 4, 0, -2");
        assertQueryOrdered("SELECT a*2 AS b FROM (VALUES -1, 0, 2) t(a) ORDER BY b", "VALUES -2, 0, 4");
        assertQueryOrdered("SELECT a*-2 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY a*-1", "VALUES 2, 0, -4");
        assertQueryOrdered("SELECT a*-2 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY t.a*-1", "VALUES -4, 0, 2");
        assertQueryOrdered("SELECT a*-2 FROM (VALUES -1, 0, 2) t(a) ORDER BY a*-1", "VALUES -4, 0, 2");
        assertQueryOrdered("SELECT a*-2 FROM (VALUES -1, 0, 2) t(a) ORDER BY t.a*-1", "VALUES -4, 0, 2");
        assertQueryOrdered("SELECT a, a* -1 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY t.a", "VALUES (-1, 1), (0, 0), (2, -2)");
        assertQueryOrdered("SELECT a, a* -2 AS b FROM (VALUES -1, 0, 2) t(a) ORDER BY a + b", "VALUES (2, -4), (0, 0), (-1, 2)");
        assertQueryOrdered("SELECT a AS b, a* -2 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY a + b", "VALUES (2, -4), (0, 0), (-1, 2)");
        assertQueryOrdered("SELECT a* -2 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY a + t.a", "VALUES -4, 0, 2");
        assertQueryOrdered("SELECT k, SUM(a) a, SUM(b) a FROM (VALUES (1, 2, 3)) t(k, a, b) GROUP BY k ORDER BY k", "VALUES (1, 2, 3)");

        // coercions
        assertQueryOrdered("SELECT 1 x ORDER BY degrees(x)", "VALUES 1");
        assertQueryOrdered("SELECT a + 1 AS b FROM (VALUES 1, 2) t(a) ORDER BY -1.0 * b", "VALUES 3, 2");
        assertQueryOrdered("SELECT a AS b FROM (VALUES 1, 2) t(a) ORDER BY -1.0 * b", "VALUES 2, 1");
        assertQueryOrdered("SELECT a AS a FROM (VALUES 1, 2) t(a) ORDER BY -1.0 * a", "VALUES 2, 1");
        assertQueryOrdered("SELECT 1 x ORDER BY degrees(x)", "VALUES 1");

        // groups
        assertQueryOrdered("SELECT max(a+b), min(a+b) AS a FROM (values (1,2),(3,2),(1,5)) t(a,b) GROUP BY a ORDER BY max(t.a+t.b)", "VALUES (5, 5), (6, 3)");
        assertQueryOrdered("SELECT max(a+b), min(a+b) AS a FROM (values (1,2),(3,2),(1,5)) t(a,b) GROUP BY a ORDER BY max(t.a+t.b)*-0.1", "VALUES (6, 3), (5, 5)");
        assertQueryOrdered("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY max(b*1.0)", "VALUES 2, 1");
        assertQueryOrdered("SELECT max(a) AS b FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY b", "VALUES 1, 2");
        assertQueryOrdered("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY b*1.0", "VALUES 2, 1");
        assertQueryOrdered("SELECT max(a)*100 AS c FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY max(b) + c", "VALUES 100, 200");
        assertQueryOrdered("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY b", "VALUES 2, 1");
        assertQueryOrdered("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY t.b ORDER BY t.b*1.0", "VALUES 2, 1");
        assertQueryOrdered("SELECT -(a+b) AS a, -(a+b) AS b, a+b FROM (values (41, 42), (-41, -42)) t(a,b) GROUP BY a+b ORDER BY a+b", "VALUES (-83, -83, 83), (83, 83, -83)");
        assertQueryOrdered("SELECT -a AS a FROM (values (1,2),(3,2)) t(a,b) GROUP BY GROUPING SETS ((a), (a, b)) ORDER BY -a", "VALUES -1, -1, -3, -3");
        assertQueryOrdered("SELECT a AS foo FROM (values (1,2),(3,2)) t(a,b) GROUP BY GROUPING SETS ((a), (a, b)) HAVING b IS NOT NULL ORDER BY -a", "VALUES 3, 1");
        assertQueryOrdered("SELECT max(a) FROM (values (1,2),(3,2)) t(a,b) ORDER BY max(-a)", "VALUES 3");
        assertQueryFails("SELECT max(a) AS a FROM (values (1,2)) t(a,b) GROUP BY b ORDER BY max(a+b)", ".*Invalid reference to output projection attribute from ORDER BY aggregation");
        assertQueryOrdered("SELECT -a AS a, a AS b FROM (VALUES 1, 2) t(a) GROUP BY t.a ORDER BY a", "VALUES (-2, 2), (-1, 1)");
        assertQueryOrdered("SELECT -a AS a, a AS b FROM (VALUES 1, 2) t(a) GROUP BY t.a ORDER BY t.a", "VALUES (-1, 1), (-2, 2)");
        assertQueryOrdered("SELECT -a AS a, a AS b FROM (VALUES 1, 2) t(a) GROUP BY a ORDER BY t.a", "VALUES (-1, 1), (-2, 2)");
        assertQueryOrdered("SELECT -a AS a, a AS b FROM (VALUES 1, 2) t(a) GROUP BY a ORDER BY t.a+2*a", "VALUES (-2, 2), (-1, 1)");
        assertQueryOrdered("SELECT -a AS a, a AS b FROM (VALUES 1, 2) t(a) GROUP BY t.a ORDER BY t.a+2*a", "VALUES (-2, 2), (-1, 1)");

        // lambdas
        assertQueryOrdered("SELECT x AS y FROM (values (1,2), (2,3)) t(x, y) GROUP BY x ORDER BY apply(x, x -> -x) + 2*x", "VALUES 1, 2");
        assertQueryOrdered("SELECT -y AS x FROM (values (1,2), (2,3)) t(x, y) GROUP BY y ORDER BY apply(x, x -> -x)", "VALUES -2, -3");
        assertQueryOrdered("SELECT -y AS x FROM (values (1,2), (2,3)) t(x, y) GROUP BY y ORDER BY sum(apply(-y, x -> x * 1.0))", "VALUES -3, -2");

        // distinct
        assertQueryOrdered("SELECT DISTINCT -a AS b FROM (VALUES 1, 2) t(a) ORDER BY b", "VALUES -2, -1");
        assertQueryOrdered("SELECT DISTINCT -a AS b FROM (VALUES 1, 2) t(a) ORDER BY 1", "VALUES -2, -1");
        assertQueryOrdered("SELECT DISTINCT max(a) AS b FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY b", "VALUES 1, 2");
        assertQueryFails("SELECT DISTINCT -a AS b FROM (VALUES (1, 2), (3, 4)) t(a, c) ORDER BY c", ".*For SELECT DISTINCT, ORDER BY expressions must appear in select list");
        assertQueryFails("SELECT DISTINCT -a AS b FROM (VALUES (1, 2), (3, 4)) t(a, c) ORDER BY 2", ".*ORDER BY position 2 is not in select list");

        // window
        assertQueryOrdered("SELECT a FROM (VALUES 1, 2) t(a) ORDER BY -row_number() OVER ()", "VALUES 2, 1");
        assertQueryOrdered("SELECT -a AS a, first_value(-a) OVER (ORDER BY a ROWS 0 PRECEDING) AS b FROM (VALUES 1, 2) t(a) ORDER BY first_value(a) OVER (ORDER BY a ROWS 0 PRECEDING)", "VALUES (-2, -2), (-1, -1)");
        assertQueryOrdered("SELECT -a AS a FROM (VALUES 1, 2) t(a) ORDER BY first_value(a+t.a*2) OVER (ORDER BY a ROWS 0 PRECEDING)", "VALUES -1, -2");

        assertQueryFails("SELECT a, a* -1 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY a", ".*'a' is ambiguous");
    }

    @Test
    public void testOrderByWithAggregation()
    {
        assertQuery("" +
                        "SELECT x, sum(cast(x AS double))\n" +
                        "FROM (VALUES '1.0') t(x)\n" +
                        "GROUP BY x\n" +
                        "ORDER BY sum(cast(t.x AS double))",
                "VALUES ('1.0', 1.0)");

        queryTemplate("SELECT count(*) %output% FROM (SELECT substr(name,1,1) letter FROM nation) x GROUP BY %groupBy% ORDER BY %orderBy%")
                .replaceAll(
                        parameter("output").of("", ", letter", ", letter AS y"),
                        parameter("groupBy").of("x.letter", "letter"),
                        parameter("orderBy").of("x.letter", "letter"))
                .forEach(this::assertQueryOrdered);
    }

    @Test
    public void testOrderByLimitAll()
    {
        assertQuery("SELECT custkey, totalprice FROM orders ORDER BY orderkey LIMIT ALL", "SELECT custkey, totalprice FROM orders ORDER BY orderkey");
    }

    @Test
    public void testOrderByMultipleFields()
    {
        assertQueryOrdered("SELECT custkey, orderstatus FROM orders ORDER BY custkey DESC, orderstatus");
    }

    @Test
    public void testDuplicateColumnsInOrderByClause()
    {
        MaterializedResult actual = computeActual("SELECT * FROM (VALUES INTEGER '3', INTEGER '2', INTEGER '1') t(a) ORDER BY a ASC, a DESC");

        MaterializedResult expected = resultBuilder(getSession(), INTEGER)
                .row(1)
                .row(2)
                .row(3)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testOrderByWithNulls()
    {
        // nulls first
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS FIRST, custkey ASC");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) DESC NULLS FIRST, custkey ASC");

        // nulls last
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS LAST, custkey ASC");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) DESC NULLS LAST, custkey ASC");

        // assure that default is nulls last
        assertQueryOrdered(
                "SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC, custkey ASC",
                "SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS LAST, custkey ASC");
    }

    @Test
    public void testOrderByAlias()
    {
        assertQueryOrdered("SELECT orderstatus x FROM orders ORDER BY x ASC");
    }

    @Test
    public void testOrderByAliasWithSameNameAsUnselectedColumn()
    {
        assertQueryOrdered("SELECT orderstatus orderdate FROM orders ORDER BY orderdate ASC");
    }

    @Test
    public void testOrderByOrdinal()
    {
        assertQueryOrdered("SELECT orderstatus, orderdate FROM orders ORDER BY 2, 1");
    }

    @Test
    public void testOrderByOrdinalWithWildcard()
    {
        assertQueryOrdered("SELECT * FROM orders ORDER BY 1");
    }

    @Test
    public void testOrderByWithSimilarExpressions()
    {
        assertQuery(
                "WITH t AS (SELECT 1 x, 2 y) SELECT x, y FROM t ORDER BY x, y",
                "SELECT 1, 2");
        assertQuery(
                "WITH t AS (SELECT 1 x, 2 y) SELECT x, y FROM t ORDER BY x, y LIMIT 1",
                "SELECT 1, 2");
        assertQuery(
                "WITH t AS (SELECT 1 x, 1 y) SELECT x, y FROM t ORDER BY x, y LIMIT 1",
                "SELECT 1, 1");
        assertQuery(
                "WITH t AS (SELECT orderkey x, orderkey y FROM orders) SELECT x, y FROM t ORDER BY x, y LIMIT 1",
                "SELECT 1, 1");
        assertQuery(
                "WITH t AS (SELECT orderkey x, orderkey y FROM orders) SELECT x, y FROM t ORDER BY x, y DESC LIMIT 1",
                "SELECT 1, 1");
        assertQuery(
                "WITH t AS (SELECT orderkey x, totalprice y, orderkey z FROM orders) SELECT x, y, z FROM t ORDER BY x, y, z LIMIT 1",
                "SELECT 1, 172799.49, 1");
    }

    @Test
    public void testOrderByUnderManyProjections()
    {
        assertQuery("SELECT nationkey, arbitrary_column + arbitrary_column " +
                "FROM " +
                "( " +
                "   SELECT nationkey, COALESCE(arbitrary_column, 0) arbitrary_column " +
                "   FROM ( " +
                "      SELECT nationkey, 1 arbitrary_column " +
                "      FROM nation " +
                "      ORDER BY 1 ASC))");
    }

    @Test
    public void testUndistributedOrderBy()
    {
        Session undistributedOrderBy = Session.builder(getSession())
                .setSystemProperty(DISTRIBUTED_SORT, "false")
                .build();
        assertQueryOrdered(undistributedOrderBy, "SELECT orderstatus FROM orders ORDER BY orderstatus");
    }

    @Test
    public void testOrderLimitCompaction()
    {
        assertQueryOrdered("SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)");
    }

    @Test
    public void testCaseInsensitiveOutputAliasInOrderBy()
    {
        assertQueryOrdered("SELECT orderkey X FROM orders ORDER BY x");
    }
}
