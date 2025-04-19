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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.AccessControlInfo;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestUtilizedColumnsAnalyzer
        extends AbstractAnalyzerTest
{
    @Test
    public void testWildcardSelect()
    {
        // Test wildcard select
        assertUtilizedTableColumns("SELECT * FROM t1",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c", "d"))));

        // Test outer select * captures everything
        assertUtilizedTableColumns("SELECT * FROM (SELECT a + b FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));
        assertUtilizedTableColumns("SELECT * FROM (SELECT a + b FROM (SELECT * FROM t1))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));
    }

    @Test
    public void testCountStar()
    {
        // Test count(*) should not need column access for anything, but should still need table access
        assertUtilizedTableColumns("SELECT count(*) FROM t1",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of())));
    }

    @Test
    public void testRecursiveProjectionPruning()
    {
        // Test recursive pruning of projections in subquery
        assertUtilizedTableColumns("SELECT a FROM (SELECT * FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));
        assertUtilizedTableColumns("SELECT a FROM (SELECT a, b FROM (SELECT a, b, c FROM t1))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));
    }

    @Test
    public void testAliasing()
    {
        // Test aliasing
        assertUtilizedTableColumns("SELECT x FROM (SELECT *, a as x FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));

        // Test alias expression with multiple columns referenced in alias
        assertUtilizedTableColumns("SELECT x FROM (SELECT *, a + b as x FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));

        // Test chained aliasing
        assertUtilizedTableColumns("SELECT x + 3 FROM (SELECT y + 2 as x FROM (SELECT z + 1 as y FROM (SELECT a as z FROM t1 WHERE b = 1)))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));

        // Test aliased relation
        assertUtilizedTableColumns("SELECT mytable.a, mytable.b FROM (SELECT * FROM t1 WHERE t1.c = 1) mytable",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"))));

        // Test aliased relation for self-join
        assertUtilizedTableColumns("SELECT x.a, y.b FROM t1 x, t1 y",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));

        // Test filter on aliased relation
        assertUtilizedTableColumns("SELECT count(x) FROM (SELECT a as x, * FROM t1) t WHERE t.b = 3",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));

        // Test aliased relation with column aliases
        assertUtilizedTableColumns("SELECT y FROM (SELECT x, y FROM t1 AS mytable (w, x, y, z))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("c"))));
    }

    @Test
    public void testJoin()
    {
        // Cartesian join
        assertUtilizedTableColumns("SELECT * FROM (SELECT * FROM t1, t2)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c", "d"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a", "b"))));

        // Joins with column aliases
        assertUtilizedTableColumns("SELECT t2.a + y FROM t2 CROSS JOIN (SELECT *, c + d AS y FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("c", "d"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"))));
        assertUtilizedTableColumns("SELECT mytable.a + myothertable.b AS f1, myothertable.z FROM t2 AS mytable CROSS JOIN (SELECT *, a + b AS x, c - 1 AS z FROM t1) myothertable",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("b", "c"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"))));

        // Columns in ON join condition should be checked
        assertUtilizedTableColumns("SELECT t1.a FROM t1 JOIN t2 ON t1.c = t2.b",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "c"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("b"))));

        // When joining to subquery, prune unused columns from subquery
        assertUtilizedTableColumns("SELECT t2.a FROM t2 FULL OUTER JOIN (SELECT a, b FROM t1 WHERE c > 0) mytable ON t2.a = mytable.a",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "c"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"))));

        // Prune unused columns from joined relation
        assertUtilizedTableColumns("SELECT w FROM (SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a AND t1.b = t2.b) AS t(u, v, w, x, y, z)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a", "b"))));

        // Join with USING clause
        assertUtilizedTableColumns("SELECT t1.c FROM t1 JOIN t2 USING (a, b)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a", "b"))));
    }

    @Test
    public void testGroupBy()
    {
        // Capture column in GROUP BY
        assertUtilizedTableColumns("SELECT a, b FROM (SELECT count(a) AS a, avg(b) AS b FROM t1 GROUP BY c)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"))));

        // Capture columns in subquery in GROUP BY expression
        assertUtilizedTableColumns("SELECT a, b FROM (SELECT count(a) AS a, avg(b) AS b FROM t1 GROUP BY (c * (SELECT max(a) FROM t2)))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"))));

        // Capture column in HAVING
        assertUtilizedTableColumns("SELECT count(a) AS c FROM t1 WHERE b = 0 GROUP BY c HAVING max(d) > 10",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c", "d"))));

        // Capture columns in subquery in HAVING expression
        assertUtilizedTableColumns("SELECT count(a) FROM t1 GROUP BY b HAVING count(a) > (SELECT max(a) FROM t2)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"))));

        // ROLLUP, CUBE, and GROUPING SETS do not allow for expressions so we only need to test for explicit columns

        // Capture column in ROLLUP
        assertUtilizedTableColumns("SELECT a, b FROM (SELECT count(a) AS a, avg(b) AS b FROM t1 GROUP BY ROLLUP(c))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"))));

        // Capture column in CUBE
        assertUtilizedTableColumns("SELECT a, b FROM (SELECT count(a) AS a, avg(b) AS b FROM t1 GROUP BY CUBE(c, d))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c", "d"))));

        // Capture columns in GROUPING SETS
        assertUtilizedTableColumns("SELECT x FROM (SELECT sum(c) AS x FROM t1 GROUP BY GROUPING SETS (a, b))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"))));
        assertUtilizedTableColumns("SELECT x FROM (SELECT grouping(a, b) AS x FROM t1 GROUP BY GROUPING SETS (a, b))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));

        // Test ordinal in GROUP BY
        assertUtilizedTableColumns("SELECT * FROM (SELECT count(a), count(b), c FROM t1 WHERE b = 0 GROUP BY 3)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"))));
    }

    public void testOrderBy()
    {
        // Column reference in ORDER BY
        assertUtilizedTableColumns("SELECT * FROM (SELECT b, c FROM t1 WHERE b = 0 ORDER BY b)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("b", "c"))));

        // Test ordinal in ORDER BY
        assertUtilizedTableColumns("SELECT * FROM (SELECT b, c FROM t1 WHERE b = 0 ORDER BY 2)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("b", "c"))));
    }

    @Test
    public void testCTE()
    {
        // Test CTE columns are pruned
        assertUtilizedTableColumns("WITH mytable AS (SELECT * FROM t1) SELECT x FROM (SELECT a AS x FROM mytable)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));

        // CTE filter column is captured
        assertUtilizedTableColumns("WITH mytable AS (SELECT a as x, b as y FROM t1 WHERE c = 0) SELECT x FROM mytable",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "c"))));

        // Unused CTE shouldn't require column level permissions, but should require table level permissions
        assertUtilizedTableColumns("WITH mytable AS (SELECT * FROM t1) SELECT a FROM t2",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of(), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"))));

        // Test CTE aliased twice for self-join
        assertUtilizedTableColumns("WITH t AS (SELECT a, b FROM t1) SELECT 1 FROM t AS mytable JOIN t as myothertable ON mytable.a = myothertable.b",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));
    }

    @Test
    public void testInsert()
    {
        assertUtilizedTableColumns("INSERT INTO t2 SELECT a, b FROM t1",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));
    }

    @Test
    public void testCTAS()
    {
        assertUtilizedTableColumns("CREATE TABLE foo AS SELECT a, b FROM t1",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));
    }

    @Test
    public void testSetOperations()
    {
        // Test EXCEPT
        assertUtilizedTableColumns("SELECT a, b FROM t1 WHERE c = 0 EXCEPT SELECT * from t2",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a", "b"))));

        // Test INTERSECT
        assertUtilizedTableColumns("SELECT a FROM (SELECT * FROM t1 WHERE t1.b = 0 INTERSECT SELECT * from t1 WHERE t1.c = 0)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"))));

        // Test UNION
        assertUtilizedTableColumns("SELECT * FROM ((SELECT a, b FROM t1 WHERE c = 0) UNION (SELECT * FROM t2 WHERE b = 0))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a", "b"))));
        assertUtilizedTableColumns("SELECT a, b FROM (SELECT a, b, c, d FROM t1 UNION ALL SELECT a AS w, b AS x, a AS y, b AS z FROM t2) WHERE c = 0 and d = c",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(
                        QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c", "d"),
                        QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a", "b"))));
    }

    @Test
    public void testLateral()
    {
        // Select item in lateral should be checked
        assertUtilizedTableColumns("SELECT a, x FROM t1 CROSS JOIN LATERAL (SELECT b as x)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));

        // Test pruning of select items in lateral if they are unused
        assertUtilizedTableColumns("SELECT a FROM t1 CROSS JOIN LATERAL (SELECT b as x)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));

        // Test lateral from different table
        assertUtilizedTableColumns("SELECT * FROM t1 CROSS JOIN LATERAL (SELECT b FROM t2)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c", "d"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("b"))));

        // Test chained lateral
        assertUtilizedTableColumns("SELECT a, x, y FROM t1 CROSS JOIN LATERAL (SELECT a + 1 as x) CROSS JOIN LATERAL (SELECT x + 2 as y)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));
    }

    @Test
    public void testSampledRelation()
    {
        assertUtilizedTableColumns("SELECT a FROM (SELECT * FROM t1 TABLESAMPLE BERNOULLI (10))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));
    }

    @Test
    public void testUnnest()
    {
        // Unnest column gets pruned if unused
        assertUtilizedTableColumns("SELECT a FROM t7 CROSS JOIN UNNEST (c) AS t(x)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t7"), ImmutableSet.of("a"))));

        // Unnest column used through alias
        assertUtilizedTableColumns("SELECT a, x FROM t7 CROSS JOIN UNNEST (c) AS t(x)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t7"), ImmutableSet.of("a", "c"))));

        // Multiple unnest columns
        assertUtilizedTableColumns("SELECT a, x, y FROM t7 CROSS JOIN UNNEST (c, d) AS t(x, y)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t7"), ImmutableSet.of("a", "c", "d"))));

        // Unused unnest columns are pruned, but used ones are kept
        assertUtilizedTableColumns("SELECT a, y FROM t7 CROSS JOIN UNNEST (c, d) AS t(x, y)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t7"), ImmutableSet.of("a", "d"))));

        // Unnest from subquery
        assertUtilizedTableColumns("SELECT y FROM (SELECT (SELECT ARRAY_AGG(a) FROM t1) x) CROSS JOIN UNNEST(x) AS t(y)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));
    }

    @Test
    public void testUnnestMap()
    {
        assertUtilizedTableColumns("SELECT t.x, t.y FROM (select * from t1) as cte1 CROSS JOIN UNNEST (MAP(array_agg(cte1.a), array_agg(cte1.a))) AS t(x, y)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));

        // Fall back to checking all columns does not happen
        assertUtilizedTableColumns("SELECT cte1.a, t.y FROM (select * from t1) as cte1 CROSS JOIN UNNEST (MAP(array_agg(cte1.c), array_agg(cte1.c))) AS t(x, y)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "c"))));

        // The map was constructed from t1.c and t1.d, so in the outer scope, if one of the key or value fields get used, both count as being utilized
        assertUtilizedTableColumns("SELECT t.c3, cte1.x FROM t1 CROSS JOIN UNNEST (array_agg(t1.b),MAP(array_agg(t1.c), array_agg(t1.d))) AS t(c1 , c2, c3) inner join (select * from t13) as cte1 on cte1.x = t1.d",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("c", "d"),
                        QualifiedObjectName.valueOf("tpch.s1.t13"), ImmutableSet.of("x"))));

        // Tricky case: t.c3 is utilized. It has field index of 2, but it should correspond to the expression index 1 (array_agg(t1.c))
        assertUtilizedTableColumns("SELECT t.c3, cte1.x FROM t1 CROSS JOIN UNNEST (MAP(array_agg(t1.b), array_agg(t1.b)), array_agg(t1.c), array_agg(t1.d)) AS t(c1 , c2, c3, c4) inner join (select * from t13) as cte1 on cte1.x = t1.d",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("c", "d"),
                        QualifiedObjectName.valueOf("tpch.s1.t13"), ImmutableSet.of("x"))));
    }

    @Test
    public void testValues()
    {
        assertUtilizedTableColumns("SELECT * FROM (VALUES 1, 2, 3)", ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of()));
        assertUtilizedTableColumns("SELECT * FROM (VALUES array[2, 2]) a(x) CROSS JOIN UNNEST(x)", ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of()));
        assertUtilizedTableColumns("SELECT a, b FROM t1 CROSS JOIN (VALUES 1, 2, 3)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));
    }

    @Test
    public void testInSubquery()
    {
        // IN/NOT IN subqueries should keep all their select columns
        assertUtilizedTableColumns("SELECT a FROM (SELECT * FROM t1 WHERE t1.b IN (SELECT b FROM t2))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("b"))));
        assertUtilizedTableColumns("SELECT a FROM (SELECT * FROM t1 WHERE t1.b IN (SELECT b FROM t2 WHERE t2.a = 0) AND t1.c = 0)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a", "b"))));
        assertUtilizedTableColumns("SELECT a, b FROM t1 WHERE t1.b IN (SELECT b FROM t2 WHERE t2.a NOT IN (SELECT x FROM t3) AND t1.c = 0)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(
                        QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"),
                        QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a", "b"),
                        QualifiedObjectName.valueOf("tpch.s1.t3"), ImmutableSet.of("x"))));

        // Test aliased IN subquery (self join)
        assertUtilizedTableColumns("SELECT r1.a, r1.b FROM t1 as r1 WHERE r1.b IN (SELECT r2.c FROM t1 as r2 WHERE r2.d = 0)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c", "d"))));

        // Test correlated IN subquery
        assertUtilizedTableColumns("SELECT r1.a, r1.b, r2.a FROM t1 as r1 JOIN t2 as r2 ON r1.a = r2.a WHERE 0 IN (SELECT x FROM t3 r3 WHERE r2.a = r3.a)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(
                        QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"),
                        QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"),
                        QualifiedObjectName.valueOf("tpch.s1.t3"), ImmutableSet.of("a", "x"))));
    }

    @Test
    public void testExistsSubquery()
    {
        // In exists/not exists subqueries, the select list should be ignored
        assertUtilizedTableColumns("SELECT a FROM t1 WHERE NOT EXISTS (SELECT * FROM t2)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of())));
        assertUtilizedTableColumns("SELECT a FROM (SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t2.b = t1.b))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("b"))));

        // Chain of correlated exists subquery (self join)
        assertUtilizedTableColumns("SELECT r1.a, r1.b FROM t2 r1, t1 WHERE EXISTS (SELECT r2.a, r2.b FROM t2 r2 WHERE r2.a = r1.b AND EXISTS (SELECT r3.a, r3.b FROM t2 r3 WHERE r3.a = r2.b AND r3.b = t1.c))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("c"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a", "b"))));

        // Exists subquery in SELECT
        assertUtilizedTableColumns("SELECT EXISTS(SELECT * FROM t1) = EXISTS(SELECT * FROM t2)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of(), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of())));
        assertUtilizedTableColumns("SELECT EXISTS(SELECT * FROM t1 WHERE t1.a = 0) = EXISTS(SELECT * FROM t2)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of())));
    }

    @Test
    public void testScalarSubquery()
    {
        // Scalar subquery in SELECT expression
        assertUtilizedTableColumns("SELECT a, (SELECT avg(a) FROM t2) FROM t1",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"))));
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT a, ((SELECT avg(a) FROM t2) * 5) as mycolumn FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of(), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"))));

        // Scalar subquery in SELECT should be pruned if unreferenced
        assertUtilizedTableColumns("SELECT a FROM (SELECT a, (SELECT avg(a) FROM t2) as mycolumn FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of())));

        // Scalar subquery in GROUP BY
        assertUtilizedTableColumns("SELECT count(a) FROM t1 GROUP BY (b * (SELECT avg(a) FROM t2))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"))));
        assertUtilizedTableColumns("SELECT count(a) FROM t1 GROUP BY (b * (SELECT avg(c)))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"))));

        // Scalar subquery in HAVING
        assertUtilizedTableColumns("SELECT count(a) FROM t1 GROUP BY b HAVING count(a) > (SELECT avg(a) FROM t2)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"))));
        assertUtilizedTableColumns("SELECT count(r1.a) FROM t1 as r1 GROUP BY b HAVING count(r1.a) > (SELECT avg(r1.b))",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));

        // Scalar subquery in where with ANY/ALL
        assertUtilizedTableColumns("SELECT t1.c FROM t1 WHERE t1.b >= ANY(SELECT max(t1.a) FROM t1 GROUP BY t1.b)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"))));
        assertUtilizedTableColumns("SELECT a FROM t1 WHERE t1.b >= ALL(SELECT b FROM t2)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("b"))));
    }

    @Test
    public void testDerivedColumns()
    {
        assertUtilizedTableColumns("SELECT b_is_zero FROM (SELECT *, b = 0 AS b_is_zero FROM t1 WHERE a = 0)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));
        assertUtilizedTableColumns("WITH mycte AS (SELECT *, b = 0 as b_is_zero FROM t1 WHERE a = 0) SELECT a FROM mycte",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));
    }

    @Test
    public void testOrderByInAggregation()
    {
        assertUtilizedTableColumns("SELECT myarray FROM (SELECT array_agg(a ORDER BY b, c DESC) AS myarray FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"))));
    }

    @Test
    public void testWindowFunction()
    {
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT sum(a) OVER (PARTITION BY b ORDER BY c) AS mycolumn FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"))));
    }

    @Test
    public void testAggregationFilter()
    {
        // Filter expression should be captured
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT sum(a) FILTER (WHERE b > 0) AS mycolumn FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));

        // Filter expression should be captured for count star
        assertUtilizedTableColumns("SELECT count(*) FILTER (WHERE a > 5) FROM t1",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));

        // If the filter expression is an IN subquery, all select items of the subquery should be captured
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT sum(a) FILTER (WHERE b IN (SELECT c FROM t1)) AS mycolumn FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"))));

        // Filter expression referencing different table should be captured
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT sum(a) FILTER (WHERE b IN (SELECT b FROM t2)) AS mycolumn FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("b"))));
    }

    @Test
    public void testLambdaExpressions()
    {
        // Column reference inside lambda should be captured
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT apply(0, x -> x + a) as mycolumn FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));

        // Aggregated column as lambda argument
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT apply(sum(a), x -> x * x) as mycolumn FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));

        // Prune columns that are not used in lambda expression
        assertUtilizedTableColumns("SELECT apply(0, x -> x + a) FROM (SELECT * FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));

        // Column references in transform
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT transform(c, x -> x * b) as mycolumn FROM t7)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t7"), ImmutableSet.of("b", "c"))));

        // Column references in map_filter
        assertUtilizedTableColumns("SELECT map_filter(x, (k, v) -> k = a) FROM (SELECT a, b, MAP(c, d) as x FROM t7)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t7"), ImmutableSet.of("a", "c", "d"))));

        // Column references in reduce
        assertUtilizedTableColumns("SELECT reduce(c, a, (s, x) -> s + x, s -> b * s) FROM (SELECT * FROM t7)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t7"), ImmutableSet.of("a", "b", "c"))));
    }

    @Test
    public void testConditionals()
    {
        // If expression with subquery
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT if(false, a, (SELECT max(a) FROM t2)) as mycolumn FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"))));

        // Simple case expression with subquery
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT CASE a WHEN 1 THEN 1 WHEN 2 THEN (SELECT max(a) FROM t2) ELSE 0 END as mycolumn FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"))));

        // Searched case expression with subquery
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT CASE WHEN a = 1 THEN 1 WHEN b = 2 THEN (SELECT max(a) FROM t2) ELSE 0 END as mycolumn FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"))));

        // Coalesce expression with subquery
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT COALESCE(a, b, (SELECT max(a) FROM t2), (SELECT max(b) FROM t2)) as mycolumn FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a", "b"))));

        // Nullif expression with subquery
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT NULLIF(a, (SELECT max(a) FROM t2)) as mycolumn FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"))));

        // Try expression with subquery
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT TRY(a / (SELECT max(a) FROM t2)) as mycolumn FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"), QualifiedObjectName.valueOf("tpch.s1.t2"), ImmutableSet.of("a"))));
    }

    @Test
    public void testConstructors()
    {
        // Array constructor
        assertUtilizedTableColumns("SELECT * FROM (SELECT ARRAY[a, b] FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));
        assertUtilizedTableColumns("SELECT a FROM t7 WHERE ARRAY[t7.b] = c",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t7"), ImmutableSet.of("a", "b", "c"))));
        assertUtilizedTableColumns("SELECT c FROM (SELECT ARRAY[a, b], c FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("c"))));

        // Map constructor
        assertUtilizedTableColumns("SELECT * FROM (SELECT MAP(array_agg(a), array_agg(b)) FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));

        // Row constructor
        assertUtilizedTableColumns("SELECT * FROM (SELECT ROW(a, b) FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));
    }

    @Test
    public void testDereference()
    {
        assertUtilizedTableColumns(
                "SELECT b.x.y FROM t10",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t10"), ImmutableSet.of("b"))));
    }

    @Test
    public void testNoPruningWhenShortCircuited()
    {
        // Should not prune even if conditional is constant
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT if(false, a, b) as mycolumn FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));

        // Should not prune even if WHERE condition can be short-circuited
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT a as mycolumn FROM t1 WHERE b > 0 AND false AND c < 10)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"))));
    }

    @Test
    public void testUDF()
    {
        // Column reference in UDF in filter
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT a as mycolumn FROM t1 WHERE unittest.memory.square(b) > 0)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b"))));

        // Prune column reference in UDF in select
        assertUtilizedTableColumns("SELECT mycolumn FROM (SELECT a as mycolumn, unittest.memory.square(b) FROM t1)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));
    }

    @Test
    public void testCteWithExpressionInSelect()
    {
        assertUtilizedTableColumns(
                "with cte as (select x as c1, y as c2, z + 1 as c3 from t13) select c1, c3 from (select * from cte)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t13"), ImmutableSet.of("x", "z"))));
    }

    @Test
    public void testMultipleCtes()
    {
        assertUtilizedTableColumns(
                "with cte1 as (select x as c1, y as c2, z + 1 as c3 from t13), cte2 as (select c1 + 1 as a, c3 as b from cte1) select a, b from (select * from cte2)",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t13"), ImmutableSet.of("x", "z"))));
        assertUtilizedTableColumns(
                "with cte1 as (select x as c1, z + 1 as c2, y from t13), cte2 as (select c1 + 1 c3, c2 +1 as c4 from cte1) select c3 from cte2",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t13"), ImmutableSet.of("x"))));
    }

    @Test
    public void testMultipleCtesUsingSameTable()
    {
        assertUtilizedTableColumns(
                "with cte1 as (select x + 1 as c1, y as c2, z + 1 as c3 from t13), cte2 as (with cte1 AS (select y +1 as c1 from t13) select * from cte1) SELECT cte1.c1, cte2.c1 from cte1 join cte2 on cte1.c1=cte2.c1",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t13"), ImmutableSet.of("x", "y"))));
    }

    @Test
    public void testMultipleCtesSameNameSameTable()
    {
        // TODO(kevintang2022): Fix visitWithQuery so that it looks up relation properly.
        // Currently, it just looks the relations using the name (string) of the CTE
        assertUtilizedTableColumns(
                "with cte1 as (select x + 1 as c1 from t13), cte2 as (with cte1 as (select x + 1 as c2, y + 1 as c3, z as c4 from t13) select * from cte1) select cte1.c1, cte2.c3 from cte1 join cte2 on true",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t13"), ImmutableSet.of("x", "y", "z"))));
    }

    @Test
    public void testMultipleCtesSameNameDifferentTable()
    {
        // This happens because the two CTE1s having the same name, and the CTE1 of t13 has its first column utilized, so the CTE1 of t1 will also have its first column added.
        assertUtilizedTableColumns(
                "with cte1 as (select x + 1 as c1 from t13), cte2 as (with cte1 as (select a + 1 as c2, b + 1 as c3, c as c4 from t1) select * from cte1) select cte1.c1 from cte1 join cte2 on true",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t13"), ImmutableSet.of("x"),
                        QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a"))));
    }

    @Test
    public void testMultipleCtesDifferentNameSameTable()
    {
        assertUtilizedTableColumns(
                "with cte1 as (select x + 1 as c1 from t13), cte2 as (with cte3 as (select x + 1 as c2, y + 1 as c3, z as c4 from t13) select * from cte3) select cte1.c1, cte2.c3 from cte1 join cte2 on true",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t13"), ImmutableSet.of("x", "y"))));
    }

    @Test
    public void testMultipleCtesDifferentNameDifferentTable()
    {
        // This is the same behavior for join queries like "select t1.a from t1 join t13 on true" regardless of wrapping in CTE
        assertUtilizedTableColumns(
                "with cte1 as (select x + 1 as c1 from t13), cte2 as (with cte3 as (select a + 1 as c2, b + 1 as c3, c as c4 from t1) select * from cte3) select cte1.c1 from cte1 join cte2 on true",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t13"), ImmutableSet.of("x"),
                        QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of())));
    }

    @Test
    public void testMultipleCtesSameNameDifferentTableMultipleColumnsUtilized()
    {
        // This will fallback to checking all utilized columns on t1 because CTE1 of t13 only has one item in the select expression, but CTE1 of t1 has 3, so there's a mismatch.
        // It's trying to add the third column of CTE1 of t1 but it does not exist.
        assertUtilizedTableColumns(
                "with cte1 as (select x + 1 as c1 from t13), cte2 as (with cte1 as (select a + 1 as c2, b + 1 as c3, c as c4 from t1) select * from cte1) select cte1.c1, cte2.c4 from cte1 join cte2 on true",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t13"), ImmutableSet.of("x"),
                        QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b", "c"))));
    }

    public void testInvokerView()
    {
        assertUtilizedTableColumns("SELECT view_invoker1.a, view_invoker1.c, view_invoker2.y FROM view_invoker1 left join view_invoker2 on view_invoker2.y = view_invoker1.c",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(
                        QualifiedObjectName.valueOf("tpch.s1.view_invoker1"), ImmutableSet.of("a", "c"),
                        QualifiedObjectName.valueOf("tpch.s1.view_invoker2"), ImmutableSet.of("y"),
                        QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "c"),
                        QualifiedObjectName.valueOf("tpch.s1.t13"), ImmutableSet.of("y"))));

        assertUtilizedTableColumns("SELECT view_invoker_with_cte1.c1, view_invoker_with_cte1.c3 FROM view_invoker_with_cte1",
                ImmutableMap.of("AllowAllAccessControl:user", ImmutableMap.of(
                        QualifiedObjectName.valueOf("tpch.s1.view_invoker_with_cte1"), ImmutableSet.of("c1", "c3"),
                        QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "c"))));
    }

    public void testDefinerView()
    {
        @Language("SQL") String query = "SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertUtilizedTableColumns(query,
                ImmutableMap.of("ViewAccessControl:different_user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "c")),
                        "AllowAllAccessControl:user", ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.view_invoker2"), ImmutableSet.of("y"),
                                QualifiedObjectName.valueOf("tpch.s1.view_definer1"), ImmutableSet.of("a", "c"),
                                QualifiedObjectName.valueOf("tpch.s1.t13"), ImmutableSet.of("y"))));
    }

    private String extractAccessControlInfo(AccessControlInfo accessControlInfo)
    {
        return accessControlInfo.getAccessControl().getClass().getSimpleName() + ":" + accessControlInfo.getIdentity().getUser();
    }

    private void assertUtilizedTableColumns(@Language("SQL") String query, Map<String, Map<QualifiedObjectName, Set<String>>> expected)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .readOnly()
                .execute(CLIENT_SESSION, session -> {
                    Analyzer analyzer = createAnalyzer(session, metadata, WarningCollector.NOOP, query);
                    Statement statement = SQL_PARSER.createStatement(query);
                    Analysis analysis = analyzer.analyze(statement);
                    assertEquals(analysis.getUtilizedTableColumnReferences().entrySet().stream().collect(Collectors.toMap(entry -> extractAccessControlInfo(entry.getKey()), Map.Entry::getValue)), expected);
                });
    }
}
