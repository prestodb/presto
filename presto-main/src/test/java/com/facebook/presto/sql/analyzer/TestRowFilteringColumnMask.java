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

import org.testng.annotations.Test;

/**
 * Created by localadmin on 9/11/18.
 */
public class TestRowFilteringColumnMask
        extends TestAnalyzerBase
{
    @Test
    public void testSanityQuery()
    {
        analyzeWithRowColRewrite("SELECT * FROM s1.t1 ");
    }

    @Test
    public void testGroupByWithSubquerySelectExpression()
    {
        analyzeWithRowColRewrite("SELECT (SELECT t1.a) FROM t1 GROUP BY a");
        analyzeWithRowColRewrite("SELECT (SELECT a) FROM t1 GROUP BY t1.a");

        // u.a is not GROUP-ed BY and it is used in select Subquery expression
        analyzeWithRowColRewrite("SELECT (SELECT u.a FROM (values 1) u(a)) " +
                "FROM t1 u GROUP BY b");

        // u.a is not GROUP-ed BY but select Subquery expression is using a different (shadowing) u.a
        analyzeWithRowColRewrite("SELECT (SELECT 1 FROM t1 u WHERE a = u.a) FROM t1 u GROUP BY b", "SELECT (SELECT 1 FROM t1 u WHERE a = u.a) FROM ((select a,ceil(b) b,c,d from t1 where abs(a)>3) t1) u GROUP BY b");
    }

    @Test
    public void testGroupByWithExistsSelectExpression()
    {
        analyzeWithRowColRewrite("SELECT EXISTS(SELECT t1.a) FROM t1 GROUP BY a");
        analyzeWithRowColRewrite("SELECT EXISTS(SELECT a) FROM t1 GROUP BY t1.a");

        // u.a is not GROUP-ed BY and it is used in select Subquery expression
        analyzeWithRowColRewrite("SELECT EXISTS(SELECT u.a FROM (values 1) u(a)) " +
                "FROM t1 u GROUP BY b");

        analyzeWithRowColRewrite("SELECT EXISTS(SELECT 1 FROM t1 u WHERE a = u.a) FROM t1 u GROUP BY b", "SELECT EXISTS(SELECT 1 FROM t1 u WHERE a = u.a) FROM ((select a,ceil(b) b,c,d from t1 where abs(a)>3) t1) u GROUP BY b ");
    }

    @Test
    public void testGrouping()
    {
        analyzeWithRowColRewrite("SELECT a, b, sum(c), grouping(a, b) FROM t1 GROUP BY GROUPING SETS ((a), (a, b))");
        analyzeWithRowColRewrite("SELECT grouping(t1.a) FROM t1 GROUP BY a");
        analyzeWithRowColRewrite("SELECT grouping(b) FROM t1 GROUP BY t1.b");
        analyzeWithRowColRewrite("SELECT grouping(a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a) FROM t1 GROUP BY a");
    }

    @Test
    public void testNonDeterministicOrderBy()
    {
        analyzeWithRowColRewrite("SELECT DISTINCT random() as b FROM t1 ORDER BY b");
        analyzeWithRowColRewrite("SELECT random() FROM t1 ORDER BY random()");
        analyzeWithRowColRewrite("SELECT a FROM t1 ORDER BY random()");
    }

    @Test
    public void testDistinctAggregations()
    {
        analyzeWithRowColRewrite("SELECT COUNT(DISTINCT a) as a, SUM(a) FROM t1 ");
    }

    @Test
    public void testOrderByExpressionOnOutputColumn()
    {
        // TODO: analyze output
        analyzeWithRowColRewrite("SELECT a x FROM t1 ORDER BY x + 1");
        analyzeWithRowColRewrite("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY max(b*1e0)");
    }

    @Test
    public void testOrderByExpressionOnOutputColumn2()
    {
        // TODO: validate output
        analyzeWithRowColRewrite("SELECT a x FROM t1 ORDER BY a + 1");
    }

    @Test
    public void testOrderByWithWildcard()
    {
        // TODO: validate output
        analyzeWithRowColRewrite("SELECT t1.* FROM t1 ORDER BY a");
    }

    @Test
    public void testOrderByWithGroupByAndSubquerySelectExpression()
    {
        analyzeWithRowColRewrite("SELECT a FROM t1 GROUP BY a ORDER BY (SELECT a)");
        analyzeWithRowColRewrite("SELECT a AS b FROM t1 GROUP BY t1.a ORDER BY (SELECT b)");

        analyzeWithRowColRewrite("SELECT a FROM t1 GROUP BY a ORDER BY MAX((SELECT x FROM (VALUES 4) t(x)))");

        analyzeWithRowColRewrite("SELECT CAST(ROW(1) AS ROW(someField BIGINT)) AS x\n" +
                "FROM (VALUES (1, 2)) t(a, b)\n" +
                "GROUP BY b\n" +
                "ORDER BY (SELECT x.someField)");
    }

    @Test
    public void testJoinOnConstantExpression()
    {
        analyzeWithRowColRewrite("SELECT * FROM t1 JOIN t2 ON 1 = 1");
    }

    @Test
    public void testNonEquiOuterJoin()
    {
        analyzeWithRowColRewrite("SELECT * FROM t1 LEFT JOIN t2 ON t1.a + t2.a = 1");
        analyzeWithRowColRewrite("SELECT * FROM t1 RIGHT JOIN t2 ON t1.a + t2.a = 1");
        analyzeWithRowColRewrite("SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a OR t1.b = t2.b");
    }

    @Test
    public void testImplicitCrossJoin()
    {
        // TODO: validate output
        analyzeWithRowColRewrite("SELECT * FROM t1 , t2 ");
    }

    @Test
    public void testGroupByOrdinalsWithWildcard()
    {
        // TODO: verify output
        analyzeWithRowColRewrite("SELECT t1.*, a FROM t1 GROUP BY 1,2,c,d");
    }

    @Test
    public void testGroupByWithQualifiedName2()
    {
        // TODO: verify output
        analyzeWithRowColRewrite("SELECT t1.a FROM t1 GROUP BY a");
    }

    @Test
    public void testGroupByWithQualifiedName3()
    {
        // TODO: verify output
        analyzeWithRowColRewrite("SELECT * FROM t1 GROUP BY t1.a, t1.b, t1.c, t1.d");
    }

    @Test
    public void testGroupByWithRowExpression()
    {
        // TODO: verify output
        analyzeWithRowColRewrite("SELECT (a, b) FROM t1 GROUP BY a, b");
    }

    @Test
    public void testHaving()
    {
        // TODO: verify output
        analyzeWithRowColRewrite("SELECT sum(a) FROM t1 HAVING avg(a) - avg(b) > 10");
    }

    @Test
    public void testWithCaseInsensitiveResolution()
    {
        // TODO: verify output
        analyzeWithRowColRewrite("WITH AB AS (SELECT * FROM t1 ) SELECT * FROM ab");
    }

    @Test
    public void testStartTransaction()
    {
        analyzeWithRowColRewrite("START TRANSACTION");
        analyzeWithRowColRewrite("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
    }

    @Test
    public void testCommit()
    {
        analyzeWithRowColRewrite("COMMIT");
        analyzeWithRowColRewrite("COMMIT WORK");
    }

    @Test
    public void testRollback()
    {
        analyzeWithRowColRewrite("ROLLBACK");
        analyzeWithRowColRewrite("ROLLBACK WORK");
    }

    @Test
    public void testExplainAnalyze()
    {
        analyzeWithRowColRewrite("EXPLAIN ANALYZE SELECT * FROM t1");
    }

    @Test
    public void testInsert()
    {
        analyzeWithRowColRewrite("INSERT INTO t1 SELECT * FROM t1", "INSERT INTO t1 SELECT * from ((select a,ceil(b) b,c,d from t1 where abs(a)>3) t1)");
        analyzeWithRowColRewrite("INSERT INTO t3 SELECT a, b FROM t1", "INSERT INTO t3 SELECT a,b from ((select a,ceil(b) b,c,d from t1 where abs(a)>3) t1)");
        analyzeWithRowColRewrite("INSERT INTO t1 (a,b,c,d) SELECT d,b,c,a from t1", "INSERT INTO t1 (a,b,c,d) SELECT d,b,c,a from ((select a,ceil(b) b,c,d from t1 where abs(a)>3) t1)");
    }

    @Test
    public void testGroupBy()
    {
        // TODO: validate output
        analyzeWithRowColRewrite("SELECT a, SUM(b) FROM t1 GROUP BY a");
    }

    @Test
    public void testSingleGroupingSet()
    {
        // TODO: validate output
        analyzeWithRowColRewrite("SELECT SUM(b) FROM t1 GROUP BY ()");
        analyzeWithRowColRewrite("SELECT SUM(b) FROM t1 GROUP BY GROUPING SETS (())");
        analyzeWithRowColRewrite("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS (a)");
        analyzeWithRowColRewrite("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS (a)");
        analyzeWithRowColRewrite("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b))");
    }

    @Test
    public void testMultipleGroupingSetMultipleColumns()
    {
        // TODO: validate output
        analyzeWithRowColRewrite("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b), (c, d))");
        analyzeWithRowColRewrite("SELECT a, SUM(b) FROM t1 GROUP BY a, b, GROUPING SETS ((c, d))");
        analyzeWithRowColRewrite("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a), (c, d))");
        analyzeWithRowColRewrite("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b)), ROLLUP (c, d)");
        analyzeWithRowColRewrite("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b)), CUBE (c, d)");
    }

    @Test
    public void testCreateTable()
    {
        analyzeWithRowColRewrite("CREATE TABLE test(a, b) AS SELECT 1, 2");
        analyzeWithRowColRewrite("CREATE TABLE t10(a,b) as select a,b from t1 ");
    }

    @Test
    public void testCreateSchema()
    {
        analyzeWithRowColRewrite("CREATE SCHEMA test");
        analyzeWithRowColRewrite("CREATE SCHEMA test WITH (p1 = 'p1')");
    }

//    @Test
//    public void testExistingRecursiveView()
//    {
//        analyzeWithRowColRewrite("SELECT * FROM v1 a JOIN v1 b ON a.a = b.a");
//        analyzeWithRowColRewrite("SELECT * FROM v1 a JOIN (SELECT * from v1) b ON a.a = b.a");
//    }

//    @Test
//    public void testShowCreateView()
//    {
//        analyzeWithRowColRewrite("SHOW CREATE VIEW v1","");
//        analyzeWithRowColRewrite("SHOW CREATE VIEW v2","");
//    }

//    @Test
//    public void testStoredViewAnalysisScoping()
//    {
//        // the view must not be analyzed using the query context
//        analyzeWithRowColRewrite("WITH t1 AS (SELECT 123 x) SELECT * FROM v1");
//    }
//
//    @Test
//    public void testStoredViewResolution()
//    {
//        // the view must be analyzed relative to its own catalog/schema
//        analyzeWithRowColRewrite("SELECT * FROM c3.s3.v3");
//    }
//
//    @Test
//    public void testQualifiedViewColumnResolution()
//    {
//        // it should be possible to qualify the column reference with the view name
//        analyzeWithRowColRewrite("SELECT v1.a FROM v1");
//    }
//
//    @Test
//    public void testViewWithUppercaseColumn()
//    {
//        analyzeWithRowColRewrite("SELECT * FROM v4");
//    }

    @Test
    public void testLambda()
    {
        analyzeWithRowColRewrite("SELECT apply(5, x -> abs(x)) from t1 ");
    }

    @Test
    public void testLambdaCapture()
    {
        analyzeWithRowColRewrite("SELECT apply(c1, x -> x + c2) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(c1, c2)");
        analyzeWithRowColRewrite("SELECT apply(c1 + 10, x -> apply(x + 100, y -> c1)) FROM (VALUES 1) t(c1)");

        // reference lambda variable of the not-immediately-enclosing lambda
        analyzeWithRowColRewrite("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 1000) t(x)");
        analyzeWithRowColRewrite("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 'abc') t(x)");
        analyzeWithRowColRewrite("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 1000) t(x)");
        analyzeWithRowColRewrite("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 'abc') t(x)");
    }

    @Test
    public void testLambdaInAggregationContext()
    {
        analyzeWithRowColRewrite("SELECT apply(sum(0), a -> a * a) FROM t1 ");
        analyzeWithRowColRewrite("SELECT apply(x, i -> i - 1), sum(y) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) group by x");
    }

    @Test
    public void testLambdaInSubqueryContext()
    {
        // GROUP BY column captured in lambda
        analyzeWithRowColRewrite(
                "SELECT (SELECT apply(0, x -> x + b) FROM (VALUES 1) x(a)) FROM t1 u GROUP BY b");

        // name shadowing
        analyzeWithRowColRewrite("SELECT (SELECT apply(0, x -> x + a) FROM (VALUES 1) x(a)) FROM t1 u GROUP BY b");
        analyzeWithRowColRewrite("SELECT (SELECT apply(0, a -> a + a)) FROM t1 u GROUP BY b");
    }

    @Test
    public void testLambdaWithSubqueryInOrderBy()
    {
        analyzeWithRowColRewrite("SELECT a FROM t1 ORDER BY (SELECT apply(0, x -> x + a))");
        analyzeWithRowColRewrite("SELECT a AS output_column FROM t1 ORDER BY (SELECT apply(0, x -> x + output_column))");
        analyzeWithRowColRewrite("SELECT count(*) FROM t1 GROUP BY a ORDER BY (SELECT apply(0, x -> x + a))");
        analyzeWithRowColRewrite("SELECT count(*) AS output_column FROM t1 GROUP BY a ORDER BY (SELECT apply(0, x -> x + output_column))");
    }

    @Test
    public void testHiddenColumn()
    {
        analyzeWithRowColRewrite("SELECT a FROM t5", "SELECT a from (SELECT ceil(a) a from t5 where abs(a)>3) t5");
    }

    @Test
    public void testWith()
    {
        analyzeWithRowColRewrite("with Test as ( select a from t1 ) select a from Test ");
    }

    @Test
    public void testColWithoutPerms()
    {
        analyzeWithRowColRewrite(" select a from t3", " select a from ( select ceil(a) a from t3 where abs(a)> 3 ) t3 ");
    }
}
