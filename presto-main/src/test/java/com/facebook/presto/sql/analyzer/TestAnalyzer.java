package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.NativeColumnHandle;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.TestingMetadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestAnalyzer
{
    private Analyzer analyzer;

    @BeforeMethod(alwaysRun = true)
    public void setup()
            throws Exception
    {
        TestingMetadata metadata = new TestingMetadata();

        metadata.createTable(new TableMetadata(new QualifiedTableName("default", "default", "t1"),
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(1)),
                        new ColumnMetadata("b", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(2)),
                        new ColumnMetadata("c", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(3)),
                        new ColumnMetadata("d", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(4))
                ), new NativeTableHandle(1)));

        metadata.createTable(new TableMetadata(new QualifiedTableName("default", "default", "t2"),
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(1)),
                        new ColumnMetadata("b", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(2))
                ), new NativeTableHandle(2)));

        metadata.createTable(new TableMetadata(new QualifiedTableName("default", "default", "t3"),
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(1)),
                        new ColumnMetadata("b", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(2))
                ), new NativeTableHandle(3)));

        analyzer = new Analyzer(new Session(null, "default", "default"), metadata);
    }

    private void analyze(@Language("SQL") String query)
    {
        Statement statement = SqlParser.createStatement(query);
        analyzer.analyze(statement);
    }

    
    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = ".*'t1'.*")
    public void testDuplicateTable()
            throws Exception
    {
        analyze("SELECT * FROM t1 JOIN t1 USING (a)");
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = ".*'x'.*")
    public void testDuplicateTableAliases()
            throws Exception
    {
        analyze("SELECT * FROM t1 x JOIN t2 x USING (a)");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testHavingReferencesOutputAlias()
            throws Exception
    {
        analyze("SELECT sum(a) x FROM t1 HAVING x > 5");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testWildcardWithInvalidPrefix()
            throws Exception
    {
        analyze("SELECT foo.* FROM t1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testGroupByWithWildcard2()
            throws Exception
    {
        analyze("SELECT * FROM t1 GROUP BY 1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testGroupByInvalidOrdinal()
            throws Exception
    {
        analyze("SELECT * FROM t1 GROUP BY 10");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testGroupByInvalidOrdinal2()
            throws Exception
    {
        analyze("SELECT * FROM t1 GROUP BY 0");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testGroupByInvalidOrdinal3()
            throws Exception
    {
        analyze("SELECT * FROM t1 GROUP BY -1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testOrderByInvalidOrdinal()
            throws Exception
    {
        analyze("SELECT * FROM t1 ORDER BY 10");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testOrderByInvalidOrdinal2()
            throws Exception
    {
        analyze("SELECT * FROM t1 ORDER BY 0");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testAggregationInWhereClause()
            throws Exception
    {
        analyze("SELECT * FROM t1 WHERE sum(a) > 1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testNestedAggregation()
            throws Exception
    {
        analyze("SELECT sum(count(*)) FROM t1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testWindowFunctionInWhereClause()
            throws Exception
    {
        analyze("SELECT * FROM t1 WHERE foo() over () > 1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testAggregationsInGroupBy()
            throws Exception
    {
        analyze("SELECT * FROM t1 GROUP BY sum(a)");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testWindowFunctionsInGroupBy()
            throws Exception
    {
        analyze("SELECT * FROM t1 GROUP BY rank() over ()");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testInvalidTable()
            throws Exception
    {
        analyze("SELECT * FROM foo");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testNonAggregateInSelect()
            throws Exception
    {
        analyze("SELECT a, sum(b) FROM t1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testNonAggregateInSelect2()
            throws Exception
    {
        analyze("SELECT sum(b) / a FROM t1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testNonAggregateInSelect3()
            throws Exception
    {
        analyze("SELECT sum(b) / a FROM t1 group by c");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testNonAggregateInOrderBy()
            throws Exception
    {
        analyze("SELECT sum(b) FROM t1 ORDER BY a + 1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testNonAggregateInHaving()
            throws Exception
    {
        analyze("SELECT a, sum(b) FROM t1 GROUP BY a HAVING c > 5");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testInvalidAttributeInSelect()
            throws Exception
    {
        analyze("SELECT f FROM t1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testInvalidAttributeInOrderBy()
            throws Exception
    {
        analyze("SELECT * FROM t1 ORDER BY f");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testInvalidAttributeInGroupBy()
            throws Exception
    {
        analyze("SELECT count(*) FROM t1 GROUP BY f");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testInvalidAttributeInWhere()
            throws Exception
    {
        analyze("SELECT * FROM t1 WHERE f > 1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testOrderByMustAppearInSelectWithDistinct()
            throws Exception
    {
        analyze("SELECT DISTINCT a FROM t1 ORDER BY b");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testNonBooleanWhereClause()
            throws Exception
    {
        analyze("SELECT * FROM t1 WHERE a");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testDistinctAggregations()
            throws Exception
    {
        analyze("SELECT COUNT(DISTINCT a) FROM t1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testOrderByExpressionOnOutputColumn()
            throws Exception
    {
        analyze("SELECT a x FROM t1 ORDER BY x + 1");
    }

    @Test
    public void testOrderByExpressionOnOutputColumn2()
            throws Exception
    {
        // TODO: validate output
        analyze("SELECT a x FROM t1 ORDER BY a + 1");
    }

    @Test
    public void testOrderByWithWildcard()
            throws Exception
    {
        // TODO: validate output
        analyze("SELECT a, t1.* FROM t1 ORDER BY a");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testGroupByOrdinalWithWildcardAndAnonymousColumns()
            throws Exception
    {
        analyze("SELECT u1.*, u2.* FROM (select a, b + 1 from t1) u1 JOIN (select a, b + 2 from t1) u2 USING (a) GROUP BY u1.a, u2.a, 3");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testMismatchedColumnAliasCount()
            throws Exception
    {
        analyze("SELECT * FROM t1 u (x, y)");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testJoinOnConstantExpression()
            throws Exception
    {
        analyze("SELECT * FROM t1 JOIN t2 ON 1 = 1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testJoinOnAmbiguousName()
            throws Exception
    {
        analyze("SELECT * FROM t1 JOIN t2 ON a = a");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testAggregationInJoinClause()
            throws Exception
    {
        analyze("SELECT * FROM t1 JOIN t2 ON sum(t1.a) = t2.a");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testWindowFunctionInJoinClause()
            throws Exception
    {
        analyze("SELECT * FROM t1 JOIN t2 ON sum(t1.a) over () = t2.a");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testNonEquiJoin1()
            throws Exception
    {
        analyze("SELECT * FROM t1 JOIN t2 ON t1.a < t2.a");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testNonEquiJoin2()
            throws Exception
    {
        analyze("SELECT * FROM t1 JOIN t2 ON t1.a + t2.a = 1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testNonEquiJoin3()
            throws Exception
    {
        analyze("SELECT * FROM t1 JOIN t2 ON t1.a = t2.a OR t1.b = t2.b");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testNonBooleanHaving()
            throws Exception
    {
        analyze("SELECT sum(a) FROM t1 HAVING sum(a)");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testAmbiguousReferenceInOrderBy()
            throws Exception
    {
        analyze("SELECT a x, b x FROM t1 ORDER BY x");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testNaturalJoinNotSupported()
            throws Exception
    {
        analyze("SELECT * FROM t1 NATURAL JOIN t2");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testOuterJoinNotSupported()
            throws Exception
    {
        analyze("SELECT * FROM t1 LEFT OUTER JOIN t2 USING (a)");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testAggregateWithNestedWindowFunction()
            throws Exception
    {
        analyze("SELECT avg(sum(a) OVER ()) FROM t1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testNestedWindowFunctions1()
            throws Exception
    {
        analyze("SELECT sum(sum(a) OVER ()) OVER () FROM t1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testNestedWindowFunctions2()
            throws Exception
    {
        analyze("SELECT avg(a) OVER (PARTITION BY sum(b) OVER ()) FROM t1");
    }

    @Test(expectedExceptions = SemanticException.class)
    public void testNestedWindowFunction3()
            throws Exception
    {
        analyze("SELECT avg(a) OVER (ORDER BY sum(b) OVER ()) FROM t1");
    }

    @Test(expectedExceptions = SemanticException.class) // distinct in window args not yet supported
    public void testDistinctInWindowFunctionParameter()
            throws Exception
    {
        analyze("SELECT a, count(DISTINCT b) OVER () FROM t1");
    }

    @Test
    public void testGroupByOrdinalsWithWildcard()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT t1.*, a FROM t1 GROUP BY 1,2,c,d");
    }

    @Test
    public void testGroupByWithQualifiedName()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT a FROM t1 GROUP BY t1.a");
    }

    @Test
    public void testGroupByWithQualifiedName2()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT t1.a FROM t1 GROUP BY a");
    }

    @Test
    public void testGroupByWithQualifiedName3()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT * FROM t1 GROUP BY t1.a, t1.b, t1.c, t1.d");
    }

    @Test
    public void testHaving()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT sum(a) FROM t1 HAVING avg(a) - avg(b) > 10");
    }
}
