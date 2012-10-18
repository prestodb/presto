package com.facebook.presto.sql.compiler.transforms;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.compiler.AnalysisResult;
import com.facebook.presto.sql.compiler.SemanticAnalyzer;
import com.facebook.presto.sql.compiler.TreeRewriter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Statement;
import org.antlr.runtime.RecognitionException;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

public class TestQualifyNames
        extends TestRewriter
{
    @Override
    protected QualifyNames getRewriter(Metadata metadata, Node tree)
    {
        SemanticAnalyzer analyzer = new SemanticAnalyzer(metadata);
        AnalysisResult result = analyzer.analyze(tree);
        return new QualifyNames(result.getResolvedNames());
    }

    @Test
    public void testSimple()
            throws Exception
    {
        assertRewrite(
                "SELECT orderkey FROM ORDERS",
                "SELECT ORDERS.orderkey FROM ORDERS");
    }

    @Test
    public void testGroupBy()
            throws RecognitionException
    {
        assertRewrite(
                "SELECT orderkey FROM ORDERS GROUP BY orderkey",
                "SELECT ORDERS.orderkey FROM ORDERS GROUP BY ORDERS.orderkey");
    }

    @Test
    public void testAliased()
            throws RecognitionException
    {
        assertRewrite(
                "SELECT orderkey x FROM ORDERS GROUP BY orderkey",
                "SELECT ORDERS.orderkey x FROM ORDERS GROUP BY ORDERS.orderkey");
    }

    @Test
    public void testSubSelect()
            throws Exception
    {
        assertRewrite(
                "SELECT x FROM (\n" +
                        "   SELECT orderkey x \n" +
                        "   FROM ORDERS\n" +
                        ") U",
                "SELECT U.x FROM (\n" +
                        "   SELECT ORDERS.orderkey x \n" +
                        "   FROM ORDERS\n" +
                        ") U");
    }

    @Test
    public void testNestedSubquery()
            throws Exception
    {
        assertRewrite(
                "SELECT orderkey \n" +
                        "FROM ORDERS\n" +
                        "WHERE totalprice IN (" +
                        "   SELECT totalprice v " +
                        "   FROM ORDERS" +
                        ")",
                "SELECT ORDERS.orderkey \n" +
                        "FROM ORDERS\n" +
                        "WHERE ORDERS.totalprice IN (" +
                        "   SELECT ORDERS.totalprice v " +
                        "   FROM ORDERS" +
                        ")");
    }
}
