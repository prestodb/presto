package com.facebook.presto.sql.compiler.transforms;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.tree.Node;
import org.antlr.runtime.RecognitionException;
import org.testng.annotations.Test;

public class TestRemoveRedundantProjections
        extends TestRewriter
{
    @Override
    protected RemoveRedundantProjections getRewriter(Metadata metadata, Node tree)
    {
        return new RemoveRedundantProjections();
    }

    @Test
    public void testBasic()
            throws Exception
    {
        assertRewrite(
                "SELECT a a, b b FROM (\n" +
                        "SELECT orderkey a, totalprice b\n" +
                        "FROM ORDERS" +
                        ") U",
                "SELECT orderkey a, totalprice b FROM ORDERS"
        );
    }

    @Test
    public void testWithFullyQualifiedNames()
            throws Exception
    {
        assertRewrite(
                "SELECT U.a a, U.b b FROM (\n" +
                        "SELECT ORDERS.orderkey a, ORDERS.totalprice b\n" +
                        "FROM ORDERS" +
                        ") U",
                "SELECT ORDERS.orderkey a, ORDERS.totalprice b FROM ORDERS"
        );
    }

    @Test
    public void testDoesNotRewriteFunctions()
            throws Exception
    {
        assertRewrite(
                "SELECT SUM(a) s FROM (\n" +
                        "SELECT ORDERS.orderkey a FROM ORDERS\n" +
                        ") U",
                "SELECT SUM(a) s FROM (\n" +
                        "SELECT ORDERS.orderkey a FROM ORDERS\n" +
                        ") U"
        );
    }

    @Test
    public void testReorder()
            throws Exception
    {
        assertRewrite(
                "SELECT k k, v v \n" +
                        "FROM (\n" +
                        "   SELECT v v, k k \n" +
                        "   FROM (\n" +
                        "       SELECT orderstatus k, SUM(totalprice) v \n" +
                        "       FROM ORDERS \n" +
                        "       GROUP BY orderstatus\n" +
                        "   ) U\n" +
                        ") V",
                "SELECT k k, v v \n" +
                        "FROM (\n" +
                        "   SELECT orderstatus k, SUM(totalprice) v \n" +
                        "   FROM ORDERS \n" +
                        "   GROUP BY orderstatus" +
                        ") U"
        );
    }

    @Test
    public void testDoesNotUnwrapGroupBy()
            throws RecognitionException
    {
        assertRewrite(
                "SELECT sum sum\n" +
                        "FROM (\n" +
                        "   SELECT orderdate k, SUM(ORDERS.totalprice) sum\n" +
                        "   FROM ORDERS\n" +
                        "   GROUP BY orderdate\n" +
                        ") U",
                "SELECT sum sum\n" +
                        "FROM (\n" +
                        "   SELECT orderdate k, SUM(ORDERS.totalprice) sum\n" +
                        "   FROM ORDERS\n" +
                        "   GROUP BY orderdate\n" +
                        ") U"
        );
    }
}
