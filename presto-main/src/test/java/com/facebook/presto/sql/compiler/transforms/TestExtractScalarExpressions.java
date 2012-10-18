package com.facebook.presto.sql.compiler.transforms;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.compiler.NameGenerator;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

public class TestExtractScalarExpressions
        extends TestRewriter
{
    @Override
    protected ExtractScalarExpressions getRewriter(Metadata metadata, Node tree)
    {
        return new ExtractScalarExpressions(metadata, new NameGenerator(ImmutableSet.<String>of(), ImmutableSet.<QualifiedName>of()));
    }

    @Test
    public void testSimple()
            throws Exception
    {
        assertRewrite(
                "SELECT SUM(ORDERS.orderkey + ORDERS.custkey) x FROM ORDERS",
                "SELECT SUM(_R0._a0) x\n" +
                        "FROM (\n" +
                        "   SELECT ORDERS.orderkey + ORDERS.custkey _a0\n" +
                        "   FROM ORDERS\n" +
                        ") _R0");
    }

    @Test
    public void testMultipleExpressions()
            throws Exception
    {
        assertRewrite(
                "SELECT SUM(ORDERS.orderkey + ORDERS.custkey) x, AVG(ORDERS.orderkey * 2) y FROM ORDERS",
                "SELECT SUM(_R0._a0) x, AVG(_R0._a1) y\n" +
                        "FROM (\n" +
                        "   SELECT (ORDERS.orderkey + ORDERS.custkey) _a0, (ORDERS.orderkey * 2) _a1\n" +
                        "   FROM ORDERS\n" +
                        ") _R0");
    }

    @Test
    public void testWithGroupByKey()
            throws Exception
    {
        assertRewrite(
                "SELECT ORDERS.orderkey key, SUM(orderkey + custkey) sum\n" +
                        "FROM ORDERS\n" +
                        "GROUP BY ORDERS.orderkey",
                "SELECT _R0._a0 key, SUM(_R0._a1) sum\n" +
                        "FROM (\n" +
                        "   SELECT ORDERS.orderkey _a0, (orderkey + custkey) _a1\n" +
                        "   FROM ORDERS\n" +
                        ") _R0\n" +
                        "GROUP BY _R0._a0");
    }

    @Test
    public void testNoOp()
            throws Exception
    {
        assertRewrite(
                "SELECT SUM(ORDERS.totalprice) x FROM ORDERS",
                "SELECT SUM(ORDERS.totalprice) x FROM ORDERS");
    }
}
