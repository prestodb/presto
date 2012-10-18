package com.facebook.presto.sql.compiler.transforms;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.compiler.NameGenerator;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

public class TestMaterializeImplicitAliases
    extends TestRewriter
{
    @Override
    protected MaterializeImplicitAliases getRewriter(Metadata metadata, Node tree)
    {
        return new MaterializeImplicitAliases(new NameGenerator(ImmutableSet.<String>of(), ImmutableSet.<QualifiedName>of()));
    }

    @Test
    public void testSimple()
            throws Exception
    {
        assertRewrite(
                "SELECT orderkey FROM ORDERS",
                "SELECT orderkey orderkey FROM ORDERS"
        );
    }

    @Test
    public void testMultipleFields()
            throws Exception
    {
        assertRewrite(
                "SELECT orderkey, totalprice FROM ORDERS",
                "SELECT orderkey orderkey, totalprice totalprice FROM ORDERS"
        );
    }

    @Test
    public void testExpression()
            throws Exception
    {
        assertRewrite(
                "SELECT 1 FROM ORDERS",
                "SELECT 1 _a0 FROM ORDERS"
        );
    }
    @Test
    public void testNested()
            throws Exception
    {
        assertRewrite(
                "SELECT orderkey FROM (\n" +
                        "   SELECT orderkey " +
                        "   FROM ORDERS\n" +
                        ") U",
                "SELECT orderkey orderkey FROM (\n" +
                        "   SELECT orderkey orderkey " +
                        "   FROM ORDERS\n" +
                        ") U"
        );
    }

    @Test
    public void testQualified()
            throws Exception
    {
        assertRewrite(
                "SELECT ORDERS.orderkey FROM ORDERS",
                "SELECT ORDERS.orderkey orderkey FROM ORDERS"
        );
    }

    @Test
    public void testUnaliasedExpression()
            throws Exception
    {
        assertRewrite(
                "SELECT SUM(orderkey) FROM ORDERS",
                "SELECT SUM(orderkey) SUM FROM ORDERS"
        );
    }

    @Test
    public void testAlreadyAliased()
            throws Exception
    {
        assertRewrite(
                "SELECT ORDERS.orderkey orderkey FROM ORDERS",
                "SELECT ORDERS.orderkey orderkey FROM ORDERS"
        );
    }

    @Test
    public void testAllColumnWildcard()
            throws Exception
    {
        assertRewrite(
                "SELECT * FROM ORDERS",
                "SELECT * FROM ORDERS"
        );
    }

}
