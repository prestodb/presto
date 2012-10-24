package com.facebook.presto.sql.compiler.transforms;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.compiler.NameGenerator;
import com.facebook.presto.sql.compiler.SessionMetadata;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

public class TestExtractAggregates
    extends TestRewriter
{
    @Override
    protected ExtractAggregates getRewriter(Metadata metadata, Node tree)
    {
        NameGenerator nameGenerator = new NameGenerator(ImmutableSet.<String>of(), ImmutableSet.<QualifiedName>of());
        return new ExtractAggregates(new SessionMetadata(metadata), nameGenerator);
    }

    @Test
    public void testSimple()
            throws Exception
    {
        assertRewrite(
                "SELECT SUM(totalprice) s FROM ORDERS",
                "SELECT _R0._a0 s\n" +
                        "FROM (\n" +
                        "   SELECT SUM(totalprice) _a0\n" +
                        "   FROM ORDERS\n" +
                        ") _R0");
    }


    @Test
    public void testRepeatedAggregation()
            throws Exception
    {
        assertRewrite(
                "SELECT SUM(totalprice) v1, SUM(totalprice) v2 FROM ORDERS",
                "SELECT _R0._a0 v1, _R0._a0 v2\n" +
                        "FROM (\n" +
                        "   SELECT SUM(totalprice) _a0\n" +
                        "   FROM ORDERS\n" +
                        ") _R0"
        );
    }

    @Test
    public void testArithmeticExpression()
            throws Exception
    {
        assertRewrite(
                "SELECT (SUM(totalprice) + AVG(totalprice)) s FROM ORDERS",
                "SELECT (_R0._a0 + _R0._a1) s\n" +
                        "FROM (\n" +
                        "   SELECT SUM(totalprice) _a0, AVG(totalprice) _a1\n" +
                        "   FROM ORDERS\n" +
                        ") _R0");
    }

    @Test
    public void testAggregateWithNested()
            throws Exception
    {
        assertRewrite(
                "SELECT id id, SUM(v) s \n" +
                        "FROM (\n" +
                        "   SELECT orderkey id, totalprice v \n" +
                        "   FROM ORDERS \n" +
                        ") U \n" +
                        "GROUP BY id",
                "SELECT _R0._a0 id, _R0._a1 s\n" +
                        "FROM (\n" +
                        "   SELECT id _a0, SUM(v) _a1\n" +
                        "   FROM (\n" +
                        "      SELECT orderkey id, totalprice v\n" +
                        "      FROM ORDERS\n" +
                        "   ) U\n" +
                        "   GROUP BY id\n" +
                        ") _R0");
    }

    @Test
    public void testAggregateWithWhereClause()
            throws Exception
    {
        assertRewrite(
                "SELECT orderkey id, SUM(totalprice) * AVG(totalprice) c\n" +
                        "FROM ORDERS\n" +
                        "WHERE totalprice > 10 \n" +
                        "GROUP BY orderkey",
                "SELECT _R0._a0 id, (_R0._a1 * _R0._a2) c\n" +
                        "FROM (\n" +
                        "   SELECT orderkey _a0, SUM(totalprice) _a1, AVG(totalprice) _a2\n" +
                        "   FROM ORDERS\n" +
                        "   WHERE totalprice > 10" +
                        "   GROUP BY orderkey\n" +
                        ") _R0"
        );
    }

    @Test
    public void testNestedGroupBy()
            throws Exception
    {
        assertRewrite(
                "SELECT id id, m m\n" +
                        "FROM ( \n" +
                        "   SELECT orderkey id, SUM(totalprice) + 1 m \n" +
                        "   FROM ORDERS \n" +
                        "   WHERE totalprice > 10 \n" +
                        "   GROUP BY orderkey \n" +
                        ") U \n",
                "SELECT id id, m m\n" +
                        "FROM (\n" +
                        "   SELECT _R0._a0 id, (_R0._a1 + 1) m\n" +
                        "   FROM (\n" +
                        "      SELECT orderkey _a0, SUM(totalprice) _a1\n" +
                        "      FROM ORDERS\n" +
                        "      WHERE totalprice > 10\n" +
                        "      GROUP BY orderkey\n" +
                        "   ) _R0\n" +
                        ") U"
        );
    }
}
