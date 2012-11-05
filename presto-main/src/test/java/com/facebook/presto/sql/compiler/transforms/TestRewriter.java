package com.facebook.presto.sql.compiler.transforms;

import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.TestingMetadata;
import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.compiler.NodeRewriter;
import com.facebook.presto.sql.compiler.SemanticAnalyzer;
import com.facebook.presto.sql.compiler.TreeRewriter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import org.antlr.runtime.RecognitionException;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeMethod;

public abstract class TestRewriter
{
    protected Metadata metadata;

    protected abstract NodeRewriter<?> getRewriter(Metadata metadata, Node tree);

    @BeforeMethod
    public void setup()
            throws Exception
    {
        TableMetadata ordersTable = new TableMetadata("default", "default", "ORDERS", ImmutableList.of(
                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "orderkey"),
                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "custkey"),
                new ColumnMetadata(TupleInfo.Type.DOUBLE, "totalprice"),
                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "orderdate"),
                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "orderstatus"),
                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "orderpriority"),
                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "clerk"),
                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "shippriority"),
                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "comment")));

        TableMetadata lineItemTable = new TableMetadata("default", "default", "LINEITEM", ImmutableList.of(
                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "orderkey"),
                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "partkey"),
                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "suppkey"),
                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "linenumber"),
                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "quantity"),
                new ColumnMetadata(TupleInfo.Type.DOUBLE, "extendedprice"),
                new ColumnMetadata(TupleInfo.Type.DOUBLE, "discount"),
                new ColumnMetadata(TupleInfo.Type.DOUBLE, "tax"),
                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "returnflag"),
                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "linestatus"),
                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "shipdate"),
                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "commitdate"),
                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "receiptdate"),
                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "shipinstruct"),
                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "shipmode"),
                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "comment")));

        metadata = new TestingMetadata();
        metadata.createTable(ordersTable);
        metadata.createTable(lineItemTable);
    }

    protected void assertRewrite(@Language("SQL") String actual, @Language("SQL") String expected)
            throws RecognitionException
    {
        Statement actualTree = SqlParser.createStatement(actual);
        Statement expectedTree = SqlParser.createStatement(expected);

        NodeRewriter<?> rewriter = getRewriter(metadata, actualTree);

        assertValidQuery(actualTree, metadata);
        assertValidQuery(expectedTree, metadata); // sanity check to make sure expected query is valid

        assertEqualsTree(TreeRewriter.rewriteWith(rewriter, actualTree), expectedTree);
    }

    public static <T extends Node> void assertEqualsTree(T actual, T expected)
    {
        if (!expected.equals(actual)) {
            throw new AssertionError(String.format("Expected: %s\nActual:%s", SqlFormatter.toString(expected), SqlFormatter.toString(actual)));
        }
    }

    public static <T extends Node> void assertValidQuery(T query, Metadata metadata)
    {
        new SemanticAnalyzer(metadata).analyze(query);
    }
}
