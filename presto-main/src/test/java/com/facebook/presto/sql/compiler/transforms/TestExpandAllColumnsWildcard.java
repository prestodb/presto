package com.facebook.presto.sql.compiler.transforms;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.tree.Node;
import org.antlr.runtime.RecognitionException;
import org.testng.annotations.Test;

public class TestExpandAllColumnsWildcard
        extends TestRewriter
{
    @Override
    protected ExpandAllColumnsWildcard getRewriter(Metadata metadata, Node tree)
    {
        return new ExpandAllColumnsWildcard(metadata);
    }

    @Test
    public void testFromTable()
            throws Exception
    {
        assertRewrite(
                "SELECT * FROM ORDERS",
                "SELECT ORDERS.orderkey orderkey, " +
                        "ORDERS.custkey custkey, " +
                        "ORDERS.totalprice totalprice, " +
                        "ORDERS.orderdate orderdate, " +
                        "ORDERS.orderstatus orderstatus, " +
                        "ORDERS.orderpriority orderpriority, " +
                        "ORDERS.clerk clerk, " +
                        "ORDERS.shippriority shippriority, " +
                        "ORDERS.comment comment\n" +
                        "FROM ORDERS"
        );
    }

    @Test
    public void testFromSubQuery()
            throws RecognitionException
    {
        assertRewrite(
                "SELECT * FROM (\n" +
                        "   SELECT orderkey id, totalprice value FROM ORDERS\n" +
                        ") U",
                "SELECT U.id id, U.value value FROM (\n" +
                        "   SELECT orderkey id, totalprice value FROM ORDERS\n" +
                        ") U"
        );
    }

    @Test
    public void testFromSubQueryWithAliases()
            throws Exception
    {
        assertRewrite(
                "SELECT * FROM (\n" +
                        "   SELECT orderkey x, totalprice y FROM ORDERS\n" +
                        ") U",
                "SELECT U.x x, U.y y FROM (\n" +
                        "   SELECT orderkey x, totalprice y FROM ORDERS\n" +
                        ") U"
        );
    }

    @Test(enabled = false) // TODO
    public void testFromSubQueryWithRepeatedAliases()
            throws Exception
    {
        assertRewrite(
                "SELECT * FROM (\n" +
                        "   SELECT orderkey x, totalprice x FROM ORDERS\n" +
                        ") U",
                "SELECT U._a0 x, U._a1 x FROM (\n" +
                        "   SELECT orderkey _a0, totalprice _a1 FROM ORDERS\n" +
                        ") U"
        );
    }

    @Test
    public void testFromTableWithPrefix()
            throws Exception
    {
        assertRewrite(
                "SELECT ORDERS.* FROM ORDERS",
                "SELECT ORDERS.orderkey orderkey, " +
                        "ORDERS.custkey custkey, " +
                        "ORDERS.totalprice totalprice, " +
                        "ORDERS.orderdate orderdate, " +
                        "ORDERS.orderstatus orderstatus, " +
                        "ORDERS.orderpriority orderpriority, " +
                        "ORDERS.clerk clerk, " +
                        "ORDERS.shippriority shippriority, " +
                        "ORDERS.comment comment\n" +
                        "FROM ORDERS"
        );
    }

    @Test
    public void testNested()
            throws RecognitionException
    {
        assertRewrite(
                "SELECT * FROM (\n" +
                        "SELECT * FROM ORDERS\n" +
                        ") U",
                "SELECT U.orderkey orderkey, " +
                        "U.custkey custkey, " +
                        "U.totalprice totalprice, " +
                        "U.orderdate orderdate, " +
                        "U.orderstatus orderstatus, " +
                        "U.orderpriority orderpriority, " +
                        "U.clerk clerk, " +
                        "U.shippriority shippriority, " +
                        "U.comment comment\n" +
                        "FROM (" +
                        "   SELECT ORDERS.orderkey orderkey, " +
                        "   ORDERS.custkey custkey, " +
                        "   ORDERS.totalprice totalprice, " +
                        "   ORDERS.orderdate orderdate, " +
                        "   ORDERS.orderstatus orderstatus, " +
                        "   ORDERS.orderpriority orderpriority, " +
                        "   ORDERS.clerk clerk, " +
                        "   ORDERS.shippriority shippriority, " +
                        "   ORDERS.comment comment\n" +
                        "   FROM ORDERS" +
                        ") U"
        );
    }

    @Test
    public void testFromMultipleTables()
            throws RecognitionException
    {
        assertRewrite(
                "SELECT ORDERS.*, LINEITEM.* FROM ORDERS, LINEITEM",
                "SELECT ORDERS.orderkey orderkey, " +
                        "ORDERS.custkey custkey, " +
                        "ORDERS.totalprice totalprice, " +
                        "ORDERS.orderdate orderdate, " +
                        "ORDERS.orderstatus orderstatus, " +
                        "ORDERS.orderpriority orderpriority, " +
                        "ORDERS.clerk clerk, " +
                        "ORDERS.shippriority shippriority, " +
                        "ORDERS.comment comment, " +
                        "LINEITEM.orderkey orderkey, " +
                        "LINEITEM.partkey partkey, " +
                        "LINEITEM.suppkey suppkey, " +
                        "LINEITEM.linenumber linenumber, " +
                        "LINEITEM.quantity quantity, " +
                        "LINEITEM.extendedprice extendedprice, " +
                        "LINEITEM.discount discount, " +
                        "LINEITEM.tax tax, " +
                        "LINEITEM.returnflag returnflag, " +
                        "LINEITEM.linestatus linestatus, " +
                        "LINEITEM.shipdate shipdate, " +
                        "LINEITEM.commitdate commitdate, " +
                        "LINEITEM.receiptdate receiptdate, " +
                        "LINEITEM.shipinstruct shipinstruct, " +
                        "LINEITEM.shipmode shipmode, " +
                        "LINEITEM.comment comment\n" +
                        "FROM ORDERS, LINEITEM"
        );

    }
}
