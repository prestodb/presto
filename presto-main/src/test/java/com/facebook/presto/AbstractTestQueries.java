package com.facebook.presto;

import com.facebook.presto.ingest.RecordCursor;
import com.facebook.presto.ingest.RecordSet;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.MaterializedResult;
import com.facebook.presto.util.TestingTpchBlocksProvider;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import org.intellij.lang.annotations.Language;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.PreparedBatchPart;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;
import static com.facebook.presto.util.TestingTpchBlocksProvider.readTpchRecords;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestQueries
{
    private Handle handle;
    private Metadata metadata;
    private TpchDataStreamProvider dataProvider;

    @Test
    public void testJoinWithMultiFieldGroupBy()
            throws Exception
    {
        assertQuery("SELECT orderstatus FROM lineitem JOIN (SELECT DISTINCT orderkey, orderstatus FROM ORDERS) T on lineitem.orderkey = T.orderkey");
    }

    @Test
    public void testGroupByMultipleFieldsWithPredicateOnAggregationArgument()
            throws Exception
    {
        assertQuery("SELECT custkey, orderstatus, MAX(orderkey) FROM ORDERS WHERE orderkey = 1 GROUP BY custkey, orderstatus");
    }

    @Test
    public void testReorderOutputsOfGroupByAggregation()
            throws Exception
    {
        assertQuery("SELECT orderstatus, a, custkey, b FROM (SELECT custkey, orderstatus, -COUNT(*) a, MAX(orderkey) b FROM ORDERS WHERE orderkey = 1 GROUP BY custkey, orderstatus) T");
    }

    @Test
    public void testGroupAggregationOverNestedGroupByAggregation()
            throws Exception
    {
        assertQuery("SELECT sum(custkey), max(orderstatus), min(c) FROM (SELECT orderstatus, custkey, COUNT(*) c FROM ORDERS GROUP BY orderstatus, custkey) T");
    }

    @Test
    public void testDistinctMultipleFields()
            throws Exception
    {
        assertQuery("SELECT DISTINCT custkey, orderstatus FROM ORDERS");
    }

    @Test
    public void testArithmeticNegation()
            throws Exception
    {
        assertQuery("SELECT -custkey FROM orders");
    }

    @Test
    public void testDistinct()
            throws Exception
    {
        assertQuery("SELECT DISTINCT custkey FROM orders");
    }

    // TODO: we need to properly propagate exceptions with their actual classes
    @Test(expectedExceptions = Exception.class, expectedExceptionsMessageRegExp = "DISTINCT in aggregation parameters not yet supported")
    public void testCountDistinct()
            throws Exception
    {
        assertQuery("SELECT COUNT(DISTINCT custkey) FROM orders");
    }

    @Test
    public void testDistinctWithOrderBy()
            throws Exception
    {
        assertQuery("SELECT DISTINCT custkey FROM orders ORDER BY custkey LIMIT 10");
    }

    @Test(expectedExceptions = Exception.class, expectedExceptionsMessageRegExp = "Expressions must appear in select list for SELECT DISTINCT, ORDER BY.*")
    public void testDistinctWithOrderByNotInSelect()
            throws Exception
    {
        assertQuery("SELECT DISTINCT custkey FROM orders ORDER BY orderkey LIMIT 10");
    }

    @Test
    public void testOrderByLimit()
            throws Exception
    {
        assertQueryOrdered("SELECT custkey, orderstatus FROM ORDERS ORDER BY orderkey DESC LIMIT 10");
    }

    @Test
    public void testOrderByExpressionWithLimit()
            throws Exception
    {
        assertQueryOrdered("SELECT custkey, orderstatus FROM ORDERS ORDER BY orderkey + 1 DESC LIMIT 10");
    }

    @Test
    public void testGroupByOrderByLimit()
            throws Exception
    {
        assertQueryOrdered("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey ORDER BY SUM(totalprice) DESC LIMIT 10");
    }

    @Test
    public void testLimitZero()
            throws Exception
    {
        assertQuery("SELECT custkey, totalprice FROM orders LIMIT 0");
    }

    @Test
    public void testRepeatedAggregations()
            throws Exception
    {
        assertQuery("SELECT SUM(orderkey), SUM(orderkey) FROM ORDERS");
    }

    @Test
    public void testRepeatedOutputs()
            throws Exception
    {
        assertQuery("SELECT orderkey a, orderkey b FROM ORDERS WHERE orderstatus = 'F'");
    }

    @Test
    public void testLimit()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT orderkey FROM ORDERS LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM ORDERS", actual.getTupleInfo());


        assertEquals(actual.getMaterializedTuples().size(), 10);
        assertTrue(all.getMaterializedTuples().containsAll(actual.getMaterializedTuples()));
    }

    @Test
    public void testAggregationWithLimit()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey LIMIT 10");
        MaterializedResult all = computeExpected("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey", actual.getTupleInfo());

        assertEquals(actual.getMaterializedTuples().size(), 10);
        assertTrue(all.getMaterializedTuples().containsAll(actual.getMaterializedTuples()));
    }

    @Test
    public void testLimitInInlineView()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT orderkey FROM (SELECT orderkey FROM ORDERS LIMIT 100) T LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM ORDERS", actual.getTupleInfo());

        assertEquals(actual.getMaterializedTuples().size(), 10);
        assertTrue(all.getMaterializedTuples().containsAll(actual.getMaterializedTuples()));
    }

    @Test
    public void testCountAll()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM ORDERS");
    }

    @Test
    public void testCountColumn()
            throws Exception
    {
        assertQuery("SELECT COUNT(orderkey) FROM ORDERS");
        assertQuery("SELECT COUNT(orderstatus) FROM ORDERS");
        assertQuery("SELECT COUNT(orderdate) FROM ORDERS");
        assertQuery("SELECT COUNT(1) FROM ORDERS");

        assertQuery("SELECT COUNT(NULLIF(orderstatus, 'F')) FROM ORDERS");
        assertQuery("SELECT COUNT(CAST(NULL AS BIGINT)) FROM ORDERS"); // todo: make COUNT(null) work
    }

    @Test
    public void testWildcard()
            throws Exception
    {
        assertQuery("SELECT * FROM ORDERS");
    }

    @Test
    public void testQualifiedWildcardFromAlias()
            throws Exception
    {
        assertQuery("SELECT T.* FROM ORDERS T");
    }

    @Test
    public void testQualifiedWildcardFromInlineView()
            throws Exception
    {
        assertQuery("SELECT T.* FROM (SELECT orderkey + custkey FROM ORDERS) T");
    }

    @Test
    public void testQualifiedWildcard()
            throws Exception
    {
        assertQuery("SELECT ORDERS.* FROM ORDERS");
    }

    @Test
    public void testAverageAll()
            throws Exception
    {
        assertQuery("SELECT AVG(totalprice) FROM ORDERS");
    }

    @Test
    public void testVariance()
            throws Exception
    {
        // int64
        assertQuery("SELECT VAR_SAMP(custkey) FROM ORDERS");
        assertQuery("SELECT VAR_SAMP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT VAR_SAMP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT VAR_SAMP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT VAR_SAMP(totalprice) FROM ORDERS");
        assertQuery("SELECT VAR_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT VAR_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT VAR_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 0) T");
    }

    @Test
    public void testVariancePop()
            throws Exception
    {
        // int64
        assertQuery("SELECT VAR_POP(custkey) FROM ORDERS");
        assertQuery("SELECT VAR_POP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT VAR_POP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT VAR_POP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT VAR_POP(totalprice) FROM ORDERS");
        assertQuery("SELECT VAR_POP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT VAR_POP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT VAR_POP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 0) T");
    }

    @Test
    public void testStdDev()
            throws Exception
    {
        // int64
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM ORDERS");
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM ORDERS");
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 0) T");
    }

    @Test
    public void testStdDevPop()
            throws Exception
    {
        // int64
        assertQuery("SELECT STDDEV_POP(custkey) FROM ORDERS");
        assertQuery("SELECT STDDEV_POP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT STDDEV_POP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT STDDEV_POP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT STDDEV_POP(totalprice) FROM ORDERS");
        assertQuery("SELECT STDDEV_POP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT STDDEV_POP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT STDDEV_POP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 0) T");
    }

    @Test
    public void testCountAllWithPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM ORDERS WHERE orderstatus = 'F'");
    }

    @Test
    public void testGroupByNoAggregations()
            throws Exception
    {
        assertQuery("SELECT custkey FROM ORDERS GROUP BY custkey");
    }

    @Test
    public void testGroupByCount()
            throws Exception
    {
        assertQuery(
                "SELECT orderstatus, COUNT(*) FROM ORDERS GROUP BY orderstatus",
                "SELECT orderstatus, CAST(COUNT(*) AS INTEGER) FROM orders GROUP BY orderstatus"
        );
    }

    @Test
    public void testGroupByMultipleFields()
            throws Exception
    {
        assertQuery("SELECT custkey, orderstatus, COUNT(*) FROM ORDERS GROUP BY custkey, orderstatus");
    }

    @Test
    public void testGroupByWithAlias()
            throws Exception
    {
        assertQuery(
                "SELECT orderdate x, COUNT(*) FROM orders GROUP BY orderdate",
                "SELECT orderdate x, CAST(COUNT(*) AS INTEGER) FROM orders GROUP BY orderdate"
        );
    }

    @Test
    public void testGroupBySum()
            throws Exception
    {
        assertQuery("SELECT orderstatus, SUM(totalprice) FROM ORDERS GROUP BY orderstatus");
    }

    @Test
    public void testCountAllWithComparison()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE tax < discount");
    }

    @Test
    public void testSelectWithComparison()
            throws Exception
    {
        assertQuery("SELECT orderkey FROM lineitem WHERE tax < discount");
    }

    @Test
    public void testCountWithNotPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE NOT tax < discount");
    }

    @Test
    public void testCountWithNullPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE NULL");
    }

    @Test
    public void testCountWithIsNullPredicate()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') IS NULL",
                "SELECT COUNT(*) FROM orders WHERE orderstatus = 'F' "
        );
    }

    @Test
    public void testCountWithIsNotNullPredicate()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') IS NOT NULL",
                "SELECT COUNT(*) FROM orders WHERE orderstatus <> 'F' "
        );
    }

    @Test
    public void testCountWithNullIfPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') = orderstatus ");
    }

    @Test
    public void testCountWithCoalescePredicate()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM orders WHERE COALESCE(NULLIF(orderstatus, 'F'), 'bar') = 'bar'",
                "SELECT COUNT(*) FROM orders WHERE orderstatus = 'F'"
        );
    }

    @Test
    public void testCountWithAndPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE tax < discount AND tax > 0.01 AND discount < 0.05");
    }

    @Test
    public void testCountWithOrPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE tax < 0.01 OR discount > 0.05");
    }

    @Test
    public void testCountWithInlineView()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT orderkey FROM lineitem) x");
    }

    @Test
    public void testNestedCount()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT orderkey, COUNT(*) FROM lineitem GROUP BY orderkey) x");
    }

    @Test
    public void testAggregationWithProjection()
            throws Exception
    {
        assertQuery("SELECT sum(totalprice * 2) - sum(totalprice) FROM orders");
    }

    @Test
    public void testAggregationWithProjection2()
            throws Exception
    {
        assertQuery("SELECT sum(totalprice * 2) + sum(totalprice * 2) FROM orders");
    }

    @Test
    public void testInlineView()
            throws Exception
    {
        assertQuery("SELECT orderkey, custkey FROM (SELECT orderkey, custkey FROM ORDERS) U");
    }

    @Test
    public void testAliasedInInlineView()
            throws Exception
    {
        assertQuery("SELECT x, y FROM (SELECT orderkey x, custkey y FROM ORDERS) U");
    }

    @Test
    public void testGroupByWithoutAggregation()
            throws Exception
    {
        assertQuery("SELECT orderstatus FROM orders GROUP BY orderstatus");
    }

    @Test
    public void testHistogram()
            throws Exception
    {
        assertQuery("SELECT lines, COUNT(*) FROM (SELECT orderkey, COUNT(*) lines FROM lineitem GROUP BY orderkey) U GROUP BY lines");
    }

    @Test
    public void testSimpleJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testJoinWithConstantExpression()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND 123 = 123");
    }

    @Test(expectedExceptions = Exception.class, expectedExceptionsMessageRegExp = ".*not supported.*")
    public void testJoinOnConstantExpression()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON 123 = 123");
    }

    @Test
    public void testJoinUsing()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM lineitem join orders using (orderkey)",
                "SELECT COUNT(*) FROM lineitem join orders on lineitem.orderkey = orders.orderkey"
        );
    }

    @Test
    public void testJoinWithReversedComparison()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON orders.orderkey = lineitem.orderkey");
    }

    @Test
    public void testJoinWithComplexExpressions()
            throws Exception
    {
        assertQuery("SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey = CAST(orders.orderkey AS BIGINT)");
    }

    @Test
    public void testJoinWithComplexExpressions2()
            throws Exception
    {
        assertQuery("SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey = CASE WHEN orders.custkey = 1 and orders.orderstatus = 'F' THEN orders.orderkey ELSE NULL END");
    }

    @Test
    public void testJoinWithComplexExpressions3()
            throws Exception
    {
        assertQuery(
                "SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey + 1 = orders.orderkey + 1",
                "SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey " // H2 takes a million years because it can't join efficiently on a non-indexed field/expression
        );
    }

    @Test
    public void testSelfJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM orders a JOIN orders b on a.orderkey = b.orderkey");
    }

    @Test
    public void testWilcardFromJoin()
            throws Exception
    {
        assertQuery(
                "SELECT * FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b using (orderkey)",
                "SELECT * FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b on a.orderkey = b.orderkey"
        );
    }

    @Test
    public void testQualifiedWilcardFromJoin()
            throws Exception
    {
        assertQuery(
                "SELECT a.*, b.* FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b using (orderkey)",
                "SELECT a.*, b.* FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b on a.orderkey = b.orderkey"
        );
    }

    @Test
    public void testJoinAggregations()
            throws Exception
    {
        assertQuery(
                "SELECT x + y FROM (" +
                        "   SELECT orderdate, COUNT(*) x FROM orders GROUP BY orderdate) a JOIN (" +
                        "   SELECT orderdate, COUNT(*) y FROM orders GROUP BY orderdate) b ON a.orderdate = b.orderdate");
    }

    @Test
    public void testJoinOnMultipleFields()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.shipdate = orders.orderdate");
    }

    @Test
    public void testJoinUsingMultipleFields()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM lineitem JOIN (SELECT orderkey, orderdate shipdate FROM ORDERS) T USING (orderkey, shipdate)",
                "SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.shipdate = orders.orderdate"
        );
    }

    @Test
    public void testJoinWithNonJoinExpression()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey = 1");
    }

    @Test
    public void testOrderBy()
            throws Exception
    {
        assertQueryOrdered("SELECT orderstatus FROM orders ORDER BY orderstatus");
    }

    @Test
    public void testOrderBy2()
            throws Exception
    {
        assertQueryOrdered("SELECT orderstatus FROM orders ORDER BY orderkey DESC");
    }

    @Test
    public void testOrderByMultipleFields()
            throws Exception
    {
        assertQuery("SELECT orderkey, orderstatus FROM orders ORDER BY custkey DESC, orderstatus");
    }

    @Test(enabled = false)
    public void testOrderByAlias()
            throws Exception
    {
        assertQueryOrdered("SELECT orderstatus x FROM orders ORDER BY x ASC");
    }

    @Test(enabled = false)
    public void testOrderByAliasWithSameNameAsUnselectedColumn()
            throws Exception
    {
        assertQueryOrdered("SELECT orderstatus orderdate FROM orders ORDER BY orderdate ASC");
    }


    @SuppressWarnings("PointlessArithmeticExpression")
    @Test
    public void testWindowFunctions()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, orderstatus\n" +
                ", row_number() OVER (ORDER BY orderkey * 2) *\n" +
                "  row_number() OVER (ORDER BY orderkey DESC) + 100\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) x\n" +
                "ORDER BY orderkey");

        MaterializedResult expected = resultBuilder(FIXED_INT_64, VARIABLE_BINARY, FIXED_INT_64)
                .row(1, "O", (1 * 10) + 100)
                .row(2, "O", (2 * 9) + 100)
                .row(3, "F", (3 * 8) + 100)
                .row(4, "O", (4 * 7) + 100)
                .row(5, "F", (5 * 6) + 100)
                .row(6, "F", (6 * 5) + 100)
                .row(7, "O", (7 * 4) + 100)
                .row(32, "O", (8 * 3) + 100)
                .row(33, "F", (9 * 2) + 100)
                .row(34, "O", (10 * 1) + 100)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testScalarFunction()
            throws Exception
    {
        assertQuery("SELECT SUBSTR('Quadratically', 5, 6) FROM orders LIMIT 1");
    }

    @Test
    public void testCast()
            throws Exception
    {
        assertQuery("SELECT CAST(totalprice AS BIGINT) FROM orders");
        assertQuery("SELECT CAST(orderkey AS DOUBLE) FROM orders");
        assertQuery("SELECT CAST(orderkey AS VARCHAR) FROM orders");
    }

    @Test
    public void testConcatOperator()
            throws Exception
    {
        assertQuery("SELECT '12' || '34' FROM orders LIMIT 1");
    }

    @Test
    public void testQuotedIdentifiers()
            throws Exception
    {
        assertQuery("SELECT \"TOTALPRICE\" \"my price\" FROM \"ORDERS\"");
    }

    @Test(expectedExceptions = Exception.class, expectedExceptionsMessageRegExp = ".*orderkey_1.*")
    public void testInvalidColumn()
            throws Exception
    {
        computeActual("select * from lineitem l join (select orderkey_1, custkey from orders) o on l.orderkey = o.orderkey_1");
    }

    @BeforeClass(alwaysRun = true)
    public void setupDatabase()
            throws Exception
    {
        handle = DBI.open("jdbc:h2:mem:test" + System.nanoTime());

        RecordSet ordersRecords = readTpchRecords("orders");
        handle.execute("CREATE TABLE orders (\n" +
                "  orderkey BIGINT PRIMARY KEY,\n" +
                "  custkey BIGINT NOT NULL,\n" +
                "  orderstatus CHAR(1) NOT NULL,\n" +
                "  totalprice DOUBLE NOT NULL,\n" +
                "  orderdate CHAR(10) NOT NULL,\n" +
                "  orderpriority CHAR(15) NOT NULL,\n" +
                "  clerk CHAR(15) NOT NULL,\n" +
                "  shippriority BIGINT NOT NULL,\n" +
                "  comment VARCHAR(79) NOT NULL\n" +
                ")");
        insertRows(TpchSchema.createOrders(), handle, ordersRecords);

        RecordSet lineItemRecords = readTpchRecords("lineitem");
        handle.execute("CREATE TABLE lineitem (\n" +
                "  orderkey BIGINT,\n" +
                "  partkey BIGINT NOT NULL,\n" +
                "  suppkey BIGINT NOT NULL,\n" +
                "  linenumber BIGINT,\n" +
                "  quantity BIGINT NOT NULL,\n" +
                "  extendedprice DOUBLE NOT NULL,\n" +
                "  discount DOUBLE NOT NULL,\n" +
                "  tax DOUBLE NOT NULL,\n" +
                "  returnflag CHAR(1) NOT NULL,\n" +
                "  linestatus CHAR(1) NOT NULL,\n" +
                "  shipdate CHAR(10) NOT NULL,\n" +
                "  commitdate CHAR(10) NOT NULL,\n" +
                "  receiptdate CHAR(10) NOT NULL,\n" +
                "  shipinstruct VARCHAR(25) NOT NULL,\n" +
                "  shipmode VARCHAR(10) NOT NULL,\n" +
                "  comment VARCHAR(44) NOT NULL,\n" +
                "  PRIMARY KEY (orderkey, linenumber)" +
                ")");
        insertRows(TpchSchema.createLineItem(), handle, lineItemRecords);

        metadata = TpchSchema.createMetadata();

        TestingTpchBlocksProvider tpchBlocksProvider = new TestingTpchBlocksProvider(
                ImmutableMap.of(
                        "orders", ordersRecords,
                        "lineitem", lineItemRecords
                )
        );

        dataProvider = new TpchDataStreamProvider(tpchBlocksProvider);

        setUpQueryFramework(TpchSchema.CATALOG_NAME, TpchSchema.SCHEMA_NAME, dataProvider, metadata);
    }

    @AfterClass(alwaysRun = true)
    public void cleanupDatabase()
            throws Exception
    {
        tearDownQueryFramework();
        handle.close();
    }

    protected void setUpQueryFramework(String catalog, String schema, DataStreamProvider dataStreamProvider, Metadata metadata) throws Exception {}
    protected void tearDownQueryFramework() throws Exception {}
    protected abstract MaterializedResult computeActual(@Language("SQL") String sql);

    private void assertQuery(@Language("SQL") String sql)
            throws Exception
    {
        assertQuery(sql, sql, false);
    }

    private void assertQueryOrdered(@Language("SQL") String sql)
            throws Exception
    {
        assertQuery(sql, sql, true);
    }

    private void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        assertQuery(actual, expected, false);
    }

    private void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected, boolean ensureOrdering)
            throws Exception
    {
        MaterializedResult actualResults = computeActual(actual);
        MaterializedResult expectedResults = computeExpected(expected, actualResults.getTupleInfo());

        if (ensureOrdering) {
            assertEquals(actualResults.getMaterializedTuples(), expectedResults.getMaterializedTuples());
        }
        else {
            assertEqualsIgnoreOrder(actualResults.getMaterializedTuples(), expectedResults.getMaterializedTuples());
        }
    }

    public static void assertEqualsIgnoreOrder(Iterable<?> actual, Iterable<?> expected)
    {
        assertNotNull(actual, "actual is null");
        assertNotNull(expected, "expected is null");

        ImmutableMultiset<?> actualSet = ImmutableMultiset.copyOf(actual);
        ImmutableMultiset<?> expectedSet = ImmutableMultiset.copyOf(expected);
        if (!actualSet.equals(expectedSet)) {
            Assert.fail("not equal");
        }
    }

    private MaterializedResult computeExpected(@Language("SQL") final String sql, TupleInfo resultTupleInfo)
    {
        return new MaterializedResult(
                handle.createQuery(sql)
                        .map(tupleMapper(resultTupleInfo))
                        .list(),
                resultTupleInfo
        );
    }

    private static ResultSetMapper<Tuple> tupleMapper(final TupleInfo tupleInfo)
    {
        return new ResultSetMapper<Tuple>()
        {
            @Override
            public Tuple map(int index, ResultSet resultSet, StatementContext ctx)
                    throws SQLException
            {
                List<TupleInfo.Type> types = tupleInfo.getTypes();
                int count = resultSet.getMetaData().getColumnCount();
                checkArgument(types.size() == count, "tuple info does not match result");
                TupleInfo.Builder builder = tupleInfo.builder();
                for (int i = 1; i <= count; i++) {
                    TupleInfo.Type type = types.get(i - 1);
                    switch (type) {
                        case FIXED_INT_64:
                            long longValue = resultSet.getLong(i);
                            if (resultSet.wasNull()) {
                                builder.appendNull();
                            }
                            else {
                                builder.append(longValue);
                            }
                            break;
                        case DOUBLE:
                            double doubleValue = resultSet.getDouble(i);
                            if (resultSet.wasNull()) {
                                builder.appendNull();
                            }
                            else {
                                builder.append(doubleValue);
                            }
                            break;
                        case VARIABLE_BINARY:
                            String value = resultSet.getString(i);
                            if (resultSet.wasNull()) {
                                builder.appendNull();
                            }
                            builder.append(Slices.wrappedBuffer(value.getBytes(UTF_8)));
                            break;
                        default:
                            throw new AssertionError("unhandled type: " + type);
                    }
                }
                return builder.build();
            }
        };
    }

    private static void insertRows(TableMetadata tableMetadata, Handle handle, RecordSet data)
    {
        String vars = Joiner.on(',').join(nCopies(tableMetadata.getColumns().size(), "?"));
        String sql = format("INSERT INTO %s VALUES (%s)", tableMetadata.getTableName(), vars);

        RecordCursor cursor = data.cursor(new OperatorStats());
        while (true) {
            // insert 1000 rows at a time
            PreparedBatch batch = handle.prepareBatch(sql);
            for (int row = 0; row < 1000; row++) {
                if (!cursor.advanceNextPosition()) {
                    batch.execute();
                    return;
                }
                PreparedBatchPart part = batch.add();
                for (int column = 0; column < tableMetadata.getColumns().size(); column++) {
                    ColumnMetadata columnMetadata = tableMetadata.getColumns().get(column);
                    switch (columnMetadata.getType()) {
                        case FIXED_INT_64:
                            part.bind(column, cursor.getLong(column));
                            break;
                        case DOUBLE:
                            part.bind(column, cursor.getDouble(column));
                            break;
                        case VARIABLE_BINARY:
                            part.bind(column, new String(cursor.getString(column), UTF_8));
                            break;
                    }
                }
            }
            batch.execute();
        }
    }
}
