package com.facebook.presto;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.ingest.DelimitedRecordSet;
import com.facebook.presto.ingest.RecordCursor;
import com.facebook.presto.ingest.RecordSet;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.server.HackPlanFragmentSourceProvider;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.sql.analyzer.AnalysisResult;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DistributedLogicalPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.TableScanPlanFragmentSource;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.tpch.TpchSplit;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import org.intellij.lang.annotations.Language;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.PreparedBatchPart;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestQueries
{
    private static final JsonCodec<TaskInfo> TASK_INFO_CODEC = JsonCodec.jsonCodec(TaskInfo.class);

    private Handle handle;
    private RecordSet ordersRecords;
    private RecordSet lineItemRecords;
    private Metadata metadata;
    private TpchDataStreamProvider dataProvider;


    @Test(enabled = false) // TODO: fix this by inserting projections that re-organize channels/fields (see LocalExecutionPlanner.visitJoin())
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

    // TODO: make this work
    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = "DISTINCT in aggregation parameters not yet supported")
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

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = "Expressions must appear in select list for SELECT DISTINCT, ORDER BY.*")
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
        List<Tuple> all = computeExpected("SELECT orderkey FROM ORDERS", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT orderkey FROM ORDERS LIMIT 10");

        assertEquals(actual.size(), 10);
        assertTrue(all.containsAll(actual));
    }

    @Test
    public void testAggregationWithLimit()
            throws Exception
    {
        List<Tuple> all = computeExpected("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey", FIXED_INT_64, DOUBLE);
        List<Tuple> actual = computeActual("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey LIMIT 10");

        assertEquals(actual.size(), 10);
        assertTrue(all.containsAll(actual));
    }

    @Test
    public void testLimitInInlineView()
            throws Exception
    {
        List<Tuple> all = computeExpected("SELECT orderkey FROM ORDERS", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT orderkey FROM (SELECT orderkey FROM ORDERS LIMIT 100) T LIMIT 10");

        assertEquals(actual.size(), 10);
        assertTrue(all.containsAll(actual));
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
        assertQuery("SELECT VAR_SAMP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 2) T");
        assertQuery("SELECT VAR_SAMP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 1) T");
        assertQuery("SELECT VAR_SAMP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT VAR_SAMP(totalprice) FROM ORDERS");
        assertQuery("SELECT VAR_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 2) T");
        assertQuery("SELECT VAR_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 1) T");
        assertQuery("SELECT VAR_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 0) T");
    }

    @Test
    public void testVariancePop()
            throws Exception
    {
        // int64
        assertQuery("SELECT VAR_POP(custkey) FROM ORDERS");
        assertQuery("SELECT VAR_POP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 2) T");
        assertQuery("SELECT VAR_POP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 1) T");
        assertQuery("SELECT VAR_POP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT VAR_POP(totalprice) FROM ORDERS");
        assertQuery("SELECT VAR_POP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 2) T");
        assertQuery("SELECT VAR_POP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 1) T");
        assertQuery("SELECT VAR_POP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 0) T");
    }

    @Test
    public void testStdDev()
            throws Exception
    {
        // int64
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM ORDERS");
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 2) T");
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 1) T");
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM ORDERS");
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 2) T");
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 1) T");
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 0) T");
    }

    @Test
    public void testStdDevPop()
            throws Exception
    {
        // int64
        assertQuery("SELECT STDDEV_POP(custkey) FROM ORDERS");
        assertQuery("SELECT STDDEV_POP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 2) T");
        assertQuery("SELECT STDDEV_POP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 1) T");
        assertQuery("SELECT STDDEV_POP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT STDDEV_POP(totalprice) FROM ORDERS");
        assertQuery("SELECT STDDEV_POP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 2) T");
        assertQuery("SELECT STDDEV_POP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 1) T");
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

    @Test(enabled = false) // TODO: fix this case
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

    @Test(enabled = false) // TODO: doesn't work because the underlying table appears twice in the same fragment
    public void testJoinAggregations()
            throws Exception
    {
        assertQuery(
                "SELECT x + y FROM (" +
                        "   SELECT orderdate, COUNT(*) x FROM orders GROUP BY orderdate) a JOIN (" +
                        "   SELECT orderdate, COUNT(*) y FROM orders GROUP BY orderdate) b USING (orderdate)");
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
    public void testQuotedIdentifiers()
            throws Exception
    {
        assertQuery("SELECT \"TOTALPRICE\" \"my price\" FROM \"ORDERS\"");
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = ".*orderkey_1.*")
    public void testInvalidColumn()
            throws Exception
    {
        computeActual("select * from lineitem l join (select orderkey_1, custkey from orders) o on l.orderkey = o.orderkey_1");
    }

    @BeforeSuite
    public void setupDatabase()
            throws IOException
    {
        handle = DBI.open("jdbc:h2:mem:test" + System.nanoTime());

        ordersRecords = readRecords("tpch/orders.dat.gz");
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

        lineItemRecords = readRecords("tpch/lineitem.dat.gz");
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

        TestTpchBlocksProvider testTpchBlocksProvider = new TestTpchBlocksProvider(
                ImmutableMap.of(
                        "orders", ordersRecords,
                        "lineitem", lineItemRecords
                )
        );

        dataProvider = new TpchDataStreamProvider(testTpchBlocksProvider);
    }

    @AfterSuite
    public void cleanupDatabase()
    {
        handle.close();
    }

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
        Operator operator = plan(actual);

        List<Tuple> actualResults = getTuples(operator);
        List<Tuple> expectedResults = computeExpected(expected, Iterables.getOnlyElement(operator.getTupleInfos()));

        if (ensureOrdering) {
            assertEquals(actualResults, expectedResults);
        }
        else {
            assertEqualsIgnoreOrder(actualResults, expectedResults);
        }
    }

    private List<Tuple> computeExpected(@Language("SQL") final String sql, TupleInfo tupleInfo)
    {
        return handle.createQuery(sql)
                .map(tupleMapper(tupleInfo))
                .list();
    }

    private List<Tuple> computeExpected(@Language("SQL") final String sql, TupleInfo.Type... types)
    {
        return computeExpected(sql, new TupleInfo(types));
    }

    private List<Tuple> computeActual(@Language("SQL") String sql)
            throws Exception
    {
        return getTuples(plan(sql));
    }

    private List<Tuple> getTuples(Operator root)
    {
        ImmutableList.Builder<Tuple> output = ImmutableList.builder();
        PageIterator iterator = root.iterator(new OperatorStats());
        while (iterator.hasNext()) {
            Page page = iterator.next();
            Preconditions.checkState(page.getChannelCount() == 1, "Expected result to produce 1 channel");

            BlockCursor cursor = Iterables.getOnlyElement(Arrays.asList(page.getBlocks())).cursor();
            while (cursor.advanceNextPosition()) {
                output.add(cursor.getTuple());
            }
        }

        return output.build();
    }

    private Operator plan(String sql)
    {
        Statement statement = SqlParser.createStatement(sql);

        Session session = new Session(null, TpchSchema.CATALOG_NAME, TpchSchema.SCHEMA_NAME);

        Analyzer analyzer = new Analyzer(session, metadata);

        AnalysisResult analysis = analyzer.analyze(statement);

        PlanNode plan = new LogicalPlanner(session, metadata).plan(analysis);
        new PlanPrinter().print(plan, analysis.getTypes());

        SubPlan subplan = new DistributedLogicalPlanner(metadata).createSubplans(plan, analysis.getSymbolAllocator(), true);
        assertTrue(subplan.getChildren().isEmpty(), "Expected subplan to have no children");

        ImmutableMap.Builder<TableHandle, TableScanPlanFragmentSource> builder = ImmutableMap.builder();
        for (PlanNode source : subplan.getFragment().getSources()) {
            TableScanNode tableScan = (TableScanNode) source;
            TpchTableHandle handle = (TpchTableHandle) tableScan.getTable();

            builder.put(handle, new TableScanPlanFragmentSource(new TpchSplit(handle)));
        }

        DataSize maxOperatorMemoryUsage = new DataSize(50, MEGABYTE);
        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(session, metadata,
                new HackPlanFragmentSourceProvider(dataProvider, null, TASK_INFO_CODEC),
                analysis.getTypes(),
                null,
                builder.build(),
                ImmutableMap.<String, ExchangePlanFragmentSource>of(),
                new OperatorStats(),
                new SourceHashProviderFactory(maxOperatorMemoryUsage),
                maxOperatorMemoryUsage
        );

        Operator operator = executionPlanner.plan(plan);

        return new FilterAndProjectOperator(operator,
                FilterFunctions.TRUE_FUNCTION,
                new Concat(operator.getTupleInfos()));
    }

    @SuppressWarnings("UnusedDeclaration")
    private static Iterable<List<Object>> tupleValues(Iterable<Tuple> tuples)
    {
        return Iterables.transform(tuples, new Function<Tuple, List<Object>>()
        {
            @Override
            public List<Object> apply(Tuple input)
            {
                return input.toValues();
            }
        });
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

    private static ResultSetMapper<Tuple> tupleMapper(final TupleInfo tupleInfo)
    {
        return new ResultSetMapper<Tuple>()
        {
            @Override
            public Tuple map(int index, ResultSet rs, StatementContext ctx)
                    throws SQLException
            {
                List<TupleInfo.Type> types = tupleInfo.getTypes();
                int count = rs.getMetaData().getColumnCount();
                checkArgument(types.size() == count, "tuple info does not match result");
                TupleInfo.Builder builder = tupleInfo.builder();
                for (int i = 1; i <= count; i++) {
                    TupleInfo.Type type = types.get(i - 1);
                    switch (type) {
                        case FIXED_INT_64:
                            long longValue = rs.getLong(i);
                            if (rs.wasNull()) {
                                builder.appendNull();
                            }
                            else {
                                builder.append(longValue);
                            }
                            break;
                        case DOUBLE:
                            double doubleValue = rs.getDouble(i);
                            if (rs.wasNull()) {
                                builder.appendNull();
                            }
                            else {
                                builder.append(doubleValue);
                            }
                            break;
                        case VARIABLE_BINARY:
                            String value = rs.getString(i);
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

    @SuppressWarnings("UnusedDeclaration")
    private static ResultSetMapper<List<Object>> listMapper()
    {
        return new ResultSetMapper<List<Object>>()
        {
            @Override
            public List<Object> map(int index, ResultSet rs, StatementContext ctx)
                    throws SQLException
            {
                int count = rs.getMetaData().getColumnCount();
                List<Object> list = new ArrayList<>(count);
                for (int i = 1; i <= count; i++) {
                    list.add(rs.getObject(i));
                }
                return list;
            }
        };
    }

    private static RecordSet readRecords(String name)
            throws IOException
    {
        // tpch does not contain nulls, but does have a trailing pipe character,
        // so omitting empty strings will prevent an extra column at the end being added
        Splitter splitter = Splitter.on('|').omitEmptyStrings();
        return new DelimitedRecordSet(readResource(name), splitter);
    }

    private static InputSupplier<InputStreamReader> readResource(final String name)
    {
        return new InputSupplier<InputStreamReader>()
        {
            @Override
            public InputStreamReader getInput()
                    throws IOException
            {
                URL url = Resources.getResource(name);
                GZIPInputStream gzip = new GZIPInputStream(url.openStream());
                return new InputStreamReader(gzip, UTF_8);
            }
        };
    }

    private static class TestTpchBlocksProvider
            implements TpchBlocksProvider
    {
        private final Map<String, RecordSet> data;

        private TestTpchBlocksProvider(Map<String, RecordSet> data)
        {
            Preconditions.checkNotNull(data, "data is null");
            this.data = ImmutableMap.copyOf(data);
        }

        @Override
        public BlockIterable getBlocks(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, BlocksFileEncoding encoding)
        {
            String tableName = tableHandle.getTableName();
            int fieldIndex = columnHandle.getFieldIndex();
            Type fieldType = columnHandle.getType();
            return new TpchBlockIterable(fieldType, tableName, fieldIndex);
        }

        @Override
        public DataSize getColumnDataSize(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, BlocksFileEncoding encoding)
        {
            throw new UnsupportedOperationException();
        }

        private class TpchBlockIterable
                implements BlockIterable
        {
            private final Type fieldType;
            private final String tableName;
            private final int fieldIndex;

            public TpchBlockIterable(Type fieldType, String tableName, int fieldIndex)
            {
                this.fieldType = fieldType;
                this.tableName = tableName;
                this.fieldIndex = fieldIndex;
            }

            @Override
            public TupleInfo getTupleInfo()
            {
                return new TupleInfo(fieldType);
            }

            @Override
            public Optional<DataSize> getDataSize()
            {
                return Optional.absent();
            }

            @Override
            public Optional<Integer> getPositionCount()
            {
                return Optional.absent();
            }

            @Override
            public Iterator<Block> iterator()
            {
                return new AbstractIterator<Block>()
                {
                    private final RecordCursor cursor = data.get(tableName).cursor(new OperatorStats());

                    @Override
                    protected Block computeNext()
                    {
                        BlockBuilder builder = new BlockBuilder(new TupleInfo(fieldType));

                        while (!builder.isFull() && cursor.advanceNextPosition()) {
                            switch (fieldType) {
                                case FIXED_INT_64:
                                    builder.append(cursor.getLong(fieldIndex));
                                    break;
                                case DOUBLE:
                                    builder.append(cursor.getDouble(fieldIndex));
                                    break;
                                case VARIABLE_BINARY:
                                    builder.append(cursor.getString(fieldIndex));
                                    break;
                            }
                        }

                        if (builder.isEmpty()) {
                            return endOfData();
                        }

                        return builder.build();
                    }
                };
            }
        }
    }

    private static class Concat
            implements ProjectionFunction
    {
        private final TupleInfo tupleInfo;

        private Concat(List<TupleInfo> infos)
        {
            List<TupleInfo.Type> types = new ArrayList<>();
            for (TupleInfo info : infos) {
                types.addAll(info.getTypes());
            }

            this.tupleInfo = new TupleInfo(types);
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }

        @Override
        public void project(TupleReadable[] cursors, BlockBuilder output)
        {
            for (TupleReadable cursor : cursors) {
                output.append(cursor.getTuple());
            }
        }
    }
}
