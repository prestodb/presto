package com.facebook.presto;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.ingest.Record;
import com.facebook.presto.ingest.RecordIterable;
import com.facebook.presto.ingest.RecordIterables;
import com.facebook.presto.ingest.RecordIterator;
import com.facebook.presto.ingest.RecordProjection;
import com.facebook.presto.ingest.RecordProjections;
import com.facebook.presto.ingest.StringRecord;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.server.ExchangePlanFragmentSource;
import com.facebook.presto.server.HackPlanFragmentSourceProvider;
import com.facebook.presto.server.QueryTaskInfo;
import com.facebook.presto.server.TableScanPlanFragmentSource;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.sql.compiler.AnalysisResult;
import com.facebook.presto.sql.compiler.Analyzer;
import com.facebook.presto.sql.compiler.SessionMetadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExecutionPlanner;
import com.facebook.presto.sql.planner.FragmentPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.sql.planner.PlanNode;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.Planner;
import com.facebook.presto.sql.planner.TableScan;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.tpch.TpchSplit;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import org.antlr.runtime.RecognitionException;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.isEmpty;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestQueries
{
    private static final JsonCodec<QueryTaskInfo> QUERY_TASK_INFO_CODEC = JsonCodec.jsonCodec(QueryTaskInfo.class);

    private Handle handle;
    private RecordIterable ordersRecords;
    private RecordIterable lineItemRecords;
    private Metadata metadata;
    private TpchDataStreamProvider dataProvider;

    @BeforeSuite
    public void setupDatabase()
            throws IOException
    {
        handle = DBI.open("jdbc:h2:mem:test" + System.nanoTime());

        ordersRecords = readRecords("tpch/orders.dat.gz", 15000);
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
        insertRows("orders", handle, ordersRecords);

        lineItemRecords = readRecords("tpch/lineitem.dat.gz", 60175);
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
        insertRows("lineitem", handle, lineItemRecords);

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

    @Test
    public void testOrderByLimit()
    {
        List<Tuple> expected = computeExpected("SELECT custkey, orderstatus FROM ORDERS ORDER BY orderkey desc LIMIT 10", FIXED_INT_64, VARIABLE_BINARY);
        List<Tuple> actual = computeActual("SELECT custkey, orderstatus FROM ORDERS ORDER BY orderkey desc LIMIT 10");

        assertEquals(actual, expected);
    }

    @Test
    public void testOrderByExpressionWithLimit()
            throws Exception
    {
        List<Tuple> expected = computeExpected("SELECT custkey, orderstatus FROM ORDERS ORDER BY orderkey + 1 desc LIMIT 10", FIXED_INT_64, VARIABLE_BINARY);
        List<Tuple> actual = computeActual("SELECT custkey, orderstatus FROM ORDERS ORDER BY orderkey + 1 desc LIMIT 10");

        assertEquals(actual, expected);
    }

    @Test
    public void testGroupByOrderByLimit()
            throws Exception
    {
        List<Tuple> expected = computeExpected("SELECT custkey, sum(totalprice) FROM ORDERS GROUP BY custkey ORDER BY sum(totalprice) desc LIMIT 10", FIXED_INT_64, DOUBLE);
        List<Tuple> actual = computeActual("SELECT custkey, sum(totalprice) FROM ORDERS GROUP BY custkey ORDER BY sum(totalprice) desc LIMIT 10");

        assertEquals(actual, expected);
    }

    @Test
    public void testRepeatedAggregations()
            throws Exception
    {
        List<Tuple> expected = computeExpected("SELECT sum(orderkey), sum(orderkey) FROM ORDERS", FIXED_INT_64, FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT sum(orderkey), sum(orderkey) FROM ORDERS");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testRepeatedOutputs()
            throws Exception
    {
        List<Tuple> expected = computeExpected("SELECT orderkey a, orderkey b FROM ORDERS WHERE orderstatus = 'F'", FIXED_INT_64, FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT orderkey a, orderkey b FROM ORDERS WHERE orderstatus = 'F'");

        assertEqualsIgnoreOrder(actual, expected);
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
        List<Tuple> all = computeExpected("SELECT custkey, sum(totalprice) FROM ORDERS GROUP BY custkey", FIXED_INT_64, DOUBLE);
        List<Tuple> actual = computeActual("SELECT custkey, sum(totalprice) FROM ORDERS GROUP BY custkey LIMIT 10");

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
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM ORDERS", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT COUNT(*) FROM ORDERS");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testWildcard()
    {
        List<Tuple> expected = computeExpected("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment FROM ORDERS",
                FIXED_INT_64, FIXED_INT_64, VARIABLE_BINARY, DOUBLE, VARIABLE_BINARY,  VARIABLE_BINARY,  VARIABLE_BINARY,  VARIABLE_BINARY,  VARIABLE_BINARY);
        List<Tuple> actual = computeActual("SELECT * FROM ORDERS");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testQualifiedWildcardFromAlias()
    {
        List<Tuple> expected = computeExpected("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment FROM ORDERS",
                FIXED_INT_64, FIXED_INT_64, VARIABLE_BINARY, DOUBLE, VARIABLE_BINARY,  VARIABLE_BINARY,  VARIABLE_BINARY,  VARIABLE_BINARY,  VARIABLE_BINARY);
        List<Tuple> actual = computeActual("SELECT T.* FROM ORDERS T");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testQualifiedWildcardFromInlineView()
            throws Exception
    {
        List<Tuple> expected = computeExpected("SELECT T.* FROM (SELECT orderkey + custkey FROM ORDERS) T", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT T.* FROM (SELECT orderkey + custkey FROM ORDERS) T");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testQualifiedWildcard()
    {
        List<Tuple> expected = computeExpected("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment FROM ORDERS",
                FIXED_INT_64, FIXED_INT_64, VARIABLE_BINARY, DOUBLE, VARIABLE_BINARY,  VARIABLE_BINARY,  VARIABLE_BINARY,  VARIABLE_BINARY,  VARIABLE_BINARY);
        List<Tuple> actual = computeActual("SELECT ORDERS.* FROM ORDERS");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testAverageAll()
    {
        List<Tuple> expected = computeExpected("SELECT AVG(totalprice) FROM ORDERS", DOUBLE);
        List<Tuple> actual = computeActual("SELECT AVG(totalprice) FROM ORDERS");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testCountAllWithPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM ORDERS WHERE orderstatus = 'F'", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT COUNT(*) FROM ORDERS WHERE orderstatus = 'F'");

        assertEqualsIgnoreOrder(actual, expected);
    }


    @Test(enabled = false)
    public void testGroupByNoAggregations()
    {
        List<Tuple> expected = computeExpected("SELECT custkey FROM ORDERS group by custkey", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT custkey FROM ORDERS group by custkey");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testGroupByCount()
    {
        List<Tuple> expected = computeExpected("SELECT orderstatus, CAST(COUNT(*) AS INTEGER) FROM orders GROUP BY orderstatus", VARIABLE_BINARY, FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT orderstatus, COUNT(*) FROM ORDERS GROUP BY orderstatus");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testGroupBySum()
    {
        List<Tuple> expected = computeExpected("SELECT orderstatus, SUM(totalprice) FROM orders GROUP BY orderstatus", VARIABLE_BINARY, DOUBLE);
        List<Tuple> actual = computeActual("SELECT orderstatus, SUM(totalprice) FROM ORDERS GROUP BY orderstatus");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testCountAllWithComparison()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM lineitem WHERE tax < discount", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT COUNT(*) FROM lineitem WHERE tax < discount");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testSelectWithComparison()
    {
        List<Tuple> expected = computeExpected("SELECT orderkey FROM lineitem WHERE tax < discount", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT orderkey FROM lineitem WHERE tax < discount");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testCountWithNotPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM lineitem WHERE NOT tax < discount", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT COUNT(*) FROM lineitem WHERE NOT tax < discount");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testCountWithNullPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM lineitem WHERE NULL", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT COUNT(*) FROM lineitem WHERE NULL");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testCountWithIsNullPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM orders WHERE orderstatus = 'F' ", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') IS NULL");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testCountWithIsNotNullPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM orders WHERE orderstatus <> 'F' ", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') IS NOT NULL");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testCountWithNullIfPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') = orderstatus", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') = orderstatus ");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testCountWithCoalescePredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM orders WHERE orderstatus = 'F'", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT COUNT(*) FROM orders WHERE COALESCE(NULLIF(orderstatus, 'F'), 'bar') = 'bar'");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testCountWithAndPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM lineitem WHERE tax < discount AND tax > 0.01 AND discount < 0.05", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT COUNT(*) FROM lineitem WHERE tax < discount AND tax > 0.01 AND discount < 0.05");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testCountWithOrPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM lineitem WHERE tax < 0.01 OR discount > 0.05", FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT COUNT(*) FROM lineitem WHERE tax < 0.01 OR discount > 0.05");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testAggregationWithProjection()
            throws Exception
    {
        List<Tuple> expected = computeExpected("SELECT sum(totalprice * 2) - sum(totalprice) FROM orders", DOUBLE);
        List<Tuple> actual = computeActual("SELECT sum(totalprice * 2) - sum(totalprice) FROM orders");

        assertEqualsIgnoreOrder(actual, expected);
    }


    @Test
    public void testAggregationWithProjection2()
            throws Exception
    {
        List<Tuple> expected = computeExpected("SELECT sum(totalprice * 2) + sum(totalprice * 2) FROM orders", DOUBLE);
        List<Tuple> actual = computeActual("SELECT sum(totalprice * 2) + sum(totalprice * 2) FROM orders");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testInlineView()
    {
        List<Tuple> expected = computeExpected("SELECT orderkey, custkey FROM (SELECT orderkey, custkey FROM ORDERS) U", FIXED_INT_64, FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT orderkey, custkey FROM (SELECT orderkey, custkey FROM ORDERS) U");

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testAliasedInInlineView()
            throws Exception
    {
        List<Tuple> expected = computeExpected("SELECT x, y FROM (SELECT orderkey x, custkey y FROM ORDERS) U", FIXED_INT_64, FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT x, y FROM (SELECT orderkey x, custkey y FROM ORDERS) U");

        assertEqualsIgnoreOrder(actual, expected);
    }


    @Test
    public void testHistogram()
            throws Exception
    {
        List<Tuple> expected = computeExpected("SELECT lines, COUNT(*) FROM (SELECT orderkey, COUNT(*) lines FROM lineitem GROUP BY orderkey) U GROUP BY lines", FIXED_INT_64, FIXED_INT_64);
        List<Tuple> actual = computeActual("SELECT lines, COUNT(*) FROM (SELECT orderkey, COUNT(*) lines FROM lineitem GROUP BY orderkey) U GROUP BY lines");

        assertEqualsIgnoreOrder(actual, expected);
    }


    private List<Tuple> computeExpected(@Language("SQL") final String sql, TupleInfo.Type... types)
    {
        TupleInfo tupleInfo = new TupleInfo(types);
        return handle.createQuery(sql)
                .map(tupleMapper(tupleInfo))
                .list();
    }

    private List<Tuple> computeActual(@Language("SQL") String sql)
    {
        Statement statement;
        try {
            statement = SqlParser.createStatement(sql);
        }
        catch (RecognitionException e) {
            throw Throwables.propagate(e);
        }

        SessionMetadata sessionMetadata = new SessionMetadata(metadata);
        sessionMetadata.using(TpchSchema.CATALOG_NAME, TpchSchema.SCHEMA_NAME);

        Analyzer analyzer = new Analyzer(sessionMetadata);

        AnalysisResult analysis = analyzer.analyze(statement);

        Planner planner = new Planner();
        PlanNode plan = planner.plan((Query) statement, analysis);
        new PlanPrinter().print(plan, analysis.getTypes());

        FragmentPlanner fragmentPlanner = new FragmentPlanner(sessionMetadata);
        List<PlanFragment> fragments = fragmentPlanner.createFragments(plan, analysis.getSymbolAllocator(), true);

        TableScan tableScan = (TableScan) Iterables.getOnlyElement(fragments).getSources().get(0);
        TpchTableHandle table = (TpchTableHandle) tableScan.getTable();

        PlanFragmentSource tableScanSource = new TableScanPlanFragmentSource(new TpchSplit(table));
        ExecutionPlanner executionPlanner = new ExecutionPlanner(sessionMetadata,
                new HackPlanFragmentSourceProvider(dataProvider, QUERY_TASK_INFO_CODEC),
                analysis.getTypes(),
                tableScanSource,
                ImmutableMap.<String, ExchangePlanFragmentSource>of(),
                new SourceHashProviderFactory());
        Operator operator = executionPlanner.plan(plan);

        TupleInfo outputTupleInfo = ExecutionPlanner.toTupleInfo(analysis, plan.getOutputSymbols());

        ImmutableList.Builder<Tuple> output = ImmutableList.builder();

        for (Page page : operator) {
            ImmutableList.Builder<BlockCursor> cursorBuilder = ImmutableList.builder();
            for (Block block : page.getBlocks()) {
                cursorBuilder.add(block.cursor());
            }

            List<BlockCursor> cursors = cursorBuilder.build();

            boolean done = false;
            while (!done) {
                TupleInfo.Builder outputBuilder = outputTupleInfo.builder();
                done = true;
                for (BlockCursor cursor : cursors) {
                    if (!cursor.advanceNextPosition()) {
                        break;
                    }
                    done = false;

                    outputBuilder.append(cursor.getTuple());
                }

                if (!done) {
                    output.add(outputBuilder.build());
                }
            }
        }

        return output.build();
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

    private static void insertRows(String table, Handle handle, RecordIterable data)
    {
        checkArgument(!isEmpty(data), "no data to insert");
        int columns = Iterables.get(data, 0).getFieldCount();
        String vars = Joiner.on(',').join(nCopies(columns, "?"));
        String sql = format("INSERT INTO %s VALUES (%s)", table, vars);

        for (List<Record> partition : Iterables.partition(data, 1000)) {
            PreparedBatch batch = handle.prepareBatch(sql);
            for (Record record : partition) {
                checkArgument(record.getFieldCount() == columns, "rows have differing column counts");
                PreparedBatchPart part = batch.add();
                for (int i = 0; i < record.getFieldCount(); i++) {
                    part.bind(i, record.getString(i));
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
                            builder.append(rs.getLong(i));
                            break;
                        case DOUBLE:
                            builder.append(rs.getDouble(i));
                            break;
                        case VARIABLE_BINARY:
                            String value = rs.getString(i);
                            builder.append(Slices.wrappedBuffer(value.getBytes(Charsets.UTF_8)));
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

    private static RecordIterable readRecords(String name, int expectedRows)
            throws IOException
    {
        Splitter splitter = Splitter.on('|');
        List<Record> records = new ArrayList<>();
        for (String line : CharStreams.readLines(readResource(name))) {
            checkArgument(!line.isEmpty(), "line is empty");
            checkArgument(line.charAt(line.length() - 1) == '|', "line does not end in delimiter");
            line = line.substring(0, line.length() - 1);
            records.add(new StringRecord(splitter.split(line)));
        }
        checkArgument(records.size() == expectedRows, "expected %s rows, but read %s", expectedRows, records.size());
        return RecordIterables.asRecordIterable(records);
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
                return new InputStreamReader(gzip, Charsets.UTF_8);
            }
        };
    }

    private static class TestTpchBlocksProvider
        implements TpchBlocksProvider
    {
        private final Map<String, RecordIterable> data;

        private TestTpchBlocksProvider(Map<String, RecordIterable> data)
        {
            Preconditions.checkNotNull(data, "data is null");
            this.data = ImmutableMap.copyOf(data);
        }

        @Override
        public BlockIterable getBlocks(final TpchTableHandle tableHandle, final TpchColumnHandle columnHandle, BlocksFileEncoding encoding)
        {
            return new BlockIterable()
            {
                @Override
                public TupleInfo getTupleInfo()
                {
                    return new TupleInfo(columnHandle.getType());
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
                    return new AbstractIterator<Block>() {
                        private final RecordIterator iterator = data.get(tableHandle.getTableName()).iterator();
                        private final RecordProjection projection = RecordProjections.createProjection(columnHandle.getFieldIndex(), columnHandle.getType());

                        @Override
                        protected Block computeNext()
                        {
                            BlockBuilder builder = new BlockBuilder(new TupleInfo(columnHandle.getType()));

                            while (iterator.hasNext() && !builder.isFull()) {
                                Record record = iterator.next();
                                projection.project(record, builder);
                            }

                            if (builder.isEmpty()) {
                                return endOfData();
                            }

                            return builder.build();
                        }
                    };
                }
            };
        }

        @Override
        public DataSize getColumnDataSize(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, BlocksFileEncoding encoding)
        {
            throw new UnsupportedOperationException();
        }
    }
}
