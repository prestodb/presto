package com.facebook.presto;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.ingest.Record;
import com.facebook.presto.ingest.RecordIterable;
import com.facebook.presto.ingest.RecordIterables;
import com.facebook.presto.ingest.RecordIterator;
import com.facebook.presto.ingest.RecordProjection;
import com.facebook.presto.ingest.RecordProjections;
import com.facebook.presto.ingest.StringRecord;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
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
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
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
    private RecordIterable ordersRecords;
    private RecordIterable lineItemRecords;
    private Metadata metadata;
    private TpchDataStreamProvider dataProvider;

    @Test
    public void testDistinct()
            throws Exception
    {
        assertQuery("SELECT DISTINCT custkey FROM orders");
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
        assertQueryOrdered("SELECT custkey, orderstatus FROM ORDERS ORDER BY orderkey desc LIMIT 10");
    }

    @Test
    public void testOrderByExpressionWithLimit()
            throws Exception
    {
        assertQueryOrdered("SELECT custkey, orderstatus FROM ORDERS ORDER BY orderkey + 1 desc LIMIT 10");
    }

    @Test
    public void testGroupByOrderByLimit()
            throws Exception
    {
        assertQueryOrdered("SELECT custkey, sum(totalprice) FROM ORDERS GROUP BY custkey ORDER BY sum(totalprice) desc LIMIT 10");
    }

    @Test
    public void testRepeatedAggregations()
            throws Exception
    {
        assertQuery("SELECT sum(orderkey), sum(orderkey) FROM ORDERS");
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
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM ORDERS");
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
        assertQuery(
                "SELECT COUNT(*) FROM lineitem join orders using (orderkey)",
                "SELECT COUNT(*) FROM lineitem join orders on lineitem.orderkey = orders.orderkey"
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
            throws RecognitionException
    {
        Statement statement = SqlParser.createStatement(sql);

        Session session = new Session(TpchSchema.CATALOG_NAME, TpchSchema.SCHEMA_NAME);

        Analyzer analyzer = new Analyzer(session, metadata);

        AnalysisResult analysis = analyzer.analyze(statement);

        PlanNode plan = new LogicalPlanner().plan((Query) statement, analysis);
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
        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(metadata,
                new HackPlanFragmentSourceProvider(dataProvider, null, TASK_INFO_CODEC),
                analysis.getTypes(),
                null,
                builder.build(),
                ImmutableMap.<String, ExchangePlanFragmentSource>of(),
                new OperatorStats(),
                new SourceHashProviderFactory(maxOperatorMemoryUsage),
                maxOperatorMemoryUsage,
                1_000_000);

        return new FilterAndProjectOperator(executionPlanner.plan(plan),
                FilterFunctions.TRUE_FUNCTION,
                new Concat(executionPlanner.plan(plan).getTupleInfos()));
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
        checkArgument(data.iterator(new OperatorStats()).hasNext(), "no data to insert");
        int columns = data.iterator(new OperatorStats()).next().getFieldCount();
        String vars = Joiner.on(',').join(nCopies(columns, "?"));
        String sql = format("INSERT INTO %s VALUES (%s)", table, vars);

        UnmodifiableIterator<List<Record>> partitions = Iterators.partition(data.iterator(new OperatorStats()), 1000);
        while (partitions.hasNext()) {
            List<Record> partition = partitions.next();
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
                    return new AbstractIterator<Block>()
                    {
                        private final RecordIterator iterator = data.get(tableHandle.getTableName()).iterator(new OperatorStats());
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
