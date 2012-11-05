package com.facebook.presto;

import com.facebook.presto.ingest.Record;
import com.facebook.presto.ingest.RecordIterable;
import com.facebook.presto.ingest.RecordIterables;
import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.ingest.RecordProjection;
import com.facebook.presto.ingest.StringRecord;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.aggregation.CountAggregation;
import com.facebook.presto.operator.aggregation.DoubleAverageAggregation;
import com.facebook.presto.operator.aggregation.DoubleSumAggregation;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.tpch.TpchSchema.Column;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
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
import java.util.List;
import java.util.zip.GZIPInputStream;

import static com.facebook.presto.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.ingest.RecordProjections.createProjection;
import static com.facebook.presto.operator.ProjectionFunctions.concat;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.tpch.TpchSchema.LineItem.DISCOUNT;
import static com.facebook.presto.tpch.TpchSchema.LineItem.TAX;
import static com.facebook.presto.tpch.TpchSchema.Orders.ORDERKEY;
import static com.facebook.presto.tpch.TpchSchema.Orders.ORDERSTATUS;
import static com.facebook.presto.tpch.TpchSchema.Orders.TOTALPRICE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.isEmpty;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;

public class TestQueries
{
    private Handle handle;
    private RecordIterable ordersRecords;
    private RecordIterable lineItemRecords;

    @BeforeSuite
    public void setupDatabase()
            throws IOException
    {
        handle = DBI.open("jdbc:h2:mem:test" + System.nanoTime());

        ordersRecords = readRecords("tpch/orders.dat.gz", 15000);
        handle.execute("CREATE TABLE orders (\n" +
                "  orderkey BIGINT NOT NULL,\n" +
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
                "  orderkey BIGINT NOT NULL,\n" +
                "  partkey BIGINT NOT NULL,\n" +
                "  suppkey BIGINT NOT NULL,\n" +
                "  linenumber BIGINT NOT NULL,\n" +
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
                "  comment VARCHAR(44) NOT NULL\n" +
                ")");
        insertRows("lineitem", handle, lineItemRecords);
    }

    @AfterSuite
    public void cleanupDatabase()
    {
        handle.close();
    }

    @Test
    public void testCountAll()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM orders", FIXED_INT_64);

        Operator orders = scanTable(ordersRecords, ORDERKEY);
        Operator aggregation = new AggregationOperator(orders,
                ImmutableList.of(CountAggregation.PROVIDER),
                ImmutableList.of(singleColumn(FIXED_INT_64, 0, 0)));
        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    @Test
    public void testAverageAll()
    {
        List<Tuple> expected = computeExpected("SELECT AVG(totalprice) FROM orders", DOUBLE);

        Operator orders = scanTable(ordersRecords, TOTALPRICE);
        Operator aggregation = new AggregationOperator(orders,
                ImmutableList.of(DoubleAverageAggregation.provider(0, 0)),
                ImmutableList.of(singleColumn(DOUBLE, 0, 0)));

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    @Test
    public void testCountAllWithPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM orders WHERE orderstatus = 'F'", FIXED_INT_64);

        Operator orders = scanTable(ordersRecords, ORDERSTATUS);
        Operator filter = new FilterAndProjectOperator(orders,
                new FilterFunction()
                {
                    @Override
                    public boolean filter(BlockCursor[] cursors)
                    {
                        return cursors[0].getSlice(0).equals(Slices.copiedBuffer("F", Charsets.UTF_8));
                    }
                },
                singleColumn(VARIABLE_BINARY, 0, 0));
        Operator aggregation = new AggregationOperator(filter,
                ImmutableList.of(CountAggregation.PROVIDER),
                ImmutableList.of(singleColumn(FIXED_INT_64, 0, 0)));

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }


    @Test
    public void testGroupByCount()
    {
        List<Tuple> expected = computeExpected("SELECT orderstatus, CAST(COUNT(*) AS INTEGER) FROM orders GROUP BY orderstatus", VARIABLE_BINARY, FIXED_INT_64);

        Operator orders = scanTable(ordersRecords, ORDERSTATUS);
        Operator aggregation = new HashAggregationOperator(orders,
                0,
                ImmutableList.of(CountAggregation.PROVIDER),
                ImmutableList.of(concat(singleColumn(VARIABLE_BINARY, 0, 0), singleColumn(FIXED_INT_64, 1, 0))));

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    @Test
    public void testGroupBySum()
    {
        List<Tuple> expected = computeExpected(
                "SELECT orderstatus, SUM(totalprice) FROM orders GROUP BY orderstatus",
                VARIABLE_BINARY, DOUBLE);

        Operator orders = scanTable(ordersRecords, ORDERSTATUS, TOTALPRICE);
        Operator aggregation = new HashAggregationOperator(orders,
                0,
                ImmutableList.of(DoubleSumAggregation.provider(1, 0)),
                ImmutableList.of(concat(singleColumn(VARIABLE_BINARY, 0, 0), singleColumn(DOUBLE, 1, 0))));

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    @Test
    public void testCountAllWithComparison()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM lineitem WHERE tax < discount", FIXED_INT_64);

        Operator lineItems = scanTable(lineItemRecords, DISCOUNT, TAX);
        Operator filter = new FilterAndProjectOperator(lineItems,
                new FilterFunction()
                {
                    @Override
                    public boolean filter(BlockCursor[] cursors)
                    {
                        return cursors[1].getDouble(0) < cursors[0].getDouble(0);
                    }
                },
                singleColumn(DOUBLE, 0, 0));
        Operator aggregation = new AggregationOperator(filter,
                ImmutableList.of(CountAggregation.PROVIDER),
                ImmutableList.of(singleColumn(FIXED_INT_64, 0, 0)));

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    @Test
    public void testSelectWithComparison()
    {
        List<Tuple> expected = computeExpected("SELECT orderkey FROM lineitem WHERE tax < discount", FIXED_INT_64);

        Operator lineItems = scanTable(lineItemRecords, ORDERKEY, DISCOUNT, TAX);
        Operator filter = new FilterAndProjectOperator(lineItems,
                new FilterFunction()
                {
                    @Override
                    public boolean filter(BlockCursor[] cursors)
                    {
                        return cursors[2].getDouble(0) < cursors[1].getDouble(0);
                    }
                },
                singleColumn(FIXED_INT_64, 0, 0));

        assertEqualsIgnoreOrder(tuples(filter), expected);
    }

    @Test
    public void testCountWithAndPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM lineitem WHERE tax < discount AND tax > 0.01 AND discount < 0.05", FIXED_INT_64);

        Operator lineItems = scanTable(lineItemRecords, DISCOUNT, TAX);
        Operator filter = new FilterAndProjectOperator(lineItems,
                new FilterFunction()
                {
                    @Override
                    public boolean filter(BlockCursor[] cursors)
                    {
                        double discount = cursors[0].getDouble(0);
                        double tax = cursors[1].getDouble(0);
                        return tax < discount && tax > 0.01 && discount < 0.05;
                    }
                },
                singleColumn(DOUBLE, 0, 0));
        Operator aggregation = new AggregationOperator(filter,
                ImmutableList.of(CountAggregation.PROVIDER),
                ImmutableList.of(singleColumn(FIXED_INT_64, 0, 0)));

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    @Test
    public void testCountWithOrPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM lineitem WHERE tax < 0.01 OR discount > 0.05", FIXED_INT_64);

        Operator lineItems = scanTable(lineItemRecords, DISCOUNT, TAX);
        Operator filter = new FilterAndProjectOperator(lineItems,
                new FilterFunction()
                {
                    @Override
                    public boolean filter(BlockCursor[] cursors)
                    {
                        double discount = cursors[0].getDouble(0);
                        double tax = cursors[1].getDouble(0);
                        return tax < 0.01 || discount > 0.05;
                    }
                },
                singleColumn(DOUBLE, 0, 0));
        Operator aggregation = new AggregationOperator(filter,
                ImmutableList.of(CountAggregation.PROVIDER),
                ImmutableList.of(singleColumn(FIXED_INT_64, 0, 0)));

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    private List<Tuple> computeExpected(final String sql, TupleInfo.Type... types)
    {
        TupleInfo tupleInfo = new TupleInfo(types);
        return handle.createQuery(sql)
                .map(tupleMapper(tupleInfo))
                .list();
    }

    private static List<Tuple> tuples(Operator operator)
    {
        assertEquals(operator.getChannelCount(), 1);
        List<Tuple> tuples = new ArrayList<>();
        for (Page page : operator) {
            BlockCursor cursor = page.getBlock(0).cursor();
            while (cursor.advanceNextPosition()) {
                tuples.add(cursor.getTuple());
            }
        }
        return tuples;
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

    private static Operator scanTable(RecordIterable records, TpchSchema.Column... columns)
    {
        ImmutableList.Builder<RecordProjection> projections = ImmutableList.builder();
        for (Column column : columns) {
            projections.add(createProjection(column.getIndex(), column.getType()));
        }
        return new RecordProjectOperator(records, projections.build());
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
}
