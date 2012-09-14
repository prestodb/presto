package com.facebook.presto;

import com.facebook.presto.aggregation.AverageAggregation;
import com.facebook.presto.aggregation.CountAggregation;
import com.facebook.presto.aggregation.DoubleSumAggregation;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.ingest.RowSourceBuilder;
import com.facebook.presto.operation.DoubleLessThanComparison;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.AndOperator;
import com.facebook.presto.operator.ApplyPredicateOperator;
import com.facebook.presto.operator.ComparisonOperator;
import com.facebook.presto.operator.FilterOperator;
import com.facebook.presto.operator.GroupByOperator;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static com.facebook.presto.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.ingest.RowSourceBuilder.RowGenerator;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static java.lang.String.format;
import static java.util.Collections.nCopies;

public class TestQueries
{
    private Handle handle;
    private List<List<String>> ordersData;
    private List<List<String>> lineitemData;

    private enum Column
    {
        ORDER_ORDERKEY(0),
        ORDER_CUSTKEY(1),
        ORDER_ORDERSTATUS(2),
        ORDER_TOTALPRICE(3),
        ORDER_ORDERDATE(4),
        ORDER_ORDERPRIORITY(5),
        ORDER_SHIPPRIORITY(6),

        LINEITEM_ORDERKEY(0),
        LINEITEM_DISCOUNT(6),
        LINEITEM_TAX(7);

        private final int index;

        Column(int index)
        {
            this.index = index;
        }

        public int getIndex()
        {
            return index;
        }
    }

    @BeforeSuite
    public void setupDatabase()
            throws IOException
    {
        handle = DBI.open("jdbc:h2:mem:test" + System.nanoTime());

        ordersData = readTestData("tpch/orders.dat.gz", 15000);
        handle.execute("CREATE TABLE orders (\n" +
                "  orderkey BIGINT NOT NULL,\n" +
                "  custkey BIGINT NOT NULL,\n" +
                "  orderstatus CHAR(1) NOT NULL,\n" +
                "  totalprice DOUBLE NOT NULL,\n" +
                "  orderdate CHAR(10) NOT NULL,\n" +
                "  orderpriority CHAR(15) NOT NULL,\n" +
                "  shippriority BIGINT NOT NULL\n" +
                ")");
        insertRows("orders", handle, ordersData);

        lineitemData = readTestData("tpch/lineitem.dat.gz", 60175);
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
                "  shipmode VARCHAR(10) NOT NULL\n" +
                ")");
        insertRows("lineitem", handle, lineitemData);
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

        TupleStream orders = createTupleStream(ordersData, Column.ORDER_ORDERKEY, FIXED_INT_64);
        AggregationOperator aggregation = new AggregationOperator(orders, CountAggregation.PROVIDER);

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    @Test
    public void testAverageAll()
    {
        List<Tuple> expected = computeExpected("SELECT AVG(totalprice) FROM orders", DOUBLE);

        TupleStream price = createTupleStream(ordersData, Column.ORDER_TOTALPRICE, DOUBLE);
        AggregationOperator aggregation = new AggregationOperator(price, AverageAggregation.PROVIDER);

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    @Test
    public void testCountAllWithPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM orders WHERE orderstatus = 'F'", FIXED_INT_64);

        TupleStream orderStatus = createTupleStream(ordersData, Column.ORDER_ORDERSTATUS, VARIABLE_BINARY);

        ApplyPredicateOperator filtered = new ApplyPredicateOperator(orderStatus, new Predicate<Cursor>()
        {
            @Override
            public boolean apply(Cursor input)
            {
                return input.getSlice(0).equals(Slices.copiedBuffer("F", Charsets.UTF_8));
            }
        });

        AggregationOperator aggregation = new AggregationOperator(filtered, CountAggregation.PROVIDER);

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }


    @Test
    public void testGroupByCount()
    {
        List<Tuple> expected = computeExpected("SELECT orderstatus, CAST(COUNT(*) AS INTEGER) FROM orders GROUP BY orderstatus", VARIABLE_BINARY, FIXED_INT_64);

        TupleStream groupBySource = createTupleStream(ordersData, Column.ORDER_ORDERSTATUS, VARIABLE_BINARY);
        TupleStream aggregateSource = createTupleStream(ordersData, Column.ORDER_ORDERSTATUS, VARIABLE_BINARY);

        GroupByOperator groupBy = new GroupByOperator(groupBySource);
        HashAggregationOperator aggregation = new HashAggregationOperator(groupBy, aggregateSource, CountAggregation.PROVIDER);

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    @Test
    public void testGroupBySum()
    {
        List<Tuple> expected = computeExpected(
                "SELECT orderstatus, SUM(totalprice) FROM orders GROUP BY orderstatus",
                VARIABLE_BINARY, DOUBLE);

        TupleStream groupBySource = createTupleStream(ordersData, Column.ORDER_ORDERSTATUS, VARIABLE_BINARY);
        TupleStream aggregateSource = createTupleStream(ordersData, Column.ORDER_TOTALPRICE, DOUBLE);

        GroupByOperator groupBy = new GroupByOperator(groupBySource);
        HashAggregationOperator aggregation = new HashAggregationOperator(groupBy, aggregateSource, DoubleSumAggregation.PROVIDER);

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    @Test
    public void testCountAllWithComparison()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM lineitem WHERE tax < discount", FIXED_INT_64);

        TupleStream discount = createTupleStream(lineitemData, Column.LINEITEM_DISCOUNT, DOUBLE);
        TupleStream tax = createTupleStream(lineitemData, Column.LINEITEM_TAX, DOUBLE);

        ComparisonOperator comparison = new ComparisonOperator(tax, discount, new DoubleLessThanComparison());
        AggregationOperator aggregation = new AggregationOperator(comparison, CountAggregation.PROVIDER);

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    @Test
    public void testSelectWithComparison()
    {
        List<Tuple> expected = computeExpected("SELECT orderkey FROM lineitem WHERE tax < discount", FIXED_INT_64);

        RowSourceBuilder orderKey = createTupleStream(lineitemData, Column.LINEITEM_ORDERKEY, FIXED_INT_64);
        TupleStream discount = createTupleStream(lineitemData, Column.LINEITEM_DISCOUNT, DOUBLE);
        TupleStream tax = createTupleStream(lineitemData, Column.LINEITEM_TAX, DOUBLE);

        ComparisonOperator comparison = new ComparisonOperator(tax, discount, new DoubleLessThanComparison());
        FilterOperator result = new FilterOperator(orderKey.getTupleInfo(), orderKey, comparison);

        assertEqualsIgnoreOrder(tuples(result), expected);
    }

    @Test
    public void testCountWithAndPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM lineitem WHERE tax < discount AND tax > 0.01 AND discount < 10.0", FIXED_INT_64);

        TupleStream discount = createTupleStream(lineitemData, Column.LINEITEM_DISCOUNT, DOUBLE);
        TupleStream filteredDiscount = new ApplyPredicateOperator(discount, new Predicate<Cursor>()
        {
            @Override
            public boolean apply(Cursor input)
            {
                return input.getDouble(0) < 10.0;
            }
        });

        TupleStream tax = createTupleStream(lineitemData, Column.LINEITEM_TAX, DOUBLE);
        TupleStream filteredTax = new ApplyPredicateOperator(tax, new Predicate<Cursor>()
        {
            @Override
            public boolean apply(Cursor input)
            {
                return input.getDouble(0) > 0.01;
            }
        });

        // TODO: use tax and discount directly once RowSourceBuilder is fixed to allow multiple cursors
        TupleStream tax2 = createTupleStream(lineitemData, Column.LINEITEM_TAX, DOUBLE);
        TupleStream discount2 = createTupleStream(lineitemData, Column.LINEITEM_DISCOUNT, DOUBLE);

        ComparisonOperator comparison = new ComparisonOperator(tax2, discount2, new DoubleLessThanComparison());
        AndOperator and = new AndOperator(filteredDiscount, filteredTax, comparison);
        AggregationOperator count = new AggregationOperator(and, CountAggregation.PROVIDER);

        assertEqualsIgnoreOrder(tuples(count), expected);
    }


    private List<Tuple> computeExpected(final String sql, TupleInfo.Type... types)
    {
        TupleInfo tupleInfo = new TupleInfo(types);
        return handle.createQuery(sql)
                .map(tupleMapper(tupleInfo))
                .list();
    }

    private static List<Tuple> tuples(TupleStream tupleStream)
    {
        Cursor cursor = tupleStream.cursor();
        List<Tuple> list = new ArrayList<>();
        while (cursor.advanceNextPosition()) {
            list.add(cursor.getTuple());
        }
        return list;
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

    private static RowSourceBuilder createTupleStream(List<List<String>> data, final Column column, final TupleInfo.Type type)
    {
        final Iterator<List<String>> iterator = data.iterator();
        return new RowSourceBuilder(new TupleInfo(type), new RowGenerator()
        {
            @Override
            public boolean generate(RowSourceBuilder.RowBuilder rowBuilder)
            {
                if (!iterator.hasNext()) {
                    return false;
                }
                String value = iterator.next().get(column.getIndex());
                switch (type) {
                    case FIXED_INT_64:
                        rowBuilder.append(Long.parseLong(value));
                        break;
                    case DOUBLE:
                        rowBuilder.append(Double.parseDouble(value));
                        break;
                    case VARIABLE_BINARY:
                        rowBuilder.append(value.getBytes(Charsets.UTF_8));
                        break;
                    default:
                        throw new AssertionError("unhandled type: " + type);
                }
                return true;
            }

            @Override
            public void close() {}
        });
    }

    private static void insertRows(String table, Handle handle, List<List<String>> data)
    {
        checkArgument(!data.isEmpty(), "no data to insert");
        int columns = data.get(0).size();
        String vars = Joiner.on(',').join(nCopies(columns, "?"));
        String sql = format("INSERT INTO %s VALUES (%s)", table, vars);

        for (List<List<String>> rows : Lists.partition(data, 1000)) {
            PreparedBatch batch = handle.prepareBatch(sql);
            for (List<String> row : rows) {
                checkArgument(row.size() == columns, "rows have differing column counts");
                PreparedBatchPart part = batch.add();
                for (int i = 0; i < row.size(); i++) {
                    part.bind(i, row.get(i));
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

    private static List<List<String>> readTestData(String name, int expectedRows)
            throws IOException
    {
        List<List<String>> data = readDelimited(readResource(name));
        checkArgument(data.size() == expectedRows, "expected %s rows, but read %s", expectedRows, data.size());
        return data;
    }

    private static List<List<String>> readDelimited(InputSupplier<? extends Reader> inputSupplier)
            throws IOException
    {
        Splitter splitter = Splitter.on('|');
        List<List<String>> list = new ArrayList<>();
        for (String line : CharStreams.readLines(inputSupplier)) {
            list.add(ImmutableList.copyOf(splitter.split(line)));
        }
        return list;
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
