package com.facebook.presto;

import com.facebook.presto.aggregation.AverageAggregation;
import com.facebook.presto.aggregation.CountAggregation;
import com.facebook.presto.aggregation.DoubleSumAggregation;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.block.uncompressed.UncompressedTupleStream;
import com.facebook.presto.operation.DoubleLessThanComparison;
import com.facebook.presto.operator.*;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tpch.TpchSchema;
import com.google.common.base.*;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import org.skife.jdbi.v2.*;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

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

import static com.facebook.presto.TupleInfo.Type.*;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static java.lang.String.format;
import static java.util.Collections.nCopies;

public class TestQueries
{
    private Handle handle;
    private List<List<String>> ordersData;
    private List<List<String>> lineitemData;

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
                "  clerk CHAR(15) NOT NULL,\n" +
                "  shippriority BIGINT NOT NULL,\n" +
                "  comment VARCHAR(79) NOT NULL\n" +
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
                "  shipmode VARCHAR(10) NOT NULL,\n" +
                "  comment VARCHAR(44) NOT NULL\n" +
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

        TupleStream orders = createTupleStream(ordersData, TpchSchema.Orders.ORDERKEY, FIXED_INT_64);
        AggregationOperator aggregation = new AggregationOperator(orders, CountAggregation.PROVIDER);

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    @Test
    public void testAverageAll()
    {
        List<Tuple> expected = computeExpected("SELECT AVG(totalprice) FROM orders", DOUBLE);

        TupleStream price = createTupleStream(ordersData, TpchSchema.Orders.TOTALPRICE, DOUBLE);
        AggregationOperator aggregation = new AggregationOperator(price, AverageAggregation.PROVIDER);

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    @Test
    public void testCountAllWithPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM orders WHERE orderstatus = 'F'", FIXED_INT_64);

        TupleStream orderStatus = createTupleStream(ordersData, TpchSchema.Orders.ORDERSTATUS, VARIABLE_BINARY);

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

        TupleStream groupBySource = createTupleStream(ordersData, TpchSchema.Orders.ORDERSTATUS, VARIABLE_BINARY);
        TupleStream aggregateSource = createTupleStream(ordersData, TpchSchema.Orders.ORDERSTATUS, VARIABLE_BINARY);

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

        TupleStream groupBySource = createTupleStream(ordersData, TpchSchema.Orders.ORDERSTATUS, VARIABLE_BINARY);
        TupleStream aggregateSource = createTupleStream(ordersData, TpchSchema.Orders.TOTALPRICE, DOUBLE);

        GroupByOperator groupBy = new GroupByOperator(groupBySource);
        HashAggregationOperator aggregation = new HashAggregationOperator(groupBy, aggregateSource, DoubleSumAggregation.PROVIDER);

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    @Test
    public void testCountAllWithComparison()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM lineitem WHERE tax < discount", FIXED_INT_64);

        TupleStream discount = createTupleStream(lineitemData, TpchSchema.LineItem.DISCOUNT, DOUBLE);
        TupleStream tax = createTupleStream(lineitemData, TpchSchema.LineItem.TAX, DOUBLE);

        ComparisonOperator comparison = new ComparisonOperator(tax, discount, new DoubleLessThanComparison());
        AggregationOperator aggregation = new AggregationOperator(comparison, CountAggregation.PROVIDER);

        assertEqualsIgnoreOrder(tuples(aggregation), expected);
    }

    @Test
    public void testSelectWithComparison()
    {
        List<Tuple> expected = computeExpected("SELECT orderkey FROM lineitem WHERE tax < discount", FIXED_INT_64);

        UncompressedTupleStream orderKey = createTupleStream(lineitemData, TpchSchema.LineItem.ORDERKEY, FIXED_INT_64);
        TupleStream discount = createTupleStream(lineitemData, TpchSchema.LineItem.DISCOUNT, DOUBLE);
        TupleStream tax = createTupleStream(lineitemData, TpchSchema.LineItem.TAX, DOUBLE);

        ComparisonOperator comparison = new ComparisonOperator(tax, discount, new DoubleLessThanComparison());
        FilterOperator result = new FilterOperator(orderKey.getTupleInfo(), orderKey, comparison);

        assertEqualsIgnoreOrder(tuples(result), expected);
    }

    @Test
    public void testCountWithAndPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM lineitem WHERE tax < discount AND tax > 0.01 AND discount < 0.05", FIXED_INT_64);

        TupleStream discount = createTupleStream(lineitemData, TpchSchema.LineItem.DISCOUNT, DOUBLE);
        TupleStream filteredDiscount = new ApplyPredicateOperator(discount, new Predicate<Cursor>()
        {
            @Override
            public boolean apply(Cursor input)
            {
                return input.getDouble(0) < 0.05;
            }
        });

        TupleStream tax = createTupleStream(lineitemData, TpchSchema.LineItem.TAX, DOUBLE);
        TupleStream filteredTax = new ApplyPredicateOperator(tax, new Predicate<Cursor>()
        {
            @Override
            public boolean apply(Cursor input)
            {
                return input.getDouble(0) > 0.01;
            }
        });

        // TODO: use tax and discount directly once RowSourceBuilder is fixed to allow multiple cursors
        TupleStream tax2 = createTupleStream(lineitemData, TpchSchema.LineItem.TAX, DOUBLE);
        TupleStream discount2 = createTupleStream(lineitemData, TpchSchema.LineItem.DISCOUNT, DOUBLE);

        ComparisonOperator comparison = new ComparisonOperator(tax2, discount2, new DoubleLessThanComparison());
        AndOperator and = new AndOperator(filteredDiscount, filteredTax, comparison);
        AggregationOperator count = new AggregationOperator(and, CountAggregation.PROVIDER);

        assertEqualsIgnoreOrder(tuples(count), expected);
    }

    @Test
    public void testCountWithOrPredicate()
    {
        List<Tuple> expected = computeExpected("SELECT COUNT(*) FROM lineitem WHERE tax < 0.01 OR discount > 0.05", FIXED_INT_64);

        TupleStream discount = createTupleStream(lineitemData, TpchSchema.LineItem.DISCOUNT, DOUBLE);
        TupleStream filteredDiscount = new ApplyPredicateOperator(discount, new Predicate<Cursor>()
        {
            @Override
            public boolean apply(Cursor input)
            {
                return input.getDouble(0) > 0.05;
            }
        });

        TupleStream tax = createTupleStream(lineitemData, TpchSchema.LineItem.TAX, DOUBLE);
        TupleStream filteredTax = new ApplyPredicateOperator(tax, new Predicate<Cursor>()
        {
            @Override
            public boolean apply(Cursor input)
            {
                return input.getDouble(0) < 0.01;
            }
        });

        OrOperator or = new OrOperator(filteredDiscount, filteredTax);
        AggregationOperator count = new AggregationOperator(or, CountAggregation.PROVIDER);

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

    private static UncompressedTupleStream createTupleStream(final List<List<String>> data, final TpchSchema.Column column, final TupleInfo.Type type)
    {
        return new UncompressedTupleStream(
                new TupleInfo(type),
                new Iterable<UncompressedBlock>() {
                    @Override
                    public Iterator<UncompressedBlock> iterator()
                    {
                        return new RowStringUncompressedBlockIterator(type, column.getIndex(), data.iterator());
                    }
                }
        );
    }

    // Given a list of string rows, extracts UncompressedBlocks for a particular column
    private static class RowStringUncompressedBlockIterator
            extends AbstractIterator<UncompressedBlock>
    {
        private final TupleInfo.Type type;
        private final TupleInfo tupleInfo;
        private final int extractedColumnIndex; // Can only extract one column index
        private final Iterator<List<String>> rowStringIterator;
        private long position = 0;

        private RowStringUncompressedBlockIterator(TupleInfo.Type type, int extractedColumnIndex, Iterator<List<String>> rowStringIterator)
        {
            this.type = checkNotNull(type, "type is null");
            tupleInfo = new TupleInfo(type);
            checkArgument(extractedColumnIndex >= 0, "extractedColumnIndex must be greater than or equal to zero");
            this.extractedColumnIndex = extractedColumnIndex;
            this.rowStringIterator = checkNotNull(rowStringIterator, "rowStringIterator is null");;
        }

        @Override
        protected UncompressedBlock computeNext()
        {
            if (!rowStringIterator.hasNext()) {
                return endOfData();
            }
            BlockBuilder blockBuilder = new BlockBuilder(position, tupleInfo);
            while (rowStringIterator.hasNext() && !blockBuilder.isFull()) {
                type.getStringValueConverter().convert(rowStringIterator.next().get(extractedColumnIndex), blockBuilder);
            }
            UncompressedBlock block = blockBuilder.build();
            position += block.getCount();
            return block;
        }
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
            checkArgument(!line.isEmpty(), "line is empty");
            checkArgument(line.charAt(line.length() - 1) == '|', "line does not end in delimiter");
            line = line.substring(0, line.length() - 1);
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
