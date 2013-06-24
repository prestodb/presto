package com.facebook.presto.operator;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.window.RowNumberFunction;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.operator.CancelTester.assertCancel;
import static com.facebook.presto.operator.CancelTester.createCancelableDataSource;
import static com.facebook.presto.operator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.tuple.TupleInfo.Type.BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;

public class TestInMemoryWindowOperator
{
    private static final DataSize MAX_SIZE = new DataSize(1, Unit.MEGABYTE);
    private static final List<WindowFunction> ROW_NUMBER = ImmutableList.<WindowFunction>of(new RowNumberFunction());

    @Test
    public void testRowNumber()
            throws Exception
    {
        Operator source = createOperator(
                new Page(
                        createLongsBlock(2, 4, 6),
                        createDoublesBlock(0.3, 0.2, 0.1)),
                new Page(
                        createLongsBlock(-1, 5),
                        createDoublesBlock(-0.1, 0.4)));

        Operator actual = new InMemoryWindowOperator(
                source,
                0,
                ints(1, 0),
                ROW_NUMBER,
                ints(),
                ints(0),
                bools(true),
                10,
                new TaskMemoryManager(MAX_SIZE));

        Operator expected = createOperator(new Page(
                createDoublesBlock(-0.1, 0.3, 0.2, 0.4, 0.1),
                createLongsBlock(-1, 2, 4, 5, 6),
                createLongsBlock(1, 2, 3, 4, 5)));

        assertOperatorEquals(actual, expected);
    }

    @Test
    public void testRowNumberPartition()
            throws Exception
    {
        TupleInfo tupleInfo = new TupleInfo(VARIABLE_BINARY, FIXED_INT_64, DOUBLE, BOOLEAN);

        Operator source = createOperator(
                new Page(new BlockBuilder(tupleInfo)
                        .append("b").append(-1).append(-0.1).append(true)
                        .append("a").append(2).append(0.3).append(false)
                        .append("a").append(4).append(0.2).append(true)
                        .build()),
                new Page(new BlockBuilder(tupleInfo)
                        .append("b").append(5).append(0.4).append(false)
                        .append("a").append(6).append(0.1).append(true)
                        .build())
        );

        Operator actual = new InMemoryWindowOperator(
                source,
                0,
                ints(0),
                ROW_NUMBER,
                ints(0),
                ints(1),
                bools(true),
                10,
                new TaskMemoryManager(MAX_SIZE));

        Operator expected = createOperator(new Page(
                new BlockBuilder(tupleInfo)
                        .append("a").append(2).append(0.3).append(false)
                        .append("a").append(4).append(0.2).append(true)
                        .append("a").append(6).append(0.1).append(true)
                        .append("b").append(-1).append(-0.1).append(true)
                        .append("b").append(5).append(0.4).append(false)
                        .build(),
                createLongsBlock(1, 2, 3, 1, 2)
        ));

        assertOperatorEquals(actual, expected);
    }

    @Test
    public void testRowNumberArbitrary()
            throws Exception
    {
        Operator source = createOperator(
                new Page(createLongsBlock(1, 3, 5, 7)),
                new Page(createLongsBlock(2, 4, 6, 8)));

        Operator actual = new InMemoryWindowOperator(
                source,
                0,
                ints(0),
                ROW_NUMBER,
                ints(),
                ints(),
                bools(),
                10,
                new TaskMemoryManager(MAX_SIZE));

        Operator expected = createOperator(new Page(
                createLongsBlock(1, 3, 5, 7, 2, 4, 6, 8),
                createLongsBlock(1, 2, 3, 4, 5, 6, 7, 8)));

        assertOperatorEquals(actual, expected);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Task exceeded max memory size of 10B")
    public void testMemoryLimit()
            throws Exception
    {
        Operator source = createOperator(
                new Page(createLongsBlock(1, 2), createDoublesBlock(0.1, 0.2)),
                new Page(createLongsBlock(-1, 4), createDoublesBlock(-0.1, 0.4)));

        Operator operator = new InMemoryWindowOperator(
                source,
                0,
                ints(1),
                ROW_NUMBER,
                ints(),
                ints(0),
                bools(true),
                10,
                new TaskMemoryManager(new DataSize(10, Unit.BYTE)));
        PageIterator iterator = operator.iterator(new OperatorStats());
        iterator.next();
    }

    @Test
    public void testCancel()
            throws Exception
    {
        BlockingOperator blockingOperator = createCancelableDataSource(new TupleInfo(VARIABLE_BINARY), new TupleInfo(VARIABLE_BINARY));
        Operator operator = new InMemoryWindowOperator(blockingOperator, 0, ints(1), ROW_NUMBER, ints(), ints(0), bools(true), 10, new TaskMemoryManager(MAX_SIZE));
        assertCancel(operator, blockingOperator);
    }

    private static int[] ints(int... array)
    {
        return array;
    }

    private static boolean[] bools(boolean... array)
    {
        return array;
    }
}
