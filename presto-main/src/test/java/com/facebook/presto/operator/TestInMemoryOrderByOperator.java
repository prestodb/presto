package com.facebook.presto.operator;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.tuple.TupleInfo;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.CancelTester.assertCancel;
import static com.facebook.presto.operator.CancelTester.createCancelableDataSource;
import static com.facebook.presto.operator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;

public class TestInMemoryOrderByOperator
{
    @Test
    public void testSingleFieldKey()
            throws Exception
    {
        Operator source = createOperator(
                new Page(
                        BlockAssertions.createLongsBlock(1, 2),
                        BlockAssertions.createDoublesBlock(0.1, 0.2)
                ),
                new Page(
                        BlockAssertions.createLongsBlock(-1, 4),
                        BlockAssertions.createDoublesBlock(-0.1, 0.4)
                )
        );

        InMemoryOrderByOperator actual = new InMemoryOrderByOperator(source, 0, new int[]{1}, 10, new TaskMemoryManager(new DataSize(1, Unit.MEGABYTE)));

        Operator expected = createOperator(new Page(BlockAssertions.createDoublesBlock(-0.1, 0.1, 0.2, 0.4)));
        assertOperatorEquals(actual, expected);
    }

    @Test
    public void testMultiFieldKey()
            throws Exception
    {
        TupleInfo tupleInfo = new TupleInfo(VARIABLE_BINARY, FIXED_INT_64);
        Operator source = createOperator(
                new Page(
                        new BlockBuilder(tupleInfo)
                                .append("a").append(1)
                                .append("b").append(2)
                                .build()
                ),
                new Page(
                        new BlockBuilder(tupleInfo)
                                .append("b").append(3)
                                .append("a").append(4)
                                .build()
                )
        );

        InMemoryOrderByOperator actual = new InMemoryOrderByOperator(source, 0, new int[]{0}, 10, new TaskMemoryManager(new DataSize(1, Unit.MEGABYTE)));

        Operator expected = createOperator(
                new Page(
                        new BlockBuilder(tupleInfo)
                                .append("a").append(1)
                                .append("a").append(4)
                                .append("b").append(2)
                                .append("b").append(3)
                                .build()
                )
        );
        assertOperatorEquals(actual, expected);
    }

    @Test
    public void testReverseOrder()
            throws Exception
    {
        Operator source = createOperator(
                new Page(
                        BlockAssertions.createLongsBlock(1, 2),
                        BlockAssertions.createDoublesBlock(0.1, 0.2)
                ),
                new Page(
                        BlockAssertions.createLongsBlock(-1, 4),
                        BlockAssertions.createDoublesBlock(-0.1, 0.4)
                )
        );

        InMemoryOrderByOperator actual = new InMemoryOrderByOperator(
                source,
                0,
                new int[]{0},
                10,
                new int[]{0},
                new boolean[]{false},
                new TaskMemoryManager(new DataSize(1, Unit.MEGABYTE)));

        Operator expected = createOperator(
                new Page(
                        BlockAssertions.createLongsBlock(4, 2, 1, -1)
                )
        );
        assertOperatorEquals(actual, expected);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Task exceeded max memory size of 10B")
    public void testMemoryLimit()
            throws Exception
    {
        Operator source = createOperator(
                new Page(
                        BlockAssertions.createLongsBlock(1, 2),
                        BlockAssertions.createDoublesBlock(0.1, 0.2)
                ),
                new Page(
                        BlockAssertions.createLongsBlock(-1, 4),
                        BlockAssertions.createDoublesBlock(-0.1, 0.4)
                )
        );

        InMemoryOrderByOperator operator = new InMemoryOrderByOperator(source, 0, new int[]{1}, 10, new TaskMemoryManager(new DataSize(10, Unit.BYTE)));
        PageIterator iterator = operator.iterator(new OperatorStats());
        iterator.next();
    }

    @Test
    public void testCancel()
            throws Exception
    {
        BlockingOperator blockingOperator = createCancelableDataSource(new TupleInfo(VARIABLE_BINARY), new TupleInfo(VARIABLE_BINARY));
        Operator operator = new InMemoryOrderByOperator(blockingOperator, 0, new int[]{1}, 10, new TaskMemoryManager(new DataSize(10, Unit.MEGABYTE)));
        assertCancel(operator, blockingOperator);
    }
}
