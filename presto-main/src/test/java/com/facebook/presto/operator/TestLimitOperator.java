package com.facebook.presto.operator;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.tuple.TupleInfo;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.CancelTester.assertCancel;
import static com.facebook.presto.operator.CancelTester.createCancelableDataSource;
import static com.facebook.presto.operator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;

public class TestLimitOperator
{
    @Test
    public void testLimitWithPageAlignment()
            throws Exception
    {
        Operator source = createOperator(
                new Page(BlockAssertions.createLongsBlock(1, 2, 3)),
                new Page(BlockAssertions.createLongsBlock(4, 5)),
                new Page(BlockAssertions.createLongsBlock(6, 7))
        );
        LimitOperator actual = new LimitOperator(source, 5);

        Operator expected = createOperator(
                        new Page(BlockAssertions.createLongsBlock(1, 2, 3)),
                        new Page(BlockAssertions.createLongsBlock(4, 5))
                );
        assertOperatorEquals(actual, expected);
    }

    @Test
    public void testLimitWithBlockView()
            throws Exception
    {
        Operator source = createOperator(
                new Page(BlockAssertions.createLongsBlock(1, 2, 3)),
                new Page(BlockAssertions.createLongsBlock(4, 5)),
                new Page(BlockAssertions.createLongsBlock(6, 7))
        );
        LimitOperator actual = new LimitOperator(source, 4);

        Operator expected = createOperator(
                        new Page(BlockAssertions.createLongsBlock(1, 2, 3)),
                        new Page(BlockAssertions.createLongsBlock(4))
                );
        assertOperatorEquals(actual, expected);
    }

    @Test
    public void testCancel()
            throws Exception
    {
        BlockingOperator blockingOperator = createCancelableDataSource(new TupleInfo(VARIABLE_BINARY), new TupleInfo(VARIABLE_BINARY));
        Operator operator = new LimitOperator(blockingOperator, 100_000);
        assertCancel(operator, blockingOperator);
    }
}
