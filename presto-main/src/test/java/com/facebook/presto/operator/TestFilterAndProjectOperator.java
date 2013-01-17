package com.facebook.presto.operator;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.CancelTester.assertCancel;
import static com.facebook.presto.operator.CancelTester.createCancelableDataSource;
import static com.facebook.presto.operator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;

public class TestFilterAndProjectOperator
{
    @Test
    public void testAlignment()
            throws Exception
    {
        Operator source = createOperator(new Page(
                BlockAssertions.createStringSequenceBlock(0, 100),
                BlockAssertions.createLongSequenceBlock(0, 100)));

        FilterAndProjectOperator actual = new FilterAndProjectOperator(source, new FilterFunction()
        {
            @Override
            public boolean filter(TupleReadable... cursors)
            {
                long value = cursors[1].getLong(0);
                return 10 <= value && value < 20;
            }
        }, ProjectionFunctions.concat(singleColumn(VARIABLE_BINARY, 0, 0), singleColumn(FIXED_INT_64, 1, 0)));

        Operator expected = createOperator(new Page(new BlockBuilder(new TupleInfo(VARIABLE_BINARY, FIXED_INT_64))
                .append("10").append(10)
                .append("11").append(11)
                .append("12").append(12)
                .append("13").append(13)
                .append("14").append(14)
                .append("15").append(15)
                .append("16").append(16)
                .append("17").append(17)
                .append("18").append(18)
                .append("19").append(19)
                .build()));

        assertOperatorEquals(actual, expected);
    }

    @Test
    public void testCancel()
            throws Exception
    {
        BlockingOperator blockingOperator = createCancelableDataSource(new TupleInfo(VARIABLE_BINARY), new TupleInfo(VARIABLE_BINARY));
        Operator operator = new FilterAndProjectOperator(blockingOperator, FilterFunctions.TRUE_FUNCTION, ProjectionFunctions.singleColumn(VARIABLE_BINARY, 0, 0));
        assertCancel(operator, blockingOperator);
    }
}
