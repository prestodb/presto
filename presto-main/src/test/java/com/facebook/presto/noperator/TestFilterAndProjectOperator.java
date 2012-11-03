package com.facebook.presto.noperator;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.BlockAssertions;
import com.facebook.presto.nblock.BlockBuilder;
import com.facebook.presto.nblock.BlockCursor;
import org.testng.annotations.Test;

import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.noperator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.noperator.OperatorAssertions.createOperator;
import static com.facebook.presto.noperator.ProjectionFunctions.singleColumn;

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
            public boolean filter(BlockCursor[] cursors)
            {
                long value = cursors[1].getLong(0);
                return 10 <= value && value < 20;
            }
        }, ProjectionFunctions.concat(singleColumn(VARIABLE_BINARY, 0, 0), singleColumn(FIXED_INT_64, 1, 0)));

        Operator expected = createOperator(new Page(new BlockBuilder(0, new TupleInfo(VARIABLE_BINARY, FIXED_INT_64))
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
}
