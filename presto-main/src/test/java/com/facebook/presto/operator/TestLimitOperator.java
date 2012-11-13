package com.facebook.presto.operator;

import com.facebook.presto.block.BlockAssertions;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;

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
}
