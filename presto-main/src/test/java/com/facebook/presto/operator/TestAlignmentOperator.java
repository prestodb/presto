package com.facebook.presto.operator;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockIterable;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;

public class TestAlignmentOperator
{
    @Test
    public void testAlignment()
            throws Exception
    {
        BlockIterable channel0 = BlockAssertions.blockIterableBuilder(VARIABLE_BINARY)
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .newBlock()
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .newBlock()
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .build();

        BlockIterable channel1 = BlockAssertions.blockIterableBuilder(FIXED_INT_64)
                .append(0)
                .append(1)
                .append(2)
                .append(3)
                .append(4)
                .append(5)
                .append(6)
                .append(7)
                .append(8)
                .append(9)
                .append(10)
                .append(11)
                .build();

        AlignmentOperator operator = new AlignmentOperator(channel0, channel1);
        assertOperatorEquals(operator, createOperator(
                new Page(createStringsBlock("alice", "bob", "charlie", "dave"),
                        createLongsBlock(0, 1, 2, 3)),
                new Page(createStringsBlock("alice", "bob", "charlie", "dave"),
                        createLongsBlock(4, 5, 6, 7)),
                new Page(createStringsBlock("alice", "bob", "charlie", "dave"),
                        createLongsBlock(8, 9, 10, 11))
        ));
    }
}
