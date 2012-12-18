package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;

import static com.facebook.presto.operator.aggregation.DoubleMinFixedWidthAggregation.DOUBLE_MIN;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;

public class TestDoubleMinAggregation
    extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = new BlockBuilder(SINGLE_DOUBLE);
        for (int i = start; i < start + length; i++) {
            blockBuilder.append((double) i);
        }
        return blockBuilder.build();
    }

    @Override
    public NewAggregationFunction getFunction()
    {
        return DOUBLE_MIN;
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        return (double) start;
    }

}
