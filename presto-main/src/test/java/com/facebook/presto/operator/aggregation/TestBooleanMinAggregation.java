package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;

import static com.facebook.presto.operator.aggregation.BooleanMinAggregation.BOOLEAN_MIN;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_BOOLEAN;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

public class TestBooleanMinAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = new BlockBuilder(SINGLE_BOOLEAN);
        for (int i = start; i < start + length; i++) {
            // true, false, true, false...
            blockBuilder.append(i % 2 == 0);
        }
        return blockBuilder.build();
    }

    @Override
    public AggregationFunction getFunction()
    {
        return BOOLEAN_MIN;
    }

    @Override
    public Boolean getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        return length > 1 ? FALSE : TRUE;
    }
}
