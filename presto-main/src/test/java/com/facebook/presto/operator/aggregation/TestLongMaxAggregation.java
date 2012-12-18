package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.Tuple;

import static com.facebook.presto.operator.aggregation.LongMaxFixedWidthAggregation.LONG_MAX;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;

public class TestLongMaxAggregation
    extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = new BlockBuilder(SINGLE_LONG);
        for (int i = start; i < start + length; i++) {
            blockBuilder.append(i);
        }
        return blockBuilder.build();
    }

    @Override
    public NewAggregationFunction getFunction()
    {
        return LONG_MAX;
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        return (long) start + length - 1;
    }

    @Override
    public Long getActualValue(AggregationFunctionStep function)
    {
        Tuple value = function.evaluate();
        if (value.isNull(0)) {
            return null;
        }
        return value.getLong(0);
    }
}
