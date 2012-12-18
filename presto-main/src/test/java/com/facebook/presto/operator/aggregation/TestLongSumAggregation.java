package com.facebook.presto.operator.aggregation;


import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.Tuple;

import static com.facebook.presto.operator.aggregation.LongSumFixedWidthAggregation.LONG_SUM;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;

public class TestLongSumAggregation
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
        return LONG_SUM;
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        long sum = 0;
        for (int i = start; i < start + length; i++) {
            sum += i;
        }
        return sum;
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
