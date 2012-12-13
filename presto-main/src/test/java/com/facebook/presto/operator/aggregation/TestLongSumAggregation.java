package com.facebook.presto.operator.aggregation;


import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.Tuple;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;

public class TestLongSumAggregation
    extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int positions)
    {
        BlockBuilder blockBuilder = new BlockBuilder(SINGLE_LONG);
        for (int i = 0; i < positions; i++) {
            blockBuilder.append(i);
        }
        return blockBuilder.build();
    }

    @Override
    public LongSumAggregation getFunction()
    {
        return new LongSumAggregation(0, 0);
    }

    @Override
    public Long getExpectedValue(int positions)
    {
        if (positions == 0) {
            return null;
        }

        long sum = 0;
        for (int i = 0; i < positions; i++) {
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
