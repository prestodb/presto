package com.facebook.presto.operator.aggregation;


import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.Tuple;

public class TestLongSumAggregation
    extends AbstractTestAggregationFunction
{
    @Override
    public BlockCursor getSequenceCursor(int max)
    {
        return new LongSequenceCursor(max);
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
    public Long getActualValue(AggregationFunction function)
    {
        Tuple value = function.evaluate();
        if (value.isNull(0)) {
            return null;
        }
        return value.getLong(0);
    }
}
