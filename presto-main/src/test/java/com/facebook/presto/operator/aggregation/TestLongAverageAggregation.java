package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.Tuple;

public class TestLongAverageAggregation
    extends AbstractTestAggregationFunction
{
    @Override
    public BlockCursor getSequenceCursor(long max)
    {
        return new LongSequenceCursor(max);
    }

    @Override
    public LongAverageAggregation getFunction()
    {
        return new LongAverageAggregation(0, 0);
    }

    @Override
    public Double getExpectedValue(long positions)
    {
        if (positions == 0) {
            return null;
        }

        double sum = 0;
        for (long i = 0; i < positions; i++) {
            sum += i;
        }
        return sum / positions;
    }

    @Override
    public Double getActualValue(AggregationFunction function)
    {
        Tuple value = function.evaluate();
        if (value.isNull(0)) {
            return null;
        }
        return value.getDouble(0);
    }
}
