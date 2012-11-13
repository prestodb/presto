package com.facebook.presto.operator.aggregation;


import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.Tuple;

public class TestDoubleSumAggregation
    extends AbstractTestAggregationFunction
{
    @Override
    public BlockCursor getSequenceCursor(int max)
    {
        return new DoubleSequenceCursor(max);
    }

    @Override
    public DoubleSumAggregation getFunction()
    {
        return new DoubleSumAggregation(0, 0);
    }

    @Override
    public Double getExpectedValue(int positions)
    {
        if (positions == 0) {
            return null;
        }

        double sum = 0;
        for (int i = 0; i < positions; i++) {
            sum += i;
        }
        return sum;
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
