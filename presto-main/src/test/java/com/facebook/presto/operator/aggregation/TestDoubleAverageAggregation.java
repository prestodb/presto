package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.Tuple;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;

public class TestDoubleAverageAggregation
    extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int max)
    {
        BlockBuilder blockBuilder = new BlockBuilder(SINGLE_DOUBLE);
        for (int i = 0; i < max; i++) {
            blockBuilder.append((double) i);
        }
        return blockBuilder.build();
    }

    @Override
    public DoubleAverageAggregation getFunction()
    {
        return new DoubleAverageAggregation(0, 0);
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
        return sum / positions;
    }

    @Override
    public Double getActualValue(AggregationFunctionStep function)
    {
        Tuple value = function.evaluate();
        if (value.isNull(0)) {
            return null;
        }
        return value.getDouble(0);
    }
}
