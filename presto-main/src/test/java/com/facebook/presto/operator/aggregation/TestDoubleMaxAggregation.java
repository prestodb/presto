package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.Tuple;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;

public class TestDoubleMaxAggregation
    extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int positions)
    {
        BlockBuilder blockBuilder = new BlockBuilder(SINGLE_DOUBLE);
        for (int i = 0; i < positions; i++) {
            blockBuilder.append((double) i);
        }
        return blockBuilder.build();
    }

    @Override
    public DoubleMaxAggregation getFunction()
    {
        return new DoubleMaxAggregation(0, 0);
    }

    @Override
    public Double getExpectedValue(int positions)
    {
        if (positions == 0) {
            return null;
        }
        return (double) (positions - 1);
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
