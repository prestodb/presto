package com.facebook.presto.operator.aggregation;


import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import org.apache.commons.math.stat.descriptive.moment.Variance;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;

public class TestLongVarianceAggregation
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
    public AggregationFunction getFunction()
    {
        return LongVarianceAggregation.VARIANCE_INSTANCE;
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        if (length < 2) {
            return null;
        }

        double [] values = new double [length];
        for (int i = 0; i < length; i++) {
            values[i] = start + i;
        }

        Variance variance = new Variance();
        return variance.evaluate(values);
    }

}
