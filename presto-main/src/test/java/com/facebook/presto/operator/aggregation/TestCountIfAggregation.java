package com.facebook.presto.operator.aggregation;


import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;

import static com.facebook.presto.operator.aggregation.CountIfAggregation.COUNT_IF;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_BOOLEAN;

public class TestCountIfAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = new BlockBuilder(SINGLE_BOOLEAN);
        for (int i = start; i < start + length; i++) {
            blockBuilder.append(i % 2 == 0);
        }
        return blockBuilder.build();
    }

    @Override
    public AggregationFunction getFunction()
    {
        return COUNT_IF;
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        long count = 0;
        for (int i = start; i < start + length; i++) {
            if (i % 2 == 0) {
                count++;
            }
        }
        return count;
    }

}
