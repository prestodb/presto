package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static java.lang.Float.floatToRawIntBits;

public class TestRealSumIfAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder conditions = BOOLEAN.createBlockBuilder(null, length);
        BlockBuilder values = REAL.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            BOOLEAN.writeBoolean(conditions, i % 2 == 0);
            REAL.writeLong(values,  floatToRawIntBits((float) i));
        }
        return new Block[] {conditions.build(), values.build()};
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        float sum = 0;
        for (int i = start; i < start + length; i++) {
            if (i % 2 == 0) {
                sum += i;
            }
        }
        return sum;
    }

    @Override
    protected String getFunctionName()
    {
        return "sum_if";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.BOOLEAN, StandardTypes.REAL);
    }

}
