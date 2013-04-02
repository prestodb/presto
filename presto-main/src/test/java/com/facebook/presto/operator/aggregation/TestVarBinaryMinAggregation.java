package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.google.common.base.Charsets;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.operator.aggregation.VarBinaryMinAggregation.VAR_BINARY_MIN;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;

public class TestVarBinaryMinAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = new BlockBuilder(SINGLE_VARBINARY);
        for (int i = 0; i < length; i++) {
            blockBuilder.append(Slices.wrappedBuffer(Ints.toByteArray(i)));
        }
        return blockBuilder.build();
    }

    @Override
    public AggregationFunction getFunction()
    {
        return VAR_BINARY_MIN;
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        Slice min = null;
        for (int i = 0; i < length; i++) {
            Slice slice = Slices.wrappedBuffer(Ints.toByteArray(i));
            min = (min == null) ? slice : Ordering.natural().min(min, slice);
        }
        return min.toString(Charsets.UTF_8);
    }

}
