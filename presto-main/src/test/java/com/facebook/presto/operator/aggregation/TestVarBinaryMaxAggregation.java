package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tuple.Tuple;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;

public class TestVarBinaryMaxAggregation
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
    public VarBinaryMaxAggregation getFunction()
    {
        return new VarBinaryMaxAggregation(0, 0);
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        Slice max = null;
        for (int i = 0; i < length; i++) {
            Slice slice = Slices.wrappedBuffer(Ints.toByteArray(i));
            max = (max == null) ? slice : Ordering.natural().max(max, slice);
        }
        return max;
    }

    @Override
    public Object getActualValue(AggregationFunctionStep function)
    {
        Tuple value = function.evaluate();
        if (value.isNull(0)) {
            return null;
        }
        return value.getSlice(0);
    }
}
