/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.Ordering;
import io.airlift.slice.Slice;

public class VarBinaryMaxAggregation
        implements VariableWidthAggregationFunction<Slice>
{
    public static final VarBinaryMaxAggregation VAR_BINARY_MAX = new VarBinaryMaxAggregation();

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return TupleInfo.SINGLE_VARBINARY;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return TupleInfo.SINGLE_VARBINARY;
    }

    @Override
    public Slice initialize()
    {
        return null;
    }

    @Override
    public Slice addInput(int positionCount, Block block, int field, Slice currentMax)
    {
        BlockCursor cursor = block.cursor();
        while (cursor.advanceNextPosition()) {
            currentMax = addInput(cursor, field, currentMax);
        }
        return currentMax;
    }

    @Override
    public Slice addInput(BlockCursor cursor, int field, Slice currentMax)
    {
        if (cursor.isNull(field)) {
            return currentMax;
        }

        Slice value = cursor.getSlice(field);
        if (currentMax == null) {
            return value;
        }
        else {
            return Ordering.natural().max(currentMax, value);
        }
    }

    @Override
    public Slice addIntermediate(BlockCursor cursor, int field, Slice currentMax)
    {
        return addInput(cursor, field, currentMax);
    }

    @Override
    public void evaluateIntermediate(Slice currentValue, BlockBuilder output)
    {
        evaluateFinal(currentValue, output);
    }

    @Override
    public void evaluateFinal(Slice currentValue, BlockBuilder output)
    {
        if (currentValue != null) {
            output.append(currentValue);
        }
        else {
            output.appendNull();
        }
    }

    @Override
    public long estimateSizeInBytes(Slice value)
    {
        return value.length();
    }
}
