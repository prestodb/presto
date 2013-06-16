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

public class VarBinaryMinAggregation
        implements VariableWidthAggregationFunction<Slice>
{
    public static final VarBinaryMinAggregation VAR_BINARY_MIN = new VarBinaryMinAggregation();

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
    public Slice addInput(int positionCount, Block[] blocks, int[] fields, Slice currentMax)
    {
        BlockCursor cursor = blocks[0].cursor();

        while (cursor.advanceNextPosition()) {
            currentMax = addInternal(cursor, fields[0], currentMax);
        }

        return currentMax;
    }

    @Override
    public Slice addInput(BlockCursor[] cursors, int[] fields, Slice currentMax)
    {
        return addInternal(cursors[0], fields[0], currentMax);
    }

    @Override
    public Slice addIntermediate(BlockCursor[] cursors, int[] fields, Slice currentMax)
    {
        return addInternal(cursors[0], fields[0], currentMax);
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

    private Slice addInternal(BlockCursor cursor, int field, Slice currentMin)
    {
        if (cursor.isNull(field)) {
            return currentMin;
        }

        Slice value = cursor.getSlice(field);
        if (currentMin == null) {
            return value;
        }
        else {
            return Ordering.natural().min(currentMin, value);
        }
    }
}
