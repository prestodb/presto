/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.Ordering;

public class VarBinaryVariableWidthMinAggregation
        implements VariableWidthAggregationFunction<Slice>
{
    public static final VarBinaryVariableWidthMinAggregation VAR_BINARY_MIN = new VarBinaryVariableWidthMinAggregation();
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
    public Slice addInput(BlockCursor cursor, Slice currentMin)
    {
        Slice value = cursor.getSlice(0);
        if (currentMin == null) {
            return value;
        }
        else {
            return Ordering.natural().min(currentMin, value);
        }
    }

    @Override
    public Slice addIntermediate(BlockCursor cursor, Slice currentMin)
    {
        return addInput(cursor, currentMin);
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
}
