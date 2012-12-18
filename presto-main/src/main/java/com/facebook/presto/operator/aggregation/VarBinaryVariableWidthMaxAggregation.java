/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.Ordering;

public class VarBinaryVariableWidthMaxAggregation
        implements VariableWidthAggregationFunction<Slice>
{
    public static final VarBinaryVariableWidthMaxAggregation VAR_BINARY_MAX = new VarBinaryVariableWidthMaxAggregation();
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
    public Slice addInput(BlockCursor cursor, Slice currentMax)
    {
        Slice value = cursor.getSlice(0);
        if (currentMax == null) {
            return value;
        }
        else {
            return Ordering.natural().max(currentMax, value);
        }
    }

    @Override
    public Slice addIntermediate(BlockCursor cursor, Slice currentMax)
    {
        return addInput(cursor, currentMax);
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
