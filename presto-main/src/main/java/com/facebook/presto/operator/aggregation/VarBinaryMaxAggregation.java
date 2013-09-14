/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    private Slice addInternal(BlockCursor cursor, int field, Slice currentMax)
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
}
