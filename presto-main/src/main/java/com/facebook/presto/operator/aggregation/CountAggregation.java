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
import io.airlift.slice.Slice;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;

public class CountAggregation
        implements FixedWidthAggregationFunction
{
    public static final CountAggregation COUNT = new CountAggregation();

    @Override
    public int getFixedSize()
    {
        return SINGLE_LONG.getFixedSize();
    }

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return SINGLE_LONG;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return SINGLE_LONG;
    }

    @Override
    public void initialize(Slice valueSlice, int valueOffset)
    {
    }

    @Override
    public void addInput(int positionCount, Block block, int field, Slice valueSlice, int valueOffset)
    {
        addCount(positionCount, valueSlice, valueOffset);
    }

    @Override
    public void addInput(BlockCursor cursor, int field, Slice valueSlice, int valueOffset)
    {
        addCount(1, valueSlice, valueOffset);
    }

    private void addCount(int positionCount, Slice valueSlice, int valueOffset)
    {
        long currentValue = SINGLE_LONG.getLong(valueSlice, valueOffset, 0);
        SINGLE_LONG.setLong(valueSlice, valueOffset, 0, currentValue + positionCount);
    }

    @Override
    public void addIntermediate(BlockCursor cursor, int field, Slice valueSlice, int valueOffset)
    {
        if (cursor.isNull(0)) {
            return;
        }

        // update current value
        long currentValue = SINGLE_LONG.getLong(valueSlice, valueOffset, 0);
        long newValue = cursor.getLong(0);
        SINGLE_LONG.setLong(valueSlice, valueOffset, 0, currentValue + newValue);
    }

    @Override
    public void evaluateIntermediate(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        evaluateFinal(valueSlice, valueOffset, output);
    }

    @Override
    public void evaluateFinal(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        if (!SINGLE_LONG.isNull(valueSlice, valueOffset, 0)) {
            long currentValue = SINGLE_LONG.getLong(valueSlice, valueOffset, 0);
            output.append(currentValue);
        }
        else {
            output.appendNull();
        }
    }
}
