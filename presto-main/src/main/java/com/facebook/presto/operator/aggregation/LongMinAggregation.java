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

public class LongMinAggregation
        implements FixedWidthAggregationFunction
{
    public static final LongMinAggregation LONG_MIN = new LongMinAggregation();

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
        // mark value null
        SINGLE_LONG.setNull(valueSlice, valueOffset, 0);
        SINGLE_LONG.setLong(valueSlice, valueOffset, 0, Long.MAX_VALUE);
    }

    @Override
    public void addInput(BlockCursor cursor, int field, Slice valueSlice, int valueOffset)
    {
        if (cursor.isNull(field)) {
            return;
        }

        // mark value not null
        SINGLE_LONG.setNotNull(valueSlice, valueOffset, 0);

        // update current value
        long currentValue = SINGLE_LONG.getLong(valueSlice, valueOffset, 0);
        long newValue = cursor.getLong(field);
        SINGLE_LONG.setLong(valueSlice, valueOffset, 0, Math.min(currentValue, newValue));
    }

    @Override
    public void addInput(int positionCount, Block block, int field, Slice valueSlice, int valueOffset)
    {
        // initialize
        boolean hasNonNull = !SINGLE_LONG.isNull(valueSlice, valueOffset);
        long min = SINGLE_LONG.getLong(valueSlice, valueOffset, 0);

        // process block
        BlockCursor cursor = block.cursor();
        while (cursor.advanceNextPosition()) {
            if (!cursor.isNull(field)) {
                hasNonNull = true;
                min = Math.min(min, cursor.getLong(field));
            }
        }

        // write new value
        if (hasNonNull) {
            SINGLE_LONG.setNotNull(valueSlice, valueOffset, 0);
            SINGLE_LONG.setLong(valueSlice, valueOffset, 0, min);
        }
    }

    @Override
    public void addIntermediate(BlockCursor cursor, int field, Slice valueSlice, int valueOffset)
    {
        addInput(cursor, field, valueSlice, valueOffset);
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
