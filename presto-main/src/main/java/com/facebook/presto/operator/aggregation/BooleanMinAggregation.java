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

import static com.facebook.presto.tuple.TupleInfo.SINGLE_BOOLEAN;

public class BooleanMinAggregation
        implements FixedWidthAggregationFunction
{
    public static final BooleanMinAggregation BOOLEAN_MIN = new BooleanMinAggregation();

    @Override
    public int getFixedSize()
    {
        return SINGLE_BOOLEAN.getFixedSize();
    }

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return SINGLE_BOOLEAN;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return SINGLE_BOOLEAN;
    }

    @Override
    public void initialize(Slice valueSlice, int valueOffset)
    {
        // mark value null
        SINGLE_BOOLEAN.setNull(valueSlice, valueOffset, 0);
        SINGLE_BOOLEAN.setBoolean(valueSlice, valueOffset, 0, Boolean.TRUE);
    }

    @Override
    public void addInput(BlockCursor cursor, int field, Slice valueSlice, int valueOffset)
    {
        if (cursor.isNull(field)) {
            return;
        }

        // mark value not null
        SINGLE_BOOLEAN.setNotNull(valueSlice, valueOffset, 0);

        // update current value
        boolean newValue = cursor.getBoolean(field);
        if (newValue == false) {
            SINGLE_BOOLEAN.setBoolean(valueSlice, valueOffset, 0, false);
        }
    }

    @Override
    public void addInput(int positionCount, Block block, int field, Slice valueSlice, int valueOffset)
    {
        // initialize
        boolean hasNonNull = !SINGLE_BOOLEAN.isNull(valueSlice, valueOffset);

        // if we already have FALSE for the min value, don't process the block
        if (hasNonNull && SINGLE_BOOLEAN.getBoolean(valueSlice, valueOffset, field) == false) {
            return;
        }

        boolean min = true;

        // process block
        BlockCursor cursor = block.cursor();
        while (cursor.advanceNextPosition()) {
            if (!cursor.isNull(field)) {
                hasNonNull = true;
                if (cursor.getBoolean(field) == false) {
                    min = false;
                    break;
                }
            }
        }

        // write new value
        if (hasNonNull) {
            SINGLE_BOOLEAN.setNotNull(valueSlice, valueOffset, 0);
            SINGLE_BOOLEAN.setBoolean(valueSlice, valueOffset, 0, min);
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
        if (!SINGLE_BOOLEAN.isNull(valueSlice, valueOffset, 0)) {
            boolean currentValue = SINGLE_BOOLEAN.getBoolean(valueSlice, valueOffset, 0);
            output.append(currentValue);
        }
        else {
            output.appendNull();
        }
    }
}
