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

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;

public class DoubleMinAggregation
        implements FixedWidthAggregationFunction
{
    public static final DoubleMinAggregation DOUBLE_MIN = new DoubleMinAggregation();

    @Override
    public int getFixedSize()
    {
        return SINGLE_DOUBLE.getFixedSize();
    }

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return SINGLE_DOUBLE;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return SINGLE_DOUBLE;
    }

    @Override
    public void initialize(Slice valueSlice, int valueOffset)
    {
        // mark value null
        SINGLE_DOUBLE.setNull(valueSlice, valueOffset, 0);
        SINGLE_DOUBLE.setDouble(valueSlice, valueOffset, 0, Double.POSITIVE_INFINITY);
    }

    @Override
    public void addInput(BlockCursor cursor, int field, Slice valueSlice, int valueOffset)
    {
        if (cursor.isNull(field)) {
            return;
        }

        // mark value not null
        SINGLE_DOUBLE.setNotNull(valueSlice, valueOffset, 0);

        // update current value
        double currentValue = SINGLE_DOUBLE.getDouble(valueSlice, valueOffset, 0);
        double newValue = cursor.getDouble(field);
        SINGLE_DOUBLE.setDouble(valueSlice, valueOffset, 0, Math.min(currentValue, newValue));
    }

    @Override
    public void addInput(int positionCount, Block block, int field, Slice valueSlice, int valueOffset)
    {
        // initialize
        boolean hasNonNull = !SINGLE_DOUBLE.isNull(valueSlice, valueOffset);
        double min = SINGLE_DOUBLE.getDouble(valueSlice, valueOffset, 0);

        // process block
        BlockCursor cursor = block.cursor();
        while (cursor.advanceNextPosition()) {
            if (!cursor.isNull(field)) {
                hasNonNull = true;
                min = Math.min(min, cursor.getDouble(field));
            }
        }

        // write new value
        if (hasNonNull) {
            SINGLE_DOUBLE.setNotNull(valueSlice, valueOffset, 0);
            SINGLE_DOUBLE.setDouble(valueSlice, valueOffset, 0, min);
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
        if (!SINGLE_DOUBLE.isNull(valueSlice, valueOffset, 0)) {
            double currentValue = SINGLE_DOUBLE.getDouble(valueSlice, valueOffset, 0);
            output.append(currentValue);
        }
        else {
            output.appendNull();
        }
    }
}
