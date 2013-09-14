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
import com.facebook.presto.block.BlockCursor;
import io.airlift.slice.Slice;

/**
 * Generate the variance for a given set of values. This implements the
 * <a href="http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm">online algorithm</a>.
 */
public class DoubleVarianceAggregation
        extends AbstractVarianceAggregation
{
    public static final DoubleVarianceAggregation VARIANCE_INSTANCE = new DoubleVarianceAggregation(false);
    public static final DoubleVarianceAggregation VARIANCE_POP_INSTANCE = new DoubleVarianceAggregation(true);

    DoubleVarianceAggregation(boolean population)
    {
        super(population);
    }

    @Override
    public void addInput(int positionCount, Block block, int field, Slice valueSlice, int valueOffset)
    {
        boolean hasValue = !VARIANCE_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0);
        long count = hasValue ? VARIANCE_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0) : 0;
        double mean = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 1);
        double m2 = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2);

        BlockCursor cursor = block.cursor();

        while (cursor.advanceNextPosition()) {
            if (cursor.isNull(field)) {
                continue;
            }

            // There is now at least one value present.
            hasValue = true;

            count++;
            double x = cursor.getDouble(field);
            double delta = x - mean;
            mean += (delta / count);
            m2 += (delta * (x - mean));
        }

        if (hasValue) {
            VARIANCE_CONTEXT_INFO.setNotNull(valueSlice, valueOffset, 0);
            VARIANCE_CONTEXT_INFO.setLong(valueSlice, valueOffset, 0, count);
            VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 1, mean);
            VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 2, m2);
        }
    }

    @Override
    public void addInput(BlockCursor cursor, int field, Slice valueSlice, int valueOffset)
    {
        boolean hasValue = !VARIANCE_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0);

        if (cursor.isNull(field)) {
            return;
        }

        long count = hasValue ? VARIANCE_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0) : 0;
        double mean = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 1);
        double m2 = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2);

        count++;
        double x = cursor.getDouble(field);
        double delta = x - mean;
        mean += (delta / count);
        m2 += (delta * (x - mean));

        if (!hasValue) {
            VARIANCE_CONTEXT_INFO.setNotNull(valueSlice, valueOffset, 0);
        }

        VARIANCE_CONTEXT_INFO.setLong(valueSlice, valueOffset, 0, count);
        VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 1, mean);
        VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 2, m2);
    }
}
