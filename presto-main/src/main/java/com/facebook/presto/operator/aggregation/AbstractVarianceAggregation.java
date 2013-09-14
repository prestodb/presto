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

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;

/**
 * Generate the variance for a given set of values. This implements the
 * <a href="http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm">online algorithm</a>.
 */
public abstract class AbstractVarianceAggregation
        implements FixedWidthAggregationFunction
{
    protected final boolean population;

    AbstractVarianceAggregation(boolean population)
    {
        this.population = population;
    }

    /**
     * Describes the tuple used by to calculate the variance.
     */
    static final TupleInfo VARIANCE_CONTEXT_INFO = new TupleInfo(
            Type.FIXED_INT_64,  // n
            Type.DOUBLE,        // mean
            Type.DOUBLE);       // m2

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return SINGLE_DOUBLE;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        // This should be the tuple info from above but the engine
        // currently does not support that. Fake up a varbinary tuple.
        // TODO - this should be fixed once the engine supports tuple returns.
        return SINGLE_VARBINARY;
    }

    @Override
    public int getFixedSize()
    {
        return VARIANCE_CONTEXT_INFO.getFixedSize();
    }

    @Override
    public void initialize(Slice valueSlice, int valueOffset)
    {
        // n == null --> No value has been calculated yet.
        VARIANCE_CONTEXT_INFO.setNull(valueSlice, valueOffset, 0);

        VARIANCE_CONTEXT_INFO.setNotNull(valueSlice, valueOffset, 1);
        VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 1, 0);

        VARIANCE_CONTEXT_INFO.setNotNull(valueSlice, valueOffset, 2);
        VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 2, 0);
    }

    @Override
    public void evaluateIntermediate(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        boolean isEmpty = VARIANCE_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0);
        if (isEmpty) {
            output.appendNull();
            return;
        }

        long count = VARIANCE_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0);
        double mean = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 1);
        double m2 = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2);

        Slice intermediateValue = Slices.allocate(VARIANCE_CONTEXT_INFO.getFixedSize());
        VARIANCE_CONTEXT_INFO.setNotNull(intermediateValue, 0);
        VARIANCE_CONTEXT_INFO.setLong(intermediateValue, 0, count);
        VARIANCE_CONTEXT_INFO.setDouble(intermediateValue, 1, mean);
        VARIANCE_CONTEXT_INFO.setDouble(intermediateValue, 2, m2);

        output.append(intermediateValue);
    }

    @Override
    public void addIntermediate(BlockCursor cursor, int field, Slice valueSlice, int valueOffset)
    {
        if (cursor.isNull(field)) {
            return;
        }

        Slice otherVariance = cursor.getSlice(field);
        long otherCount = VARIANCE_CONTEXT_INFO.getLong(otherVariance, 0);
        double otherMean = VARIANCE_CONTEXT_INFO.getDouble(otherVariance, 1);
        double otherM2 = VARIANCE_CONTEXT_INFO.getDouble(otherVariance, 2);

        long totalCount;
        double totalMean;
        double totalM2;

        if (VARIANCE_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0)) {
            totalCount = otherCount;
            totalMean = otherMean;
            totalM2 = otherM2;
        }
        else {
            long count = VARIANCE_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0);
            double mean = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 1);
            double m2 = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2);

            double delta = otherMean - mean;

            totalCount = count + otherCount;

            // Use numerically stable variant
            totalMean = ((count * mean) + (otherCount * otherMean)) / totalCount;
            totalM2 = m2 + otherM2 + ((delta * delta) * (count * otherCount)) / totalCount;
        }

        VARIANCE_CONTEXT_INFO.setNotNull(valueSlice, valueOffset, 0);
        VARIANCE_CONTEXT_INFO.setLong(valueSlice, valueOffset, 0, totalCount);
        VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 1, totalMean);
        VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 2, totalM2);
    }

    static Double buildFinalVariance(boolean population, Slice valueSlice, int valueOffset)
    {
        if (VARIANCE_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0)) {
            return null;
        }

        long count = VARIANCE_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0);
        double m2 = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2);

        if (population) {
            return m2 / count;
        }
        else {
            return (count == 1) ? null : (m2 / (count - 1));
        }
    }

    static Double buildFinalStdDev(boolean population, Slice valueSlice, int valueOffset)
    {
        Double variance = buildFinalVariance(population, valueSlice, valueOffset);
        return (variance == null) ? null : Math.sqrt(variance);
    }

    @Override
    public void evaluateFinal(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        Double result = buildFinalVariance(population, valueSlice, valueOffset);

        if (result == null) {
            output.appendNull();
        }
        else {
            output.append(result);
        }
    }
}
