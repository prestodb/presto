package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;

/**
 * Generate the variance for a given set of values.
 *
 * This implements the online algorithm as described at http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
 *
 * TODO - This code assumes that the values are in offset 0 of the various cursors. Remove this assumption.
 */
public class LongVarianceAggregation
    implements FixedWidthAggregationFunction
{
    public static final LongVarianceAggregation LONG_VARIANCE = new LongVarianceAggregation();

    /** Decribes the tuple used by to calculate the variance. */
    private static final TupleInfo VARIANCE_CONTEXT_INFO =
        new TupleInfo(Type.FIXED_INT_64,  // n
                      Type.DOUBLE,        // mean
                      Type.DOUBLE         // m2
            );

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
    public void initialize(final Slice valueSlice, final int valueOffset)
    {
        // n == null --> No value has been calculated yet.
        VARIANCE_CONTEXT_INFO.setNull(valueSlice, valueOffset, 0);

        VARIANCE_CONTEXT_INFO.setNotNull(valueSlice, valueOffset, 1);
        VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 1, 0);

        VARIANCE_CONTEXT_INFO.setNotNull(valueSlice, valueOffset, 2);
        VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 2, 0);
    }

    @Override
    public void addInput(final int positionCount, final Block block, final Slice valueSlice, final int valueOffset)
    {
        boolean hasValue = !VARIANCE_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0);
        long count = hasValue ? VARIANCE_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0) : 0;
        double mean = hasValue ? VARIANCE_CONTEXT_INFO.getLong(valueSlice, valueOffset, 1) : 0;
        double m2 = hasValue ? VARIANCE_CONTEXT_INFO.getLong(valueSlice, valueOffset, 2) : 0;

        final BlockCursor cursor = block.cursor();

        while (cursor.advanceNextPosition()) {
            if (cursor.isNull(0)) {
                continue;
            }

            // There is now at least one value present.
            hasValue = true;

            count++;
            final long x = cursor.getLong(0);
            final double delta = x - mean;
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
    public void addInput(final BlockCursor cursor, final Slice valueSlice, final int valueOffset)
    {
        boolean hasValue = !VARIANCE_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0);

        if (cursor.isNull(0)) {
            return;
        }

        long count = hasValue ? VARIANCE_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0) : 0;
        double mean = hasValue ? VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 1) : 0;
        double m2 = hasValue ? VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2) : 0;

        count++;
        final long x = cursor.getLong(0);
        final double delta = x - mean;
        mean += (delta / count);
        m2 += (delta * (x - mean));

        if (!hasValue) {
            VARIANCE_CONTEXT_INFO.setNotNull(valueSlice, valueOffset, 0);
        }

        VARIANCE_CONTEXT_INFO.setLong(valueSlice, valueOffset, 0, count);
        VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 1, mean);
        VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 2, m2);
    }

    @Override
    public void evaluateIntermediate(final Slice valueSlice, final int valueOffset, final BlockBuilder output)
    {
        final boolean isEmpty = VARIANCE_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0);
        if (isEmpty) {
            output.appendNull();
            return;
        }

        final long count = VARIANCE_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0);
        final double mean = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 1);
        final double m2 = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2);

        final Slice intermediateValue = Slices.allocate(VARIANCE_CONTEXT_INFO.getFixedSize());
        VARIANCE_CONTEXT_INFO.setNotNull(intermediateValue, 0);
        VARIANCE_CONTEXT_INFO.setLong(intermediateValue, 0, count);
        VARIANCE_CONTEXT_INFO.setDouble(intermediateValue, 1, mean);
        VARIANCE_CONTEXT_INFO.setDouble(intermediateValue, 2, m2);

        output.append(intermediateValue);
    }

    @Override
    public void addIntermediate(final BlockCursor cursor, final Slice valueSlice, final int valueOffset)
    {
        if (cursor.isNull(0)) {
            return;
        }

        final Slice otherVariance = cursor.getSlice(0);
        final long otherCount = VARIANCE_CONTEXT_INFO.getLong(otherVariance, 0);
        final double otherMean = VARIANCE_CONTEXT_INFO.getDouble(otherVariance, 1);
        final double otherM2 = VARIANCE_CONTEXT_INFO.getDouble(otherVariance, 2);

        final long totalCount;
        final double totalMean;
        final double totalM2;

        if (VARIANCE_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0)) {
            totalCount = otherCount;
            totalMean = otherMean;
            totalM2 = otherM2;
        }
        else {
            final long count = VARIANCE_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0);
            final double mean = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 1);
            final double m2 = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2);

            final double delta = otherMean - mean;

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

    @Override
    public void evaluateFinal(final Slice valueSlice, final int valueOffset, final BlockBuilder output)
    {
        if (VARIANCE_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0)) {
            output.appendNull();
            return;
        }

        final long count = VARIANCE_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0);
        final double m2 = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2);

        // if n == 1, then
        //   delta is x;
        //   mean is x;
        //   M2 is delta * (x - mean)
        // therefore M2 is 0.

        if (count == 1) {
            output.append(0.0);
            return;
        }

        output.append(m2 / (count - 1));
        return;
    }
}
