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
import com.google.common.base.Preconditions;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import io.airlift.stats.QuantileDigest;

public class DoubleApproximatePercentileWeightedAggregation
        implements VariableWidthAggregationFunction<DoubleApproximatePercentileWeightedAggregation.DigestAndPercentile>
{
    public static final DoubleApproximatePercentileWeightedAggregation INSTANCE = new DoubleApproximatePercentileWeightedAggregation();

    private static final int VALUE_INDEX = 0;
    private static final int WEIGHT_INDEX = 1;
    private static final int PERCENTILE_INDEX = 2;

    @Override
    public DigestAndPercentile initialize()
    {
        return new DigestAndPercentile(new QuantileDigest(0.01));
    }

    @Override
    public DigestAndPercentile addInput(int positionCount, Block[] blocks, int[] fields, DigestAndPercentile currentValue)
    {
        BlockCursor valueCursor = blocks[VALUE_INDEX].cursor();
        BlockCursor weightCursor = blocks[WEIGHT_INDEX].cursor();
        int valueField = fields[VALUE_INDEX];
        int weightField = fields[WEIGHT_INDEX];

        while (valueCursor.advanceNextPosition() && weightCursor.advanceNextPosition()) {
            if (!valueCursor.isNull(valueField) && !weightCursor.isNull(weightField)) {
                double value = valueCursor.getDouble(valueField);
                long weight = weightCursor.getLong(weightField);
                currentValue.digest.add(doubleToSortableLong(value), weight);
            }
        }

        BlockCursor percentileCursor = blocks[PERCENTILE_INDEX].cursor();
        int percentileField = fields[PERCENTILE_INDEX];
        if (percentileCursor.advanceNextPosition()) {
            if (!percentileCursor.isNull(percentileField)) {
                currentValue.percentile = percentileCursor.getDouble(percentileField);
            }
        }

        return currentValue;
    }

    @Override
    public DigestAndPercentile addInput(BlockCursor[] cursors, int[] fields, DigestAndPercentile currentValue)
    {
        if (!cursors[VALUE_INDEX].isNull(fields[VALUE_INDEX]) && !cursors[WEIGHT_INDEX].isNull(fields[WEIGHT_INDEX])) {
            double value = cursors[0].getDouble(fields[0]);
            long weight = cursors[WEIGHT_INDEX].getLong(fields[WEIGHT_INDEX]);
            currentValue.digest.add(doubleToSortableLong(value), weight);
        }

        if (!cursors[PERCENTILE_INDEX].isNull(fields[PERCENTILE_INDEX])) {
            currentValue.percentile = cursors[PERCENTILE_INDEX].getDouble(fields[PERCENTILE_INDEX]);
        }

        return currentValue;
    }

    @Override
    public DigestAndPercentile addIntermediate(BlockCursor[] cursors, int[] fields, DigestAndPercentile currentValue)
    {
        if (!cursors[0].isNull(fields[0])) {
            SliceInput input = cursors[0].getSlice(fields[0]).getInput();

            currentValue.digest.merge(QuantileDigest.deserialize(input));
            currentValue.percentile = input.readDouble();
        }

        return currentValue;
    }

    @Override
    public void evaluateIntermediate(DigestAndPercentile currentValue, BlockBuilder output)
    {
        if (currentValue.digest.getCount() == 0.0) {
            output.appendNull();
        }
        else {
            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(currentValue.digest.estimatedSerializedSizeInBytes());
            currentValue.digest.serialize(sliceOutput);
            sliceOutput.appendDouble(currentValue.percentile);

            output.append(sliceOutput.slice());
        }
    }

    @Override
    public void evaluateFinal(DigestAndPercentile currentValue, BlockBuilder output)
    {
        if (currentValue.digest.getCount() == 0.0) {
            output.appendNull();
        }
        else {
            Preconditions.checkState(currentValue.percentile != -1, "Percentile is missing");
            output.append(longToDouble(currentValue.digest.getQuantile(currentValue.percentile)));
        }
    }

    @Override
    public long estimateSizeInBytes(DigestAndPercentile value)
    {
        // TODO: account for DigestAndPercentile object
        return value.digest.estimatedInMemorySizeInBytes();
    }

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return TupleInfo.SINGLE_DOUBLE;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return TupleInfo.SINGLE_VARBINARY;
    }

    private static double longToDouble(long value)
    {
        if (value < 0) {
            value ^= 0x7fffffffffffffffL;
        }

        return Double.longBitsToDouble(value);
    }

    private static long doubleToSortableLong(double value)
    {
        long result = Double.doubleToRawLongBits(value);

        if (result < 0) {
            result ^= 0x7fffffffffffffffL;
        }

        return result;
    }

    public final static class DigestAndPercentile
    {
        private QuantileDigest digest;
        private double percentile = -1;

        public DigestAndPercentile(QuantileDigest digest)
        {
            this.digest = digest;
        }
    }
}
