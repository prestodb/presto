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

public class LongApproximatePercentileAggregation
        implements VariableWidthAggregationFunction<LongApproximatePercentileAggregation.DigestAndPercentile>
{
    public static final LongApproximatePercentileAggregation INSTANCE = new LongApproximatePercentileAggregation();

    @Override
    public DigestAndPercentile initialize()
    {
        return new DigestAndPercentile(new QuantileDigest(0.01));
    }

    @Override
    public DigestAndPercentile addInput(int positionCount, Block[] blocks, int[] fields, DigestAndPercentile currentValue)
    {
        BlockCursor valueCursor = blocks[0].cursor();
        int valueField = fields[0];
        while (valueCursor.advanceNextPosition()) {
            if (!valueCursor.isNull(valueField)) {
                currentValue.digest.add(valueCursor.getLong(valueField));
            }
        }

        BlockCursor percentileCursor = blocks[1].cursor();
        int percentileField = fields[1];
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
        if (!cursors[0].isNull(fields[0])) {
            currentValue.digest.add(cursors[0].getLong(fields[0]));
        }

        if (!cursors[1].isNull(fields[1])) {
            currentValue.percentile = cursors[1].getDouble(fields[1]);
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
            output.append(currentValue.digest.getQuantile(currentValue.percentile));
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
        return TupleInfo.SINGLE_LONG;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return TupleInfo.SINGLE_VARBINARY;
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
