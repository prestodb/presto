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
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.type.Type;
import com.facebook.presto.util.array.DoubleBigArray;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.base.Optional;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.type.Types.BIGINT;
import static com.facebook.presto.type.Types.DOUBLE;
import static com.facebook.presto.type.Types.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class AverageAggregation
        extends SimpleAggregationFunction
{
    private final boolean inputIsLong;

    public AverageAggregation(Type parameterType)
    {
        super(DOUBLE, VARCHAR, parameterType);

        if (parameterType == BIGINT) {
            this.inputIsLong = true;
        }
        else if (parameterType == DOUBLE) {
            this.inputIsLong = false;
        }
        else {
            throw new IllegalArgumentException("Expected parameter type to be FIXED_INT_64 or DOUBLE, but was " + parameterType);
        }
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "avg does not support approximate queries");
        return new AverageGroupedAccumulator(valueChannel, inputIsLong, maskChannel, sampleWeightChannel);
    }

    public static class AverageGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final boolean inputIsLong;

        private final LongBigArray counts;
        private final DoubleBigArray sums;

        public AverageGroupedAccumulator(int valueChannel, boolean inputIsLong, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel)
        {
            super(valueChannel, DOUBLE, VARCHAR, maskChannel, sampleWeightChannel);
            this.inputIsLong = inputIsLong;
            this.counts = new LongBigArray();
            this.sums = new DoubleBigArray();
        }

        @Override
        public long getEstimatedSize()
        {
            return counts.sizeOf() + sums.sizeOf();
        }

        @Override
        public void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());
            sums.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }
            BlockCursor sampleWeights = null;
            if (sampleWeightBlock.isPresent()) {
                sampleWeights = sampleWeightBlock.get().cursor();
            }

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());
                checkState(sampleWeights == null || sampleWeights.advanceNextPosition());

                long groupId = groupIdsBlock.getGroupId(position);

                long sampleWeight = computeSampleWeight(masks, sampleWeights);
                if (!values.isNull() && sampleWeight > 0) {
                    counts.add(groupId, sampleWeight);

                    double value;
                    if (inputIsLong) {
                        value = values.getLong();
                    }
                    else {
                        value = values.getDouble();
                    }
                    sums.add(groupId, sampleWeight * value);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void processIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());
            sums.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor intermediateValues = block.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(intermediateValues.advanceNextPosition());

                long groupId = groupIdsBlock.getGroupId(position);

                Slice value = intermediateValues.getSlice();
                long count = value.getLong(0);
                counts.add(groupId, count);

                double sum = value.getDouble(SIZE_OF_LONG);
                sums.add(groupId, sum);
            }
            checkState(!intermediateValues.advanceNextPosition());
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            long count = counts.get((long) groupId);
            double sum = sums.get((long) groupId);

            // todo replace this when general fixed with values are supported
            Slice value = Slices.allocate(SIZE_OF_LONG + SIZE_OF_DOUBLE);
            value.setLong(0, count);
            value.setDouble(SIZE_OF_LONG, sum);
            output.append(value);
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            long count = counts.get((long) groupId);
            if (count != 0) {
                double value = sums.get((long) groupId);
                output.append(value / count);
            }
            else {
                output.appendNull();
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "avg does not support approximate queries");
        return new AverageAccumulator(valueChannel, inputIsLong, maskChannel, sampleWeightChannel);
    }

    public static class AverageAccumulator
            extends SimpleAccumulator
    {
        private final boolean inputIsLong;

        private long count;
        private double sum;

        public AverageAccumulator(int valueChannel, boolean inputIsLong, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel)
        {
            super(valueChannel, DOUBLE, VARCHAR, maskChannel, sampleWeightChannel);
            this.inputIsLong = inputIsLong;
        }

        @Override
        protected void processInput(Block block, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            BlockCursor values = block.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }
            BlockCursor sampleWeights = null;
            if (sampleWeightBlock.isPresent()) {
                sampleWeights = sampleWeightBlock.get().cursor();
            }

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());
                checkState(sampleWeights == null || sampleWeights.advanceNextPosition());
                long sampleWeight = computeSampleWeight(masks, sampleWeights);
                if (!values.isNull() && sampleWeight > 0) {
                    count += sampleWeight;
                    if (inputIsLong) {
                        sum += sampleWeight * values.getLong();
                    }
                    else {
                        sum += sampleWeight * values.getDouble();
                    }
                }
            }
        }

        @Override
        protected void processIntermediate(Block block)
        {
            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());
                Slice value = intermediates.getSlice();
                count += value.getLong(0);
                sum += value.getDouble(SIZE_OF_LONG);
            }
        }

        @Override
        public void evaluateIntermediate(BlockBuilder out)
        {
            // todo replace this when general fixed with values are supported
            Slice value = Slices.allocate(SIZE_OF_LONG + SIZE_OF_DOUBLE);
            value.setLong(0, count);
            value.setDouble(SIZE_OF_LONG, sum);
            out.append(value);
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (count != 0) {
                out.append(sum / count);
            }
            else {
                out.appendNull();
            }
        }
    }
}
