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

import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.base.Optional;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.operator.aggregation.ApproximateUtils.countError;
import static com.facebook.presto.operator.aggregation.ApproximateUtils.formatApproximateResult;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class ApproximateCountColumnAggregation
        extends SimpleAggregationFunction
{
    private static final int COUNT_OFFSET = 0;
    private static final int SAMPLES_OFFSET = SIZE_OF_LONG;

    public ApproximateCountColumnAggregation(Type parameterType)
    {
        // TODO: Change intermediate to fixed width, once we have a better type system
        super(VARCHAR, VARCHAR, parameterType);
    }

    @Override
    public ApproximateCountColumnGroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        // valueChannel is -1 for intermediates
        checkArgument(sampleWeightChannel.isPresent() ^ valueChannel == -1, "sampleWeightChannel is missing");
        return new ApproximateCountColumnGroupedAccumulator(valueChannel, maskChannel, sampleWeightChannel, confidence);
    }

    public static class ApproximateCountColumnGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final LongBigArray counts;
        private final LongBigArray samples;
        private final double confidence;

        public ApproximateCountColumnGroupedAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
        {
            super(valueChannel, VARCHAR, VARCHAR, maskChannel, sampleWeightChannel);
            this.counts = new LongBigArray();
            this.samples = new LongBigArray();
            this.confidence = confidence;
        }

        @Override
        public long getEstimatedSize()
        {
            return counts.sizeOf() + samples.sizeOf();
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());
            samples.ensureCapacity(groupIdsBlock.getGroupCount());
            BlockCursor values = valuesBlock.cursor();
            BlockCursor sampleWeights = sampleWeightBlock.get().cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                long groupId = groupIdsBlock.getGroupId(position);
                checkState(masks == null || masks.advanceNextPosition(), "failed to advance mask cursor");
                checkState(sampleWeights.advanceNextPosition(), "failed to advance weight cursor");
                checkState(values.advanceNextPosition(), "failed to advance values cursor");
                long weight = values.isNull() ? 0 : SimpleAggregationFunction.computeSampleWeight(masks, sampleWeights);
                counts.add(groupId, weight);
                if (weight > 0) {
                    samples.increment(groupId);
                }
            }
        }

        @Override
        public void processIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());
            samples.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition(), "failed to advance intermediates cursor");

                long groupId = groupIdsBlock.getGroupId(position);
                Slice slice = intermediates.getSlice();
                counts.add(groupId, slice.getLong(COUNT_OFFSET));
                samples.add(groupId, slice.getLong(SAMPLES_OFFSET));
            }
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            output.appendSlice(createIntermediate(counts.get(groupId), samples.get(groupId)));
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            long count = counts.get(groupId);
            long samples = this.samples.get(groupId);
            String result = formatApproximateResult(count, countError(samples, count), confidence, true);
            output.appendSlice(Slices.utf8Slice(result));
        }
    }

    @Override
    public ApproximateCountColumnAccumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        // valueChannel is -1 for intermediates
        checkArgument(sampleWeightChannel.isPresent() ^ valueChannel == -1, "sampleWeightChannel is missing");
        return new ApproximateCountColumnAccumulator(valueChannel, maskChannel, sampleWeightChannel, confidence);
    }

    public static class ApproximateCountColumnAccumulator
            extends SimpleAccumulator
    {
        private long count;
        private long samples;
        private final double confidence;

        public ApproximateCountColumnAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
        {
            super(valueChannel, VARCHAR, VARCHAR, maskChannel, sampleWeightChannel);
            this.confidence = confidence;
        }

        @Override
        protected void processInput(Block block, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            BlockCursor values = block.cursor();
            BlockCursor sampleWeights = sampleWeightBlock.get().cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            for (int i = 0; i < block.getPositionCount(); i++) {
                checkState(masks == null || masks.advanceNextPosition(), "failed to advance mask cursor");
                checkState(sampleWeights.advanceNextPosition(), "failed to advance weight cursor");
                checkState(values.advanceNextPosition(), "failed to advance values cursor");
                long weight = values.isNull() ? 0 : SimpleAggregationFunction.computeSampleWeight(masks, sampleWeights);
                count += weight;
                if (weight > 0) {
                    samples++;
                }
            }
        }

        @Override
        public void processIntermediate(Block block)
        {
            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition(), "failed to advance intermediates cursor");
                Slice slice = intermediates.getSlice();
                count += slice.getLong(COUNT_OFFSET);
                samples += slice.getLong(SAMPLES_OFFSET);
            }
        }

        @Override
        public void evaluateIntermediate(BlockBuilder out)
        {
            out.appendSlice(createIntermediate(count, samples));
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            String result = formatApproximateResult(count, countError(samples, count), confidence, true);
            out.appendSlice(Slices.utf8Slice(result));
        }
    }

    public static Slice createIntermediate(long count, long samples)
    {
        Slice slice = Slices.allocate(2 * SIZE_OF_LONG);
        slice.setLong(COUNT_OFFSET, count);
        slice.setLong(SAMPLES_OFFSET, samples);
        return slice;
    }
}
