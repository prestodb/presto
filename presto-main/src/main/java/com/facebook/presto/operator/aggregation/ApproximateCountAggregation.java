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
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;

import static com.facebook.presto.operator.aggregation.ApproximateUtils.countError;
import static com.facebook.presto.operator.aggregation.ApproximateUtils.formatApproximateResult;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class ApproximateCountAggregation
        implements InternalAggregationFunction
{
    public static final ApproximateCountAggregation APPROXIMATE_COUNT_AGGREGATION = new ApproximateCountAggregation();

    private static final int COUNT_OFFSET = 0;
    private static final int SAMPLES_OFFSET = SIZE_OF_LONG;

    @Override
    public String name()
    {
        return "count";
    }

    @Override
    public List<Type> getParameterTypes()
    {
        return ImmutableList.of();
    }

    @Override
    public Type getFinalType()
    {
        return VARCHAR;
    }

    @Override
    public Type getIntermediateType()
    {
        // TODO: Change this to fixed width, once we have a better type system
        return VARCHAR;
    }

    @Override
    public boolean isDecomposable()
    {
        return true;
    }

    @Override
    public boolean isApproximate()
    {
        return true;
    }

    @Override
    public AccumulatorFactory bind(List<Integer> inputChannels, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
    {
        return new ApproximateCountAccumulatorFactory(inputChannels, maskChannel, sampleWeightChannel, confidence);
    }

    public static class ApproximateCountAccumulatorFactory
            implements AccumulatorFactory
    {
        private final List<Integer> inputChannels;
        private final Optional<Integer> maskChannel;
        private final Optional<Integer> sampleWeightChannel;
        private final double confidence;

        public ApproximateCountAccumulatorFactory(List<Integer> inputChannels, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
        {
            this.inputChannels = ImmutableList.copyOf(checkNotNull(inputChannels, "inputChannels is null"));
            this.maskChannel = checkNotNull(maskChannel, "maskChannel is null");
            this.sampleWeightChannel = checkNotNull(sampleWeightChannel, "sampleWeightChannel is null");
            this.confidence = confidence;
        }

        @Override
        public ApproximateCountGroupedAccumulator createGroupedAccumulator()
        {
            checkArgument(sampleWeightChannel.isPresent(), "sampleWeightChannel missing");
            return new ApproximateCountGroupedAccumulator(maskChannel, sampleWeightChannel.get(), confidence);
        }

        @Override
        public GroupedAccumulator createGroupedIntermediateAccumulator()
        {
            return new ApproximateCountGroupedAccumulator(Optional.<Integer>absent(), -1, confidence);
        }

        public static class ApproximateCountGroupedAccumulator
                implements GroupedAccumulator
        {
            private final LongBigArray counts;
            private final LongBigArray samples;
            private final Optional<Integer> maskChannel;
            private final int sampleWeightChannel;
            private final double confidence;

            public ApproximateCountGroupedAccumulator(Optional<Integer> maskChannel, int sampleWeightChannel, double confidence)
            {
                this.counts = new LongBigArray();
                this.samples = new LongBigArray();
                this.maskChannel = maskChannel;
                this.sampleWeightChannel = sampleWeightChannel;
                this.confidence = confidence;
            }

            @Override
            public long getEstimatedSize()
            {
                return counts.sizeOf() + samples.sizeOf();
            }

            @Override
            public Type getFinalType()
            {
                return VARCHAR;
            }

            @Override
            public Type getIntermediateType()
            {
                return VARCHAR;
            }

            @Override
            public void addInput(GroupByIdBlock groupIdsBlock, Page page)
            {
                counts.ensureCapacity(groupIdsBlock.getGroupCount());
                samples.ensureCapacity(groupIdsBlock.getGroupCount());
                Block masks = null;
                if (maskChannel.isPresent()) {
                    masks = page.getBlock(maskChannel.get());
                }
                Block sampleWeights = page.getBlock(sampleWeightChannel);

                for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                    long groupId = groupIdsBlock.getGroupId(position);
                    long weight = ApproximateUtils.computeSampleWeight(masks, sampleWeights, position);
                    counts.add(groupId, weight);
                    if (weight > 0) {
                        samples.increment(groupId);
                    }
                }
            }

            @Override
            public void addIntermediate(GroupByIdBlock groupIdsBlock, Block intermediates)
            {
                counts.ensureCapacity(groupIdsBlock.getGroupCount());
                samples.ensureCapacity(groupIdsBlock.getGroupCount());

                for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                    long groupId = groupIdsBlock.getGroupId(position);
                    Slice slice = VARCHAR.getSlice(intermediates, position);
                    counts.add(groupId, slice.getLong(COUNT_OFFSET));
                    samples.add(groupId, slice.getLong(SAMPLES_OFFSET));
                }
            }

            @Override
            public void evaluateIntermediate(int groupId, BlockBuilder output)
            {
                VARCHAR.writeSlice(output, createIntermediate(counts.get(groupId), samples.get(groupId)));
            }

            @Override
            public void evaluateFinal(int groupId, BlockBuilder output)
            {
                long count = counts.get(groupId);
                long samples = this.samples.get(groupId);
                String result = formatApproximateResult(count, countError(samples, count), confidence, true);
                VARCHAR.writeString(output, result);
            }
        }

        @Override
        public List<Integer> getInputChannels()
        {
            return inputChannels;
        }

        @Override
        public CountAccumulator createAccumulator()
        {
            checkArgument(sampleWeightChannel.isPresent(), "sampleWeightChannel missing");
            return new CountAccumulator(maskChannel, sampleWeightChannel.get(), confidence);
        }

        @Override
        public CountAccumulator createIntermediateAccumulator()
        {
            return new CountAccumulator(Optional.<Integer>absent(), -1, confidence);
        }

        public static class CountAccumulator
                implements Accumulator
        {
            private long count;
            private long samples;
            private final Optional<Integer> maskChannel;
            private final int sampleWeightChannel;
            private final double confidence;

            public CountAccumulator(Optional<Integer> maskChannel, int sampleWeightChannel, double confidence)
            {
                this.maskChannel = maskChannel;
                this.sampleWeightChannel = sampleWeightChannel;
                this.confidence = confidence;
            }

            @Override
            public long getEstimatedSize()
            {
                return 3 * SIZE_OF_LONG + SIZE_OF_INT + SIZE_OF_DOUBLE;
            }

            @Override
            public Type getFinalType()
            {
                return VARCHAR;
            }

            @Override
            public Type getIntermediateType()
            {
                return VARCHAR;
            }

            @Override
            public void addInput(Page page)
            {
                Block masks = null;
                if (maskChannel.isPresent()) {
                    masks = page.getBlock(maskChannel.get());
                }
                Block sampleWeights = page.getBlock(sampleWeightChannel);

                for (int position = 0; position < page.getPositionCount(); position++) {
                    long weight = ApproximateUtils.computeSampleWeight(masks, sampleWeights, position);
                    count += weight;
                    if (weight > 0) {
                        samples++;
                    }
                }
            }

            @Override
            public void addIntermediate(Block intermediates)
            {
                for (int position = 0; position < intermediates.getPositionCount(); position++) {
                    Slice slice = VARCHAR.getSlice(intermediates, position);
                    count += slice.getLong(COUNT_OFFSET);
                    samples += slice.getLong(SAMPLES_OFFSET);
                }
            }

            @Override
            public final Block evaluateIntermediate()
            {
                BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());
                VARCHAR.writeSlice(blockBuilder, createIntermediate(count, samples));
                return blockBuilder.build();
            }

            @Override
            public final Block evaluateFinal()
            {
                Slice value = Slices.utf8Slice(formatApproximateResult(count, countError(samples, count), confidence, true));

                Type finalType = getFinalType();
                BlockBuilder blockBuilder = finalType.createBlockBuilder(new BlockBuilderStatus());
                finalType.writeSlice(blockBuilder, value, 0, value.length());
                return blockBuilder.build();
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
}
