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

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class CountAggregation
        implements InternalAggregationFunction
{
    public static final CountAggregation COUNT = new CountAggregation();

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
        return BIGINT;
    }

    @Override
    public Type getIntermediateType()
    {
        return BIGINT;
    }

    @Override
    public boolean isDecomposable()
    {
        return true;
    }

    @Override
    public boolean isApproximate()
    {
        return false;
    }

    @Override
    public AccumulatorFactory bind(List<Integer> inputChannels, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
    {
        checkArgument(confidence == 1.0 && !sampleWeightChannel.isPresent(), "approximate not supported for count(*)");
        return new CountAccumulatorFactory(inputChannels, maskChannel);
    }

    public static class CountAccumulatorFactory
            implements AccumulatorFactory
    {
        private final List<Integer> inputChannels;
        private final Optional<Integer> maskChannel;

        public CountAccumulatorFactory(List<Integer> inputChannels, Optional<Integer> maskChannel)
        {
            this.inputChannels = ImmutableList.copyOf(checkNotNull(inputChannels, "inputChannels is null"));
            this.maskChannel = checkNotNull(maskChannel, "maskChannel is null");
        }

        @Override
        public CountGroupedAccumulator createGroupedAccumulator()
        {
            return new CountGroupedAccumulator(maskChannel);
        }

        @Override
        public GroupedAccumulator createGroupedIntermediateAccumulator()
        {
            return new CountGroupedAccumulator(Optional.<Integer>absent());
        }

        public static class CountGroupedAccumulator
                implements GroupedAccumulator
        {
            private final LongBigArray counts;
            private final Optional<Integer> maskChannel;

            public CountGroupedAccumulator(Optional<Integer> maskChannel)
            {
                this.counts = new LongBigArray();
                this.maskChannel = maskChannel;
            }

            @Override
            public long getEstimatedSize()
            {
                return counts.sizeOf();
            }

            @Override
            public Type getFinalType()
            {
                return BIGINT;
            }

            @Override
            public Type getIntermediateType()
            {
                return BIGINT;
            }

            @Override
            public void addInput(GroupByIdBlock groupIdsBlock, Page page)
            {
                counts.ensureCapacity(groupIdsBlock.getGroupCount());
                Block masks = maskChannel.isPresent() ? page.getBlock(maskChannel.get()) : null;

                for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                    long groupId = groupIdsBlock.getGroupId(position);
                    if (masks == null || BOOLEAN.getBoolean(masks, position)) {
                        counts.increment(groupId);
                    }
                }
            }

            @Override
            public void addIntermediate(GroupByIdBlock groupIdsBlock, Block intermediates)
            {
                counts.ensureCapacity(groupIdsBlock.getGroupCount());

                for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                    long groupId = groupIdsBlock.getGroupId(position);
                    counts.add(groupId, BIGINT.getLong(intermediates, position));
                }
            }

            @Override
            public void evaluateIntermediate(int groupId, BlockBuilder output)
            {
                evaluateFinal(groupId, output);
            }

            @Override
            public void evaluateFinal(int groupId, BlockBuilder output)
            {
                long value = counts.get((long) groupId);
                BIGINT.writeLong(output, value);
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
            return new CountAccumulator(maskChannel);
        }

        @Override
        public CountAccumulator createIntermediateAccumulator()
        {
            return new CountAccumulator(Optional.<Integer>absent());
        }

        public static class CountAccumulator
                implements Accumulator
        {
            private long count;
            private final Optional<Integer> maskChannel;

            public CountAccumulator(Optional<Integer> maskChannel)
            {
                this.maskChannel = maskChannel;
            }

            @Override
            public long getEstimatedSize()
            {
                return 0;
            }

            @Override
            public Type getFinalType()
            {
                return BIGINT;
            }

            @Override
            public Type getIntermediateType()
            {
                return BIGINT;
            }

            @Override
            public void addInput(Page page)
            {
                if (!maskChannel.isPresent()) {
                    count += page.getPositionCount();
                }
                else {
                    Block masks = page.getBlock(maskChannel.get());
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        if (masks == null || BOOLEAN.getBoolean(masks, position)) {
                            count++;
                        }
                    }
                }
            }

            @Override
            public void addIntermediate(Block intermediates)
            {
                for (int position = 0; position < intermediates.getPositionCount(); position++) {
                    count += BIGINT.getLong(intermediates, position);
                }
            }

            @Override
            public final Block evaluateIntermediate()
            {
                return evaluateFinal();
            }

            @Override
            public final Block evaluateFinal()
            {
                BlockBuilder out = getFinalType().createBlockBuilder(new BlockBuilderStatus());

                BIGINT.writeLong(out, count);

                return out.build();
            }
        }
    }
}
