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
package com.facebook.presto.tests;

import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.array.BooleanBigArray;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.base.Optional;

import java.util.List;

import static com.facebook.presto.operator.aggregation.SimpleAggregationFunction.computeSampleWeight;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class CustomSum
        implements AggregationFunction
{
    @Override
    public List<Type> getParameterTypes()
    {
        return null;
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
        return false;
    }

    @Override
    public Accumulator createAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int... argumentChannels)
    {
        checkArgument(confidence == 1.0, "custom sum does not support approximate queries");
        return new CustomSumAccumulator(argumentChannels[0], maskChannel, sampleWeightChannel);
    }

    @Override
    public Accumulator createIntermediateAggregation(double confidence)
    {
        checkArgument(confidence == 1.0, "custom sum does not support approximate queries");
        return new CustomSumAccumulator(-1, Optional.<Integer>absent(), Optional.<Integer>absent());
    }

    @Override
    public GroupedAccumulator createGroupedAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int... argumentChannels)
    {
        checkArgument(confidence == 1.0, "custom sum does not support approximate queries");
        return new CustomSumGroupedAccumulator(argumentChannels[0], maskChannel, sampleWeightChannel);
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAggregation(double confidence)
    {
        checkArgument(confidence == 1.0, "custom sum does not support approximate queries");
        return new CustomSumGroupedAccumulator(-1, Optional.<Integer>absent(), Optional.<Integer>absent());
    }

    public static class CustomSumAccumulator
            implements Accumulator
    {
        private final int channel;
        private boolean notNull;
        private long sum;
        private final Optional<Integer> maskChannel;
        private final Optional<Integer> sampleWeightChannel;

        public CustomSumAccumulator(int channel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel)
        {
            this.channel = channel;
            this.maskChannel = maskChannel;
            this.sampleWeightChannel = sampleWeightChannel;
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
            processBlock(page.getBlock(channel), maskChannel.transform(page.blockGetter()), sampleWeightChannel.transform(page.blockGetter()));
        }

        private void processBlock(Block block, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            BlockCursor values = block.cursor();
            BlockCursor masks = null;
            BlockCursor sampleWeights = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }
            if (sampleWeightBlock.isPresent()) {
                sampleWeights = sampleWeightBlock.get().cursor();
            }

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());
                checkState(sampleWeights == null || sampleWeights.advanceNextPosition());

                long sampleWeight = computeSampleWeight(masks, sampleWeights);
                if (!values.isNull() && sampleWeight > 0) {
                    notNull = true;
                    sum += sampleWeight * values.getLong();
                }
            }
        }

        @Override
        public void addIntermediate(Block block)
        {
            processBlock(block, Optional.<Block>absent(), Optional.<Block>absent());
        }

        @Override
        public Block evaluateIntermediate()
        {
            BlockBuilder out = getIntermediateType().createBlockBuilder(new BlockBuilderStatus());
            return getBlock(out);
        }

        @Override
        public Block evaluateFinal()
        {
            BlockBuilder out = getFinalType().createBlockBuilder(new BlockBuilderStatus());
            return getBlock(out);
        }

        private Block getBlock(BlockBuilder out)
        {
            if (notNull) {
                out.appendLong(sum);
            }
            else {
                out.appendNull();
            }
            return out.build();
        }
    }

    public static class CustomSumGroupedAccumulator
            implements GroupedAccumulator
    {
        private final int channel;
        private final BooleanBigArray notNull;
        private final LongBigArray sums;
        private final Optional<Integer> maskChannel;
        private final Optional<Integer> sampleWeightChannel;

        public CustomSumGroupedAccumulator(int channel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel)
        {
            this.channel = channel;
            this.notNull = new BooleanBigArray();
            this.sums = new LongBigArray();
            this.maskChannel = maskChannel;
            this.sampleWeightChannel = sampleWeightChannel;
        }

        @Override
        public long getEstimatedSize()
        {
            return notNull.sizeOf() + sums.sizeOf();
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
            processBlock(groupIdsBlock, page.getBlock(channel), maskChannel.transform(page.blockGetter()), sampleWeightChannel.transform(page.blockGetter()));
        }

        @Override
        public void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            processBlock(groupIdsBlock, block, Optional.<Block>absent(), Optional.<Block>absent());
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            evaluateFinal(groupId, output);
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            if (notNull.get((long) groupId)) {
                long value = sums.get((long) groupId);
                output.appendLong(value);
            }
            else {
                output.appendNull();
            }
        }

        private void processBlock(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            notNull.ensureCapacity(groupIdsBlock.getGroupCount());
            sums.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();
            BlockCursor masks = null;
            BlockCursor sampleWeights = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }
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
                    notNull.set(groupId, true);

                    long value = sampleWeight * values.getLong();
                    sums.add(groupId, value);
                }
            }
        }
    }
}
