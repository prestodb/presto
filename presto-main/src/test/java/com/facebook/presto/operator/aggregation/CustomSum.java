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
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.array.BooleanBigArray;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.base.Optional;

import java.util.List;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.google.common.base.Preconditions.checkState;

public class CustomSum
        implements AggregationFunction
{
    @Override
    public List<TupleInfo.Type> getParameterTypes()
    {
        return null;
    }

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return SINGLE_LONG;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return SINGLE_LONG;
    }

    @Override
    public Accumulator createAggregation(Optional<Integer> maskChannel, int... argumentChannels)
    {
        return new CustomSumAccumulator(argumentChannels[0], maskChannel);
    }

    @Override
    public Accumulator createIntermediateAggregation()
    {
        return new CustomSumAccumulator(-1, Optional.<Integer>absent());
    }

    @Override
    public GroupedAccumulator createGroupedAggregation(Optional<Integer> maskChannel, int... argumentChannels)
    {
        return new CustomSumGroupedAccumulator(argumentChannels[0], maskChannel);
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAggregation()
    {
        return new CustomSumGroupedAccumulator(-1, Optional.<Integer>absent());
    }

    public static class CustomSumAccumulator
            implements Accumulator
    {
        private final int channel;
        private boolean notNull;
        private long sum;
        private final Optional<Integer> maskChannel;

        public CustomSumAccumulator(int channel, Optional<Integer> maskChannel)
        {
            this.channel = channel;
            this.maskChannel = maskChannel;
        }

        @Override
        public TupleInfo getFinalTupleInfo()
        {
            return SINGLE_LONG;
        }

        @Override
        public TupleInfo getIntermediateTupleInfo()
        {
            return SINGLE_LONG;
        }

        @Override
        public void addInput(Page page)
        {
            processBlock(page.getBlock(channel), maskChannel.isPresent() ? Optional.of(page.getBlock(maskChannel.get())) : Optional.<Block>absent());
        }

        private void processBlock(Block block, Optional<Block> maskBlock)
        {
            BlockCursor values = block.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());
                if (!values.isNull() && (masks == null || masks.getBoolean())) {
                    notNull = true;
                    sum += values.getLong();
                }
            }
        }

        @Override
        public void addIntermediate(Block block)
        {
            processBlock(block, Optional.<Block>absent());
        }

        @Override
        public Block evaluateIntermediate()
        {
            BlockBuilder out = new BlockBuilder(getIntermediateTupleInfo());
            return getBlock(out);
        }

        @Override
        public Block evaluateFinal()
        {
            BlockBuilder out = new BlockBuilder(getFinalTupleInfo());
            return getBlock(out);
        }

        private Block getBlock(BlockBuilder out)
        {
            if (notNull) {
                out.append(sum);
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

        public CustomSumGroupedAccumulator(int channel, Optional<Integer> maskChannel)
        {
            this.channel = channel;
            this.notNull = new BooleanBigArray();
            this.sums = new LongBigArray();
            this.maskChannel = maskChannel;
        }

        @Override
        public long getEstimatedSize()
        {
            return notNull.sizeOf() + sums.sizeOf();
        }

        @Override
        public TupleInfo getFinalTupleInfo()
        {
            return SINGLE_LONG;
        }

        @Override
        public TupleInfo getIntermediateTupleInfo()
        {
            return SINGLE_LONG;
        }

        @Override
        public void addInput(GroupByIdBlock groupIdsBlock, Page page)
        {
            processBlock(groupIdsBlock, page.getBlock(channel), maskChannel.transform(page.blockGetter()));
        }

        @Override
        public void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            processBlock(groupIdsBlock, block, Optional.<Block>absent());
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
                output.append(value);
            }
            else {
                output.appendNull();
            }
        }

        private void processBlock(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock)
        {
            notNull.ensureCapacity(groupIdsBlock.getGroupCount());
            sums.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());

                long groupId = groupIdsBlock.getGroupId(position);

                if (!values.isNull() && (masks == null || masks.getBoolean())) {
                    notNull.set(groupId, true);

                    long value = values.getLong();
                    sums.add(groupId, value);
                }
            }
        }
    }
}
