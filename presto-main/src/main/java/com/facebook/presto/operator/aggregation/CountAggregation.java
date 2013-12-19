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
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.google.common.base.Preconditions.checkState;

public class CountAggregation
        implements AggregationFunction
{
    public static final CountAggregation COUNT = new CountAggregation();

    @Override
    public List<Type> getParameterTypes()
    {
        return ImmutableList.of();
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
    public CountGroupedAccumulator createGroupedAggregation(Optional<Integer> maskChannel, int[] argumentChannels)
    {
        return new CountGroupedAccumulator(maskChannel);
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAggregation()
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
        public TupleInfo getFinalTupleInfo()
        {
            return SINGLE_LONG;
        }

        @Override
        public TupleInfo getIntermediateTupleInfo()
        {
            return SINGLE_LONG;
        }

        public void addInput(GroupByIdBlock groupIdsBlock, Page page)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());

            if (!maskChannel.isPresent()) {
                for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                    long groupId = groupIdsBlock.getGroupId(position);
                    counts.increment(groupId);
                }
            }
            else {
                BlockCursor cursor = page.getBlock(maskChannel.get()).cursor();
                for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                    long groupId = groupIdsBlock.getGroupId(position);
                    cursor.advanceNextPosition();
                    if (cursor.getBoolean()) {
                        counts.increment(groupId);
                    }
                }
            }
        }

        @Override
        public void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());

                long groupId = groupIdsBlock.getGroupId(position);
                counts.add(groupId, intermediates.getLong());
            }
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            evaluateFinal(groupId, output);
        }

        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            long value = counts.get((long) groupId);
            output.append(value);
        }
    }

    @Override
    public CountAccumulator createAggregation(Optional<Integer> maskChannel, int... argumentChannels)
    {
        return new CountAccumulator(maskChannel);
    }

    @Override
    public CountAccumulator createIntermediateAggregation()
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
        public TupleInfo getFinalTupleInfo()
        {
            return SINGLE_LONG;
        }

        @Override
        public TupleInfo getIntermediateTupleInfo()
        {
            return SINGLE_LONG;
        }

        public void addInput(Page page)
        {
            if (!maskChannel.isPresent()) {
                count += page.getPositionCount();
            }
            else {
                BlockCursor masks = page.getBlock(maskChannel.get()).cursor();
                while (masks.advanceNextPosition()) {
                    if (masks.getBoolean()) {
                        count++;
                    }
                }
            }
        }

        @Override
        public void addIntermediate(Block block)
        {
            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());
                count += intermediates.getLong();
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
            BlockBuilder out = new BlockBuilder(getFinalTupleInfo());

            out.append(count);

            return out.build();
        }
    }
}
