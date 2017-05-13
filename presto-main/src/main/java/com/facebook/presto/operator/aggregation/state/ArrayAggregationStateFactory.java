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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class ArrayAggregationStateFactory
        implements AccumulatorStateFactory<ArrayAggregationState>
{
    @Override
    public ArrayAggregationState createSingleState()
    {
        return new SingleArrayAggregationState();
    }

    @Override
    public Class<? extends ArrayAggregationState> getSingleStateClass()
    {
        return SingleArrayAggregationState.class;
    }

    @Override
    public ArrayAggregationState createGroupedState()
    {
        return new GroupedArrayAggregationState();
    }

    @Override
    public Class<? extends ArrayAggregationState> getGroupedStateClass()
    {
        return GroupedArrayAggregationState.class;
    }

    public static class GroupedArrayAggregationState
            extends AbstractGroupedAccumulatorState
            implements ArrayAggregationState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedArrayAggregationState.class).instanceSize();
        private final ObjectBigArray<BlockBuilder> blockBuilders = new ObjectBigArray<BlockBuilder>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            blockBuilders.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + blockBuilders.sizeOf();
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            size += memory;
        }

        @Override
        public BlockBuilder getBlockBuilder()
        {
            return blockBuilders.get(getGroupId());
        }

        @Override
        public void setBlockBuilder(BlockBuilder value)
        {
            requireNonNull(value, "value is null");

            BlockBuilder previous = getBlockBuilder();
            if (previous != null) {
                size -= previous.getRetainedSizeInBytes();
            }
            blockBuilders.set(getGroupId(), value);
            size += value.getRetainedSizeInBytes();
        }
    }

    public static class SingleArrayAggregationState
            implements ArrayAggregationState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleArrayAggregationState.class).instanceSize();
        private BlockBuilder blockBuilder;

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (blockBuilder != null) {
                estimatedSize += blockBuilder.getRetainedSizeInBytes();
            }
            return estimatedSize;
        }

        @Override
        public BlockBuilder getBlockBuilder()
        {
            return blockBuilder;
        }

        @Override
        public void setBlockBuilder(BlockBuilder value)
        {
            requireNonNull(value, "value is null");
            blockBuilder = value;
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            // no op
        }
    }
}
