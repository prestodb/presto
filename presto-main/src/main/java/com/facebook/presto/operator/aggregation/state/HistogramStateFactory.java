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
import com.facebook.presto.operator.aggregation.GroupedTypedHistogramSharedValues;
import com.facebook.presto.operator.aggregation.SingleTypedHistogram;
import com.facebook.presto.operator.aggregation.TypedHistogram;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static java.lang.String.format;

public class HistogramStateFactory
        implements AccumulatorStateFactory<HistogramState>
{
    @Override
    public HistogramState createSingleState()
    {
        return new SingleState();
    }

    @Override
    public Class<? extends HistogramState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public HistogramState createGroupedState()
    {
        return new GroupedState();
    }

    @Override
    public Class<? extends HistogramState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements HistogramState
    {
        private static final float FILL_RATIO = 0.9f;
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedState.class).instanceSize();
        private final ObjectBigArray<TypedHistogram> typedHistogram = new ObjectBigArray<>();
        private long size;
        private BlockBuilder blockBuilder;

        @Override
        public void ensureCapacity(long size)
        {
            typedHistogram.ensureCapacity(size);
        }

        @Override
        public TypedHistogram get()
        {
            long groupId = getGroupId();
            return typedHistogram.get(groupId);
        }

        @Override
        public void set(TypedHistogram value)
        {
            if (value instanceof GroupedTypedHistogramSharedValues) {
                typedHistogram.set(getGroupId(), (GroupedTypedHistogramSharedValues) value);
            }
            else {
                GroupedTypedHistogramSharedValues groupedTypedHistogramSharedValues = new GroupedTypedHistogramSharedValues(value.getType(), value.getExpectedSize());

                groupedTypedHistogramSharedValues.addAll(value);
                typedHistogram.set(getGroupId(), groupedTypedHistogramSharedValues);
            }
        }

        @Override
        public void deserialize(Block block, Type type, int expectedSize)
        {
            typedHistogram.set(getGroupId(), new GroupedTypedHistogramSharedValues(block, type, expectedSize));
        }

        @Override
        public void initHisogramIfNeeded(Type type, int expectedSize)
        {
            long groupId = getGroupId();
            TypedHistogram value = typedHistogram.get(groupId);

            if (value == null) {
                int hashCapacity = computeHashCapacity(expectedSize);

                if (blockBuilder == null) {
                    blockBuilder = type.createBlockBuilder(null, hashCapacity);
                }

                TypedHistogram newValue = new GroupedTypedHistogramSharedValues(type, expectedSize, hashCapacity, blockBuilder);

                typedHistogram.set(groupId, newValue);
                size += newValue.getEstimatedSize();
            }
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            size += memory;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + typedHistogram.sizeOf();
        }

        private static int computeHashCapacity(int expectedSize)
        {
            return arraySize(expectedSize, FILL_RATIO);
        }
    }

    public static class SingleState
            implements HistogramState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleState.class).instanceSize();
        private SingleTypedHistogram typedHistogram = null;

        @Override
        public TypedHistogram get()
        {
            return typedHistogram;
        }

        @Override
        public void set(TypedHistogram value)
        {
            if (value instanceof SingleTypedHistogram) {
                typedHistogram = (SingleTypedHistogram) value;
            }
            else {
                throw new IllegalArgumentException(format("can't set %s to %s", this.getClass(), value.getClass()));
            }
        }

        @Override
        public void deserialize(Block block, Type type, int expectedSize)
        {
            typedHistogram = new SingleTypedHistogram(block, type, expectedSize);
        }

        @Override
        public void initHisogramIfNeeded(Type type, int expectedSize)
        {
            if (typedHistogram == null) {
                typedHistogram = new SingleTypedHistogram(type, expectedSize);
            }
        }

        @Override
        public void addMemoryUsage(long memory)
        {
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (typedHistogram != null) {
                estimatedSize += typedHistogram.getEstimatedSize();
            }
            return estimatedSize;
        }
    }
}
