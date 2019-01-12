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
package io.prestosql.operator.aggregation.minmaxby;

import io.prestosql.array.ObjectBigArray;
import io.prestosql.operator.aggregation.TypedKeyValueHeap;
import io.prestosql.operator.aggregation.state.AbstractGroupedAccumulatorState;
import io.prestosql.spi.function.AccumulatorStateFactory;
import org.openjdk.jol.info.ClassLayout;

public class MinMaxByNStateFactory
        implements AccumulatorStateFactory<MinMaxByNState>
{
    @Override
    public MinMaxByNState createSingleState()
    {
        return new SingleMinMaxByNState();
    }

    @Override
    public Class<? extends MinMaxByNState> getSingleStateClass()
    {
        return SingleMinMaxByNState.class;
    }

    @Override
    public MinMaxByNState createGroupedState()
    {
        return new GroupedMinMaxByNState();
    }

    @Override
    public Class<? extends MinMaxByNState> getGroupedStateClass()
    {
        return GroupedMinMaxByNState.class;
    }

    public static class GroupedMinMaxByNState
            extends AbstractGroupedAccumulatorState
            implements MinMaxByNState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedMinMaxByNState.class).instanceSize();
        private final ObjectBigArray<TypedKeyValueHeap> heaps = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            heaps.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + heaps.sizeOf() + size;
        }

        @Override
        public TypedKeyValueHeap getTypedKeyValueHeap()
        {
            return heaps.get(getGroupId());
        }

        @Override
        public void setTypedKeyValueHeap(TypedKeyValueHeap value)
        {
            TypedKeyValueHeap previous = getTypedKeyValueHeap();
            if (previous != null) {
                size -= previous.getEstimatedSize();
            }
            heaps.set(getGroupId(), value);
            size += value.getEstimatedSize();
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            size += memory;
        }
    }

    public static class SingleMinMaxByNState
            implements MinMaxByNState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleMinMaxByNState.class).instanceSize();
        private TypedKeyValueHeap typedKeyValueHeap;

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (typedKeyValueHeap != null) {
                estimatedSize += typedKeyValueHeap.getEstimatedSize();
            }
            return estimatedSize;
        }

        @Override
        public TypedKeyValueHeap getTypedKeyValueHeap()
        {
            return typedKeyValueHeap;
        }

        @Override
        public void setTypedKeyValueHeap(TypedKeyValueHeap typedKeyValueHeap)
        {
            this.typedKeyValueHeap = typedKeyValueHeap;
        }

        @Override
        public void addMemoryUsage(long memory)
        {
        }
    }
}
