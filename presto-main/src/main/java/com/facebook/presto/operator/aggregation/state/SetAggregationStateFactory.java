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

import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.SetOfValues;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class SetAggregationStateFactory
        implements AccumulatorStateFactory
{
    private final Type elementType;

    public SetAggregationStateFactory(Type elementType)
    {
        this.elementType = elementType;
    }

    @Override
    public SetAggregationState createSingleState()
    {
        return new SingleState(elementType);
    }

    @Override
    public Class<? extends SetAggregationState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public SetAggregationState createGroupedState()
    {
        return new GroupedState(elementType);
    }

    @Override
    public Class<? extends SetAggregationState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements SetAggregationState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedState.class).instanceSize();
        private final Type elementType;
        private long size;
        private final ObjectBigArray<SetOfValues> set = new ObjectBigArray<>();

        public GroupedState(Type elementType)
        {
            this.elementType = elementType;
        }

        @Override
        public void ensureCapacity(long size)
        {
            set.ensureCapacity(size);
        }

        @Override
        public SetOfValues get()
        {
            return set.get(getGroupId());
        }

        @Override
        public void set(SetOfValues value)
        {
            requireNonNull(value, "value is null");

            SetOfValues previous = get();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            set.set(getGroupId(), value);
            size += value.estimatedInMemorySize();
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            size += memory;
        }

        @Override
        public Type getElementType()
        {
            return elementType;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size;
        }
    }

    public static class SingleState
            implements SetAggregationState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleState.class).instanceSize();
        private final Type elementType;
        private SetOfValues set;

        public SingleState(Type elementType)
        {
            this.elementType = elementType;
        }

        @Override
        public SetOfValues get()
        {
            return set;
        }

        @Override
        public void set(SetOfValues set)
        {
            this.set = set;
        }

        @Override
        public void addMemoryUsage(long memory)
        {
        }

        @Override
        public Type getElementType()
        {
            return elementType;
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (set != null) {
                estimatedSize += set.estimatedInMemorySize();
            }
            return estimatedSize;
        }
    }
}
