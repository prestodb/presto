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

import io.airlift.slice.SliceOutput;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.array.LongBigArray;
import com.facebook.presto.util.array.ObjectBigArray;

public class ArrayAggregationStateFactory
        implements AccumulatorStateFactory<ArrayAggregationState>
{
    private final Type elementType;

    public ArrayAggregationStateFactory(Type elementType)
    {
        this.elementType = elementType;
    }

    @Override
    public ArrayAggregationState createSingleState()
    {
        return new SingleArrayAggregationState(elementType);
    }

    @Override
    public Class<? extends ArrayAggregationState> getSingleStateClass()
    {
        return SingleArrayAggregationState.class;
    }

    @Override
    public ArrayAggregationState createGroupedState()
    {
        return new GroupedArrayAggregationState(elementType);
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
        private final Type elementType;
        private final ObjectBigArray<SliceOutput> slices = new ObjectBigArray<SliceOutput>();
        private final LongBigArray entries = new LongBigArray();

        public GroupedArrayAggregationState(Type elementType)
        {
            this.elementType = elementType;
        }

        @Override
        public void ensureCapacity(long size)
        {
            slices.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return slices.sizeOf();
        }

        @Override
        public Type getType()
        {
            return elementType;
        }

        @Override
        public SliceOutput getSliceOutput()
        {
            return slices.get(getGroupId());
        }

        @Override
        public void setSliceOutput(SliceOutput value)
        {
            slices.ensureCapacity(getGroupId());
            slices.set(getGroupId(), value);
        }

        @Override
        public long getEntries()
        {
            return entries.get(getGroupId());
        }

        @Override
        public void setEntries(long value)
        {
            entries.ensureCapacity(getGroupId());
            entries.set(getGroupId(), value);
        }

    }

    public static class SingleArrayAggregationState
            implements ArrayAggregationState
    {
        private final Type elementType;
        private SliceOutput slice;
        private long entries;

        public SingleArrayAggregationState(Type elementType)
        {
            this.elementType = elementType;
        }

        @Override
        public long getEstimatedSize()
        {
            if (slice != null) {
                return slice.size();
            }
            else {
                return 0L;
            }
        }

        @Override
        public Type getType()
        {
            return elementType;
        }

        @Override
        public SliceOutput getSliceOutput()
        {
            return slice;
        }

        @Override
        public void setSliceOutput(SliceOutput value)
        {
            this.slice = value;
        }

        @Override
        public long getEntries()
        {
            return entries;
        }

        @Override
        public void setEntries(long value)
        {
            this.entries = value;
        }
    }
}
