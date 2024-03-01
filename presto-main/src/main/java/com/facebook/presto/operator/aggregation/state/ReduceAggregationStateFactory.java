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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

public class ReduceAggregationStateFactory
        implements AccumulatorStateFactory<ReduceAggregationState>
{
    @Override
    public ReduceAggregationState createSingleState()
    {
        return new SingleState();
    }

    @Override
    public Class<? extends ReduceAggregationState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public ReduceAggregationState createGroupedState()
    {
        return new GroupedState();
    }

    @Override
    public Class<? extends ReduceAggregationState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    private static long getObjectSizeInBytes(Object value)
    {
        if (value instanceof Block) {
            return ((Block) value).getRetainedSizeInBytes();
        }

        if (value instanceof Slice) {
            return ((Slice) value).getRetainedSize();
        }

        return 0;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements ReduceAggregationState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedState.class).instanceSize();

        private final ObjectBigArray<Object> values = new ObjectBigArray<>();
        private long size = INSTANCE_SIZE;

        @Override
        public long getEstimatedSize()
        {
            return size + values.sizeOf();
        }

        @Override
        public Object getValue()
        {
            return values.get(getGroupId());
        }

        @Override
        public void setValue(Object value)
        {
            size += getObjectSizeInBytes(value) - getObjectSizeInBytes(getValue());
            values.set(getGroupId(), value);
        }

        @Override
        public void ensureCapacity(long size)
        {
            values.ensureCapacity(size);
        }
    }

    public static class SingleState
            implements ReduceAggregationState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedState.class).instanceSize();

        private Object value;

        @Override
        public Object getValue()
        {
            return value;
        }

        @Override
        public void setValue(Object value)
        {
            this.value = value;
        }

        @Override
        public long getEstimatedSize()
        {
            return getObjectSizeInBytes(value) + INSTANCE_SIZE;
        }
    }
}
