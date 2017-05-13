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
import com.facebook.presto.operator.aggregation.TypedHistogram;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

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
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedState.class).instanceSize();
        private final ObjectBigArray<TypedHistogram> typedHistogram = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            typedHistogram.ensureCapacity(size);
        }

        @Override
        public TypedHistogram get()
        {
            return typedHistogram.get(getGroupId());
        }

        @Override
        public void set(TypedHistogram value)
        {
            requireNonNull(value, "value is null");

            TypedHistogram previous = get();
            if (previous != null) {
                size -= previous.getEstimatedSize();
            }

            typedHistogram.set(getGroupId(), value);
            size += value.getEstimatedSize();
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
    }

    public static class SingleState
            implements HistogramState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleState.class).instanceSize();
        private TypedHistogram typedHistogram;

        @Override
        public TypedHistogram get()
        {
            return typedHistogram;
        }

        @Override
        public void set(TypedHistogram value)
        {
            typedHistogram = value;
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
