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
package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;

import static java.util.Objects.requireNonNull;

public class UnweightedIntReservoirSampleStateFactory
        implements AccumulatorStateFactory<UnweightedIntReservoirSampleState>
{
    @Override
    public UnweightedIntReservoirSampleState createSingleState()
    {
        return new SingleState();
    }

    @Override
    public Class<? extends UnweightedIntReservoirSampleState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public UnweightedIntReservoirSampleState createGroupedState()
    {
        return new GroupedState();
    }

    @Override
    public Class<? extends UnweightedIntReservoirSampleState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements UnweightedIntReservoirSampleState
    {
        private ObjectBigArray<UnweightedIntReservoirSample> reservoirs = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            reservoirs.ensureCapacity(size);
        }

        @Override
        public void setReservoir(UnweightedIntReservoirSample reservoir)
        {
            UnweightedIntReservoirSample previous = requireNonNull(reservoir, "reservoir is null");
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            reservoirs.set(getGroupId(), reservoir);
            size += reservoir.estimatedInMemorySize();
        }

        @Override
        public UnweightedIntReservoirSample getReservoir()
        {
            return reservoirs.get(getGroupId());
        }

        @Override
        public long getEstimatedSize()
        {
            return size + reservoirs.sizeOf();
        }
    }

    public static class SingleState
            implements UnweightedIntReservoirSampleState
    {
        private UnweightedIntReservoirSample reservoir;

        @Override
        public void setReservoir(UnweightedIntReservoirSample reservoir)
        {
            this.reservoir = requireNonNull(reservoir, "reservoir is null");
        }

        @Override
        public UnweightedIntReservoirSample getReservoir()
        {
            return reservoir;
        }

        @Override
        public long getEstimatedSize()
        {
            return reservoir.estimatedInMemorySize();
        }
    }
}
