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

public class ReservoirSampleStateFactory
        implements AccumulatorStateFactory<ReservoirSampleState>
{
    @Override
    public ReservoirSampleState createSingleState()
    {
        return new SingleState();
    }

    @Override
    public Class<? extends ReservoirSampleState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public ReservoirSampleState createGroupedState()
    {
        return new GroupedState();
    }

    @Override
    public Class<? extends ReservoirSampleState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements ReservoirSampleState
    {
        private final ObjectBigArray<AbstractReservoirSample> samples = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            samples.ensureCapacity(size);
        }

        @Override
        public void setSample(AbstractReservoirSample sample)
        {
            requireNonNull(sample, "sample is null");

            AbstractReservoirSample previous = getSample();
            if (previous != null) {
                size -= previous.getRequiredBytesForSerialization();
            }

            samples.set(getGroupId(), sample);
            size += sample.getRequiredBytesForSerialization();
        }

        @Override
        public AbstractReservoirSample getSample()
        {
            return samples.get(getGroupId());
        }

        @Override
        public long getEstimatedSize()
        {
            return size + samples.sizeOf();
        }
    }

    public static class SingleState
            implements ReservoirSampleState
    {
        private AbstractReservoirSample sample;

        @Override
        public void setSample(AbstractReservoirSample sample)
        {
            requireNonNull(sample, "sample is null");

            this.sample = sample;
        }

        @Override
        public AbstractReservoirSample getSample()
        {
            return sample;
        }

        @Override
        public long getEstimatedSize()
        {
            if (sample == null) {
                return 0;
            }
            return sample.getRequiredBytesForSerialization();
        }
    }
}
