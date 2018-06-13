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
package com.facebook.presto.plugin.geospatial;

import com.esri.core.geometry.Envelope;
import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.GroupedAccumulatorState;

import java.util.ArrayList;
import java.util.List;

public class SpatialPartitioningStateFactory
        implements AccumulatorStateFactory<SpatialPartitioningState>
{
    @Override
    public SpatialPartitioningState createSingleState()
    {
        return new SingleSpatialPartitioningState();
    }

    @Override
    public Class getSingleStateClass()
    {
        return SpatialPartitioningState.class;
    }

    @Override
    public SpatialPartitioningState createGroupedState()
    {
        return new GroupedSpatialPartitioningState();
    }

    @Override
    public Class getGroupedStateClass()
    {
        return GroupedSpatialPartitioningState.class;
    }

    public static final class GroupedSpatialPartitioningState
            implements GroupedAccumulatorState, SpatialPartitioningState
    {
        private int count;
        private Envelope envelope;
        private long groupId;
        private ObjectBigArray<List<Envelope>> samples = new ObjectBigArray<>();

        @Override
        public int getCount()
        {
            return count;
        }

        @Override
        public void setCount(int count)
        {
            this.count = count;
        }

        @Override
        public Envelope getEnvelope()
        {
            return envelope;
        }

        @Override
        public void setEnvelope(Envelope envelope)
        {
            this.envelope = envelope;
        }

        @Override
        public List<Envelope> getSamples()
        {
            return samples.get(groupId);
        }

        @Override
        public void setSamples(List<Envelope> samples)
        {
            this.samples.set(groupId, samples);
        }

        @Override
        public long getEstimatedSize()
        {
            // TODO Implement
            return 0;
        }

        @Override
        public void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(long size)
        {
            samples.ensureCapacity(size);
        }
    }

    public static final class SingleSpatialPartitioningState
            implements SpatialPartitioningState
    {
        private int count;
        private Envelope envelope;
        private List<Envelope> samples = new ArrayList<>();

        @Override
        public int getCount()
        {
            return count;
        }

        @Override
        public void setCount(int count)
        {
            this.count = count;
        }

        @Override
        public Envelope getEnvelope()
        {
            return envelope;
        }

        @Override
        public void setEnvelope(Envelope envelope)
        {
            this.envelope = envelope;
        }

        @Override
        public List<Envelope> getSamples()
        {
            return samples;
        }

        @Override
        public void setSamples(List<Envelope> samples)
        {
            this.samples = samples;
        }

        @Override
        public long getEstimatedSize()
        {
            // TODO Implement
            return 0;
        }
    }
}
