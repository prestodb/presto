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
import com.facebook.presto.common.array.IntBigArray;
import com.facebook.presto.common.array.LongBigArray;
import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.geospatial.Rectangle;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.GroupedAccumulatorState;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.toIntExact;

public class SpatialPartitioningStateFactory
        implements AccumulatorStateFactory<SpatialPartitioningState>
{
    private static final int ENVELOPE_SIZE = toIntExact(new Envelope(1, 2, 3, 4).estimateMemorySize());

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
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedSpatialPartitioningState.class).instanceSize();

        private long groupId;
        private final IntBigArray partitionCounts = new IntBigArray();
        private final LongBigArray counts = new LongBigArray();
        private final ObjectBigArray<List<Rectangle>> samples = new ObjectBigArray<>();
        private int samplesCount;

        @Override
        public int getPartitionCount()
        {
            return partitionCounts.get(groupId);
        }

        @Override
        public void setPartitionCount(int partitionCount)
        {
            this.partitionCounts.set(groupId, partitionCount);
        }

        @Override
        public long getCount()
        {
            return counts.get(groupId);
        }

        @Override
        public void setCount(long count)
        {
            counts.set(groupId, count);
        }

        @Override
        public List<Rectangle> getSamples()
        {
            return samples.get(groupId);
        }

        @Override
        public void setSamples(List<Rectangle> samples)
        {
            List<Rectangle> currentSamples = this.samples.get(groupId);
            if (currentSamples != null) {
                samplesCount -= currentSamples.size();
            }
            samplesCount += samples.size();
            this.samples.set(groupId, samples);
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + partitionCounts.sizeOf() + counts.sizeOf() + samples.sizeOf() + ENVELOPE_SIZE * samplesCount;
        }

        @Override
        public void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(long size)
        {
            partitionCounts.ensureCapacity(size);
            counts.ensureCapacity(size);
            samples.ensureCapacity(size);
        }
    }

    public static final class SingleSpatialPartitioningState
            implements SpatialPartitioningState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleSpatialPartitioningState.class).instanceSize();

        private int partitionCount;
        private long count;
        private List<Rectangle> samples = new ArrayList<>();

        @Override
        public int getPartitionCount()
        {
            return partitionCount;
        }

        @Override
        public void setPartitionCount(int partitionCount)
        {
            this.partitionCount = partitionCount;
        }

        @Override
        public long getCount()
        {
            return count;
        }

        @Override
        public void setCount(long count)
        {
            this.count = count;
        }

        @Override
        public List<Rectangle> getSamples()
        {
            return samples;
        }

        @Override
        public void setSamples(List<Rectangle> samples)
        {
            this.samples = samples;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + samples.size() * ENVELOPE_SIZE;
        }
    }
}
