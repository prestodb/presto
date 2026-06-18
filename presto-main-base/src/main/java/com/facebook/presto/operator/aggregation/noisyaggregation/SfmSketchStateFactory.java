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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.SfmSketch;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.GroupedAccumulatorState;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class SfmSketchStateFactory
        implements AccumulatorStateFactory<SfmSketchState>
{
    @Override
    public SfmSketchState createSingleState()
    {
        return new SingleSfmSketchState();
    }

    @Override
    public Class<? extends SfmSketchState> getSingleStateClass()
    {
        return SingleSfmSketchState.class;
    }

    @Override
    public SfmSketchState createGroupedState()
    {
        return new GroupedSfmSketchState();
    }

    @Override
    public Class<? extends SfmSketchState> getGroupedStateClass()
    {
        return GroupedSfmSketchState.class;
    }

    public static class GroupedSfmSketchState
            implements SfmSketchState, GroupedAccumulatorState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedSfmSketchState.class).instanceSize();

        private final ObjectBigArray<SfmSketch> sketches = new ObjectBigArray<>();
        private final ObjectBigArray<Double> epsilons = new ObjectBigArray<>();

        private long retainedBytes;
        private long groupId;

        @Override
        public final void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        protected final long getGroupId()
        {
            return groupId;
        }

        @Override
        public void ensureCapacity(long size)
        {
            epsilons.ensureCapacity(size);
            sketches.ensureCapacity(size);
        }

        @Override
        public SfmSketch getSketch()
        {
            return sketches.get(getGroupId());
        }

        @Override
        public void mergeSketch(SfmSketch value)
        {
            requireNonNull(value, "value is null");
            retainedBytes -= getSketch().getRetainedSizeInBytes();
            getSketch().mergeWith(value);
            retainedBytes += getSketch().getRetainedSizeInBytes();
        }

        @Override
        public void setSketch(SfmSketch value)
        {
            requireNonNull(value, "value is null");
            if (getSketch() != null) {
                retainedBytes -= getSketch().getRetainedSizeInBytes();
            }
            sketches.set(getGroupId(), value);
            retainedBytes += value.getRetainedSizeInBytes();
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + retainedBytes + sketches.sizeOf() + epsilons.sizeOf();
        }

        @Override
        public void setEpsilon(double value)
        {
            epsilons.set(getGroupId(), value);
        }

        @Override
        public double getEpsilon()
        {
            return epsilons.get(getGroupId());
        }
    }

    public static class SingleSfmSketchState
            implements SfmSketchState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleSfmSketchState.class).instanceSize();

        private SfmSketch sketch;
        private double epsilon;

        @Override
        public SfmSketch getSketch()
        {
            return sketch;
        }

        @Override
        public void mergeSketch(SfmSketch value)
        {
            requireNonNull(value, "value is null");
            sketch.mergeWith(value);
        }

        @Override
        public void setSketch(SfmSketch value)
        {
            sketch = value;
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (sketch != null) {
                estimatedSize += sketch.getRetainedSizeInBytes();
            }
            return estimatedSize;
        }

        @Override
        public void setEpsilon(double value)
        {
            epsilon = value;
        }

        @Override
        public double getEpsilon()
        {
            return epsilon;
        }
    }
}
