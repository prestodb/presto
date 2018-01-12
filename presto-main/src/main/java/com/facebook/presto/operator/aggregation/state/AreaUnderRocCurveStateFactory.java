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

import com.facebook.presto.operator.aggregation.GroupedAreaUnderRocCurve;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class AreaUnderRocCurveStateFactory
        implements AccumulatorStateFactory<AreaUnderRocCurveState>
{
    @Override
    public AreaUnderRocCurveState createSingleState()
    {
        return new SingleState();
    }

    @Override
    public Class<? extends AreaUnderRocCurveState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public AreaUnderRocCurveState createGroupedState()
    {
        return new GroupedState();
    }

    @Override
    public Class<? extends AreaUnderRocCurveState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements AreaUnderRocCurveState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedState.class).instanceSize();
        private GroupedAreaUnderRocCurve auc;
        private long size;

        public GroupedState()
        {
            auc = new GroupedAreaUnderRocCurve();
        }

        @Override
        public void ensureCapacity(long size)
        {
            auc.ensureCapacity(size);
        }

        @Override
        public GroupedAreaUnderRocCurve get()
        {
            return auc.setGroupId(getGroupId());
        }

        @Override
        public void set(GroupedAreaUnderRocCurve value)
        {
            requireNonNull(value, "value is null");

            GroupedAreaUnderRocCurve previous = get();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            auc = value;
            size += value.estimatedInMemorySize();
        }

        @Override
        public void deserialize(Block serialized)
        {
            this.auc = new GroupedAreaUnderRocCurve(getGroupId(), serialized);
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            size += memory;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + auc.estimatedInMemorySize();
        }
    }

    public static class SingleState
            implements AreaUnderRocCurveState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleState.class).instanceSize();
        private GroupedAreaUnderRocCurve auc;

        public SingleState()
        {
            auc = new GroupedAreaUnderRocCurve();

            // set synthetic, unique group id to use GroupAreaUnderRocCurve from the single state
            auc.setGroupId(0);
        }

        @Override
        public GroupedAreaUnderRocCurve get()
        {
            return auc;
        }

        @Override
        public void set(GroupedAreaUnderRocCurve value)
        {
            auc = value;
        }

        @Override
        public void deserialize(Block serialized)
        {
            auc = new GroupedAreaUnderRocCurve(0, serialized);
        }

        @Override
        public void addMemoryUsage(long memory)
        {
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (auc != null) {
                estimatedSize += auc.estimatedInMemorySize();
            }
            return estimatedSize;
        }
    }
}
