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

package com.facebook.presto.type.khyperloglog;

import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.GroupedAccumulatorState;
import org.openjdk.jol.info.ClassLayout;

public class KHyperLogLogStateFactory
        implements AccumulatorStateFactory<KHyperLogLogState>
{
    private static final int SIZE_OF_SINGLE = ClassLayout.parseClass(SingleKHyperLogLogState.class).instanceSize();
    private static final int SIZE_OF_GROUPED = ClassLayout.parseClass(GroupedKHyperLogLogState.class).instanceSize();

    @Override
    public KHyperLogLogState createSingleState()
    {
        return new SingleKHyperLogLogState();
    }

    @Override
    public Class<? extends KHyperLogLogState> getSingleStateClass()
    {
        return SingleKHyperLogLogState.class;
    }

    @Override
    public KHyperLogLogState createGroupedState()
    {
        return new GroupedKHyperLogLogState();
    }

    @Override
    public Class<? extends KHyperLogLogState> getGroupedStateClass()
    {
        return GroupedKHyperLogLogState.class;
    }

    public static class GroupedKHyperLogLogState
            implements GroupedAccumulatorState, KHyperLogLogState
    {
        private final ObjectBigArray<KHyperLogLog> khlls = new ObjectBigArray<>();
        private long groupId;
        private long size;

        @Override
        public void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(long size)
        {
            khlls.ensureCapacity(size);
        }

        @Override
        public KHyperLogLog getKHLL()
        {
            return khlls.get(groupId);
        }

        @Override
        public void setKHLL(KHyperLogLog value)
        {
            if (getKHLL() != null) {
                size -= getKHLL().estimatedInMemorySize();
            }
            size += value.estimatedInMemorySize();
            khlls.set(groupId, value);
        }

        @Override
        public long getEstimatedSize()
        {
            return SIZE_OF_GROUPED + size + khlls.sizeOf();
        }
    }

    public static class SingleKHyperLogLogState
            implements KHyperLogLogState
    {
        private KHyperLogLog khll;

        @Override
        public KHyperLogLog getKHLL()
        {
            return khll;
        }

        @Override
        public void setKHLL(KHyperLogLog value)
        {
            this.khll = value;
        }

        @Override
        public long getEstimatedSize()
        {
            if (khll == null) {
                return SIZE_OF_SINGLE;
            }
            return SIZE_OF_SINGLE + khll.estimatedInMemorySize();
        }
    }
}
