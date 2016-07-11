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

import com.facebook.presto.array.BlockBigArray;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.AccumulatorStateFactory;

public class MaxOrMinByStateFactory
        implements AccumulatorStateFactory<MaxOrMinByState>
{
    @Override
    public MaxOrMinByState createSingleState()
    {
        return new SingleMaxOrMinByState();
    }

    @Override
    public Class<? extends MaxOrMinByState> getSingleStateClass()
    {
        return SingleMaxOrMinByState.class;
    }

    @Override
    public MaxOrMinByState createGroupedState()
    {
        return new GroupedMaxOrMinByState();
    }

    @Override
    public Class<? extends MaxOrMinByState> getGroupedStateClass()
    {
        return GroupedMaxOrMinByState.class;
    }

    public static class GroupedMaxOrMinByState
            extends AbstractGroupedAccumulatorState
            implements MaxOrMinByState
    {
        private final BlockBigArray keys = new BlockBigArray();
        private final BlockBigArray values = new BlockBigArray();

        @Override
        public void ensureCapacity(long size)
        {
            keys.ensureCapacity(size);
            values.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return keys.sizeOf() + values.sizeOf();
        }

        @Override
        public Block getKey()
        {
            return keys.get(getGroupId());
        }

        @Override
        public void setKey(Block key)
        {
            keys.set(getGroupId(), key);
        }

        @Override
        public Block getValue()
        {
            return values.get(getGroupId());
        }

        @Override
        public void setValue(Block value)
        {
            values.set(getGroupId(), value);
        }
    }

    public static class SingleMaxOrMinByState
            implements MaxOrMinByState
    {
        private Block key;
        private Block value;

        @Override
        public long getEstimatedSize()
        {
            long size = 0;
            if (key != null) {
                size += key.getRetainedSizeInBytes();
            }
            if (value != null) {
                size += value.getRetainedSizeInBytes();
            }
            return size;
        }

        @Override
        public Block getKey()
        {
            return key;
        }

        @Override
        public void setKey(Block key)
        {
            this.key = key;
        }

        @Override
        public Block getValue()
        {
            return value;
        }

        @Override
        public void setValue(Block value)
        {
            this.value = value;
        }
    }
}
