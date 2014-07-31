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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.array.BlockBigArray;

public class MaxByStateFactory
        implements AccumulatorStateFactory<MaxByState>
{
    private final Type valueType;
    private final Type keyType;

    public MaxByStateFactory(Type valueType, Type keyType)
    {
        this.valueType = valueType;
        this.keyType = keyType;
    }

    @Override
    public MaxByState createSingleState()
    {
        return new SingleMaxByState(keyType, valueType);
    }

    @Override
    public Class<? extends MaxByState> getSingleStateClass()
    {
        return SingleMaxByState.class;
    }

    @Override
    public MaxByState createGroupedState()
    {
        return new GroupedMaxByState(keyType, valueType);
    }

    @Override
    public Class<? extends MaxByState> getGroupedStateClass()
    {
        return GroupedMaxByState.class;
    }

    public static class GroupedMaxByState
            extends AbstractGroupedAccumulatorState
            implements MaxByState
    {
        private final Type keyType;
        private final Type valueType;
        private final BlockBigArray keys = new BlockBigArray();
        private final BlockBigArray values = new BlockBigArray();

        public GroupedMaxByState(Type keyType, Type valueType)
        {
            this.keyType = keyType;
            this.valueType = valueType;
        }

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
        public Type getKeyType()
        {
            return keyType;
        }

        @Override
        public Type getValueType()
        {
            return valueType;
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

    public static class SingleMaxByState
            implements MaxByState
    {
        private final Type keyType;
        private final Type valueType;
        private Block key;
        private Block value;

        public SingleMaxByState(Type keyType, Type valueType)
        {
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public long getEstimatedSize()
        {
            long size = 0;
            if (key != null) {
                size += key.getSizeInBytes();
            }
            if (value != null) {
                size += value.getSizeInBytes();
            }
            return size;
        }

        @Override
        public Type getKeyType()
        {
            return keyType;
        }

        @Override
        public Type getValueType()
        {
            return valueType;
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
