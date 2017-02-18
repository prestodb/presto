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
import com.facebook.presto.operator.aggregation.MultiKeyValuePairs;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.type.Type;

import static java.util.Objects.requireNonNull;

public class MultiKeyValuePairsStateFactory
        implements AccumulatorStateFactory<MultiKeyValuePairsState>
{
    private final Type keyType;
    private final Type valueType;

    public MultiKeyValuePairsStateFactory(Type keyType, Type valueType)
    {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public MultiKeyValuePairsState createSingleState()
    {
        return new SingleState(keyType, valueType);
    }

    @Override
    public Class<? extends MultiKeyValuePairsState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public MultiKeyValuePairsState createGroupedState()
    {
        return new GroupedState(keyType, valueType);
    }

    @Override
    public Class<? extends MultiKeyValuePairsState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements MultiKeyValuePairsState
    {
        private final Type keyType;
        private final Type valueType;
        private final ObjectBigArray<MultiKeyValuePairs> pairs = new ObjectBigArray<>();
        private long size;

        public GroupedState(Type keyType, Type valueType)
        {
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public void ensureCapacity(long size)
        {
            pairs.ensureCapacity(size);
        }

        @Override
        public MultiKeyValuePairs get()
        {
            return pairs.get(getGroupId());
        }

        @Override
        public void set(MultiKeyValuePairs value)
        {
            requireNonNull(value, "value is null");

            MultiKeyValuePairs previous = get();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            pairs.set(getGroupId(), value);
            size += value.estimatedInMemorySize();
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            size += memory;
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
        public long getEstimatedSize()
        {
            return size + pairs.sizeOf();
        }
    }

    public static class SingleState
            implements MultiKeyValuePairsState
    {
        private final Type keyType;
        private final Type valueType;
        private MultiKeyValuePairs pair;

        public SingleState(Type keyType, Type valueType)
        {
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public MultiKeyValuePairs get()
        {
            return pair;
        }

        @Override
        public void set(MultiKeyValuePairs value)
        {
            pair = value;
        }

        @Override
        public void addMemoryUsage(long memory)
        {
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
        public long getEstimatedSize()
        {
            if (pair == null) {
                return 0;
            }
            return pair.estimatedInMemorySize();
        }
    }
}
