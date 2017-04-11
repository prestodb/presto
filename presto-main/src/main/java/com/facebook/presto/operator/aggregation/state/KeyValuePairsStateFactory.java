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
import com.facebook.presto.operator.aggregation.KeyValuePairs;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.type.Type;

import static java.util.Objects.requireNonNull;

public class KeyValuePairsStateFactory
        implements AccumulatorStateFactory<KeyValuePairsState>
{
    private final Type keyType;
    private final Type valueType;

    public KeyValuePairsStateFactory(Type keyType, Type valueType)
    {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public KeyValuePairsState createSingleState()
    {
        return new SingleState(keyType, valueType);
    }

    @Override
    public Class<? extends KeyValuePairsState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public KeyValuePairsState createGroupedState()
    {
        return new GroupedState(keyType, valueType);
    }

    @Override
    public Class<? extends KeyValuePairsState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements KeyValuePairsState
    {
        private final Type keyType;
        private final Type valueType;
        private final ObjectBigArray<KeyValuePairs> pairs = new ObjectBigArray<>();
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
        public KeyValuePairs get()
        {
            return pairs.get(getGroupId());
        }

        @Override
        public void set(KeyValuePairs value)
        {
            requireNonNull(value, "value is null");

            KeyValuePairs previous = get();
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
            implements KeyValuePairsState
    {
        private final Type keyType;
        private final Type valueType;
        private KeyValuePairs pair;

        public SingleState(Type keyType, Type valueType)
        {
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public KeyValuePairs get()
        {
            return pair;
        }

        @Override
        public void set(KeyValuePairs value)
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
