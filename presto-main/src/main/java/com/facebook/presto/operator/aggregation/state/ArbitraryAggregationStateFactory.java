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

public class ArbitraryAggregationStateFactory
        implements AccumulatorStateFactory<ArbitraryAggregationState>
{
    private final Type valueType;

    public ArbitraryAggregationStateFactory(Type valueType)
    {
        this.valueType = valueType;
    }

    @Override
    public ArbitraryAggregationState createSingleState()
    {
        return new SingleArbitraryAggregationState(valueType);
    }

    @Override
    public Class<? extends ArbitraryAggregationState> getSingleStateClass()
    {
        return SingleArbitraryAggregationState.class;
    }

    @Override
    public ArbitraryAggregationState createGroupedState()
    {
        return new GroupedArbitraryAggregationState(valueType);
    }

    @Override
    public Class<? extends ArbitraryAggregationState> getGroupedStateClass()
    {
        return GroupedArbitraryAggregationState.class;
    }

    public static class GroupedArbitraryAggregationState
            extends AbstractGroupedAccumulatorState
            implements ArbitraryAggregationState
    {
        private final Type valueType;
        private final BlockBigArray values = new BlockBigArray();

        public GroupedArbitraryAggregationState(Type valueType)
        {
            this.valueType = valueType;
        }

        @Override
        public void ensureCapacity(long size)
        {
            values.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return values.sizeOf();
        }

        @Override
        public Type getType()
        {
            return valueType;
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

    public static class SingleArbitraryAggregationState
            implements ArbitraryAggregationState
    {
        private final Type valueType;
        private Block value;

        public SingleArbitraryAggregationState(Type valueType)
        {
            this.valueType = valueType;
        }

        @Override
        public long getEstimatedSize()
        {
            if (value != null) {
                return (long) value.getRetainedSizeInBytes();
            }
            else {
                return 0L;
            }
        }

        @Override
        public Type getType()
        {
            return valueType;
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
