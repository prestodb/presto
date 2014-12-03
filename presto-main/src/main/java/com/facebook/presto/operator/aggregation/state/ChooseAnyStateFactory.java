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

public class ChooseAnyStateFactory
        implements AccumulatorStateFactory<ChooseAnyState>
{
    private final Type valueType;

    public ChooseAnyStateFactory(Type valueType)
    {
        this.valueType = valueType;
    }

    @Override
    public ChooseAnyState createSingleState()
    {
        return new SingleChooseAnyState(valueType);
    }

    @Override
    public Class<? extends ChooseAnyState> getSingleStateClass()
    {
        return SingleChooseAnyState.class;
    }

    @Override
    public ChooseAnyState createGroupedState()
    {
        return new GroupedChooseAnyState(valueType);
    }

    @Override
    public Class<? extends ChooseAnyState> getGroupedStateClass()
    {
        return GroupedChooseAnyState.class;
    }

    public static class GroupedChooseAnyState
            extends AbstractGroupedAccumulatorState
            implements ChooseAnyState
    {
        private final Type valueType;
        private final BlockBigArray values = new BlockBigArray();

        public GroupedChooseAnyState(Type valueType)
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

    public static class SingleChooseAnyState
            implements ChooseAnyState
    {
        private final Type valueType;
        private Block value;

        public SingleChooseAnyState(Type valueType)
        {
            this.valueType = valueType;
        }

        @Override
        public long getEstimatedSize()
        {
            if (value != null) {
                return (long) value.getSizeInBytes();
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
