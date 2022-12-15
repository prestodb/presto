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

import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.SetOfValues;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import org.openjdk.jol.info.ClassLayout;

public class SetAggregationStateFactory
        implements AccumulatorStateFactory
{
    private final Type elementType;

    public SetAggregationStateFactory(Type elementType)
    {
        this.elementType = elementType;
    }

    @Override
    public SetAggregationState createSingleState()
    {
        return new SingleState(elementType);
    }

    @Override
    public Class<? extends SetAggregationState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public SetAggregationState createGroupedState()
    {
        return new GroupedState(elementType);
    }

    @Override
    public Class<? extends SetAggregationState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements SetAggregationState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedState.class).instanceSize();
        private final Type elementType;
        private long size;
        private final SetOfValues valueSetForAllGroups;
        private final ObjectBigArray<IntArraySet> positionSets = new ObjectBigArray<>();

        public GroupedState(Type elementType)
        {
            this.elementType = elementType;
            valueSetForAllGroups = new SetOfValues(elementType);
        }

        @Override
        public void ensureCapacity(long size)
        {
        }

        @Override
        public SetOfValues get()
        {
            IntArraySet positionSet = positionSets.get(getGroupId());
            if (positionSet == null) {
                return null;
            }

            SetOfValues setOfValues = new SetOfValues(elementType);
            for (int i : positionSet) {
                setOfValues.add(valueSetForAllGroups.getvalues(), i);
            }

            return setOfValues;
        }

        @Override
        public void set(SetOfValues setOfValues)
        {
            Block valuesBlock = setOfValues.getvalues();
            IntArraySet positionSet = positionSets.get(getGroupId());
            size -= positionSet.size() * 4;
            for (int i = 0; i < valuesBlock.getPositionCount(); i++) {
                positionSet.add(valueSetForAllGroups.add(valuesBlock, i));
            }
            size += positionSet.size() * 4;
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            size += memory;
        }

        @Override
        public void add(Block block, int position)
        {
            positionSets.ensureCapacity(getGroupId());
            IntArraySet positionSet = positionSets.get(getGroupId());
            if (positionSet == null) {
                positionSet = new IntArraySet(5);
                positionSets.set(getGroupId(), positionSet);
            }
            else {
                size -= positionSet.size() * 4;
            }

            positionSet.add(valueSetForAllGroups.add(block, position));
            size += positionSet.size() * 4;
        }

        @Override
        public Type getElementType()
        {
            return elementType;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + valueSetForAllGroups.estimatedInMemorySize() + size;
        }
    }

    public static class SingleState
            implements SetAggregationState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleState.class).instanceSize();
        private final Type elementType;
        private SetOfValues set;

        public SingleState(Type elementType)
        {
            this.elementType = elementType;
        }

        @Override
        public SetOfValues get()
        {
            return set;
        }

        @Override
        public void set(SetOfValues set)
        {
            this.set = set;
        }

        @Override
        public void addMemoryUsage(long memory)
        {
        }

        @Override
        public Type getElementType()
        {
            return elementType;
        }

        @Override
        public void add(Block block, int position)
        {
            if (set == null) {
                set = new SetOfValues(elementType);
            }

            set.add(block, position);
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (set != null) {
                estimatedSize += set.estimatedInMemorySize();
            }
            return estimatedSize;
        }
    }
}
