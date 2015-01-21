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

import java.util.ArrayList;

import com.facebook.presto.type.ArrayType;
import com.facebook.presto.util.array.ObjectBigArray;

public class ArrayAggregationStateFactory
        implements AccumulatorStateFactory<ArrayAggregationState>
{
    private final ArrayType arrayType;

    public ArrayAggregationStateFactory(ArrayType arrayType)
    {
        this.arrayType = arrayType;
    }

    @Override
    public ArrayAggregationState createSingleState()
    {
        return new SingleArrayAggregationState(arrayType);
    }

    @Override
    public Class<? extends ArrayAggregationState> getSingleStateClass()
    {
        return SingleArrayAggregationState.class;
    }

    @Override
    public ArrayAggregationState createGroupedState()
    {
        return new GroupedArrayAggregationState(arrayType);
    }

    @Override
    public Class<? extends ArrayAggregationState> getGroupedStateClass()
    {
        return GroupedArrayAggregationState.class;
    }

    public static class GroupedArrayAggregationState
            extends AbstractGroupedAccumulatorState
            implements ArrayAggregationState
    {
        private final ArrayType arrayType;
        private final ObjectBigArray<ArrayList<Object>> groupValues = new ObjectBigArray<ArrayList<Object>>();

        public GroupedArrayAggregationState(ArrayType arrayType)
        {
            this.arrayType = arrayType;
        }

        @Override
        public void ensureCapacity(long size)
        {
            groupValues.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return groupValues.sizeOf();
        }

        @Override
        public ArrayType getArrayType()
        {
            return arrayType;
        }

        @Override
        public ArrayList<Object> getArray()
        {
            return groupValues.get(getGroupId());
        }

        @Override
        public void setArray(ArrayList<Object> value)
        {
            groupValues.set(getGroupId(), value);
        }
    }

    public static class SingleArrayAggregationState
            implements ArrayAggregationState
    {
        private final ArrayType arrayType;
        private ArrayList<Object> value;

        public SingleArrayAggregationState(ArrayType arrayType)
        {
            this.arrayType = arrayType;
        }

        @Override
        public long getEstimatedSize()
        {
            if (value != null) {
                return value.size();
            }
            else {
                return 0L;
            }
        }

        @Override
        public ArrayType getArrayType()
        {
            return arrayType;
        }

        @Override
        public ArrayList<Object> getArray()
        {
            return value;
        }

        @Override
        public void setArray(ArrayList<Object> value)
        {
            this.value = value;
        }
    }
}
