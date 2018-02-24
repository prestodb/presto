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
package com.facebook.presto.operator.aggregation.arrayagg;

import com.facebook.presto.array.IntBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

/**
 * state object that uses a single BlockBuilder for all groups.
 */
public class GroupArrayAggregationState
        extends AbstractGroupedAccumulatorState
        implements ArrayAggregationState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupArrayAggregationState.class).instanceSize();
    private static final int NULL = -1;
    private static final int EXPECTED_VALUE_SIZE = 100;

    private final Type type;

    private IntBigArray headPointers;
    private IntBigArray tailPointers;
    private IntBigArray nextPointers;
    private BlockBuilder values;
    private long capacity;

    public GroupArrayAggregationState(Type type)
    {
        this.type = type;
        this.headPointers = new IntBigArray(NULL);
        this.tailPointers = new IntBigArray(NULL);
        this.nextPointers = new IntBigArray(NULL);
        this.values = type.createBlockBuilder(null, EXPECTED_VALUE_SIZE);

        capacity = 1024;
        nextPointers.ensureCapacity(capacity);
    }

    @Override
    public void ensureCapacity(long size)
    {
        headPointers.ensureCapacity(size);
        tailPointers.ensureCapacity(size);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                headPointers.sizeOf() +
                tailPointers.sizeOf() +
                nextPointers.sizeOf() +
                values.getRetainedSizeInBytes();
    }

    @Override
    public void add(Block block, int position)
    {
        long currentGroupId = getGroupId();
        int newPosition = values.getPositionCount();

        if (newPosition == capacity) {
            capacity *= 1.5;
            nextPointers.ensureCapacity(capacity);
        }

        if (headPointers.get(currentGroupId) == NULL) {
            // new linked list, set up the header pointer
            headPointers.set(currentGroupId, newPosition);
        }
        else {
            // existing linked list, link the new entry to the tail
            nextPointers.set(tailPointers.get(currentGroupId), newPosition);
        }
        tailPointers.set(currentGroupId, newPosition);

        type.appendTo(block, position, values);
    }

    @Override
    public void forEach(ArrayAggregationStateConsumer consumer)
    {
        int currentPosition = headPointers.get(getGroupId());
        while (currentPosition != NULL) {
            consumer.accept(values, currentPosition);
            currentPosition = nextPointers.get(currentPosition);
        }
    }

    @Override
    public boolean isEmpty()
    {
        return headPointers.get(getGroupId()) == NULL;
    }

    @Override
    public void reset()
    {
        this.headPointers = new IntBigArray(NULL);
        this.tailPointers = new IntBigArray(NULL);
        this.nextPointers = new IntBigArray(NULL);
        this.values = type.createBlockBuilder(null, EXPECTED_VALUE_SIZE);

        capacity = 1024;
        nextPointers.ensureCapacity(capacity);
    }
}
