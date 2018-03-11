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
import com.facebook.presto.array.LongBigArray;
import com.facebook.presto.array.ShortBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

/**
 * state object that uses a single BlockBuilder for all groups.
 */
public class GroupArrayAggregationState
        extends AbstractGroupedAccumulatorState
        implements ArrayAggregationState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupArrayAggregationState.class).instanceSize();
    private static final short NULL = -1;

    private final Type type;

    private final ShortBigArray headBlockIndex;
    private final IntBigArray headPosition;

    private final ShortBigArray nextBlockIndex;
    private final IntBigArray nextPosition;

    private final LongBigArray tailAbsoluteAddress;

    private final List<BlockBuilder> values;
    private final LongList sumPositions;
    private final PageBuilder pageBuilder;

    private long valueBlocksRetainedSizeInBytes;
    private long totalPositions;
    private long capacity;

    public GroupArrayAggregationState(Type type)
    {
        this.type = type;
        this.headBlockIndex = new ShortBigArray(NULL);
        this.headPosition = new IntBigArray(NULL);
        this.nextBlockIndex = new ShortBigArray(NULL);
        this.nextPosition = new IntBigArray(NULL);
        this.tailAbsoluteAddress = new LongBigArray(NULL);

        this.pageBuilder = new PageBuilder(ImmutableList.of(type));
        this.values = new ArrayList<>();
        this.sumPositions = new LongArrayList();
        values.add(getCurrentBlockBuilder());
        sumPositions.add(0L);
        valueBlocksRetainedSizeInBytes = 0;

        totalPositions = 0;
        capacity = 1024;
        nextBlockIndex.ensureCapacity(capacity);
        nextPosition.ensureCapacity(capacity);
    }

    @Override
    public void ensureCapacity(long size)
    {
        headBlockIndex.ensureCapacity(size);
        headPosition.ensureCapacity(size);
        tailAbsoluteAddress.ensureCapacity(size);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                headBlockIndex.sizeOf() +
                headPosition.sizeOf() +
                tailAbsoluteAddress.sizeOf() +
                nextBlockIndex.sizeOf() +
                nextPosition.sizeOf() +
                valueBlocksRetainedSizeInBytes +
                // valueBlocksRetainedSizeInBytes doesn't contain the current block builder
                getCurrentBlockBuilder().getRetainedSizeInBytes();
    }

    @Override
    public void add(Block block, int position)
    {
        long currentGroupId = getGroupId();
        short insertedBlockIndex = (short) (values.size() - 1);
        int insertedPosition = getCurrentBlockBuilder().getPositionCount();

        if (totalPositions == capacity) {
            capacity *= 1.5;
            nextBlockIndex.ensureCapacity(capacity);
            nextPosition.ensureCapacity(capacity);
        }

        if (isEmpty()) {
            // new linked list, set up the header pointer
            headBlockIndex.set(currentGroupId, insertedBlockIndex);
            headPosition.set(currentGroupId, insertedPosition);
        }
        else {
            // existing linked list, link the new entry to the tail
            long absoluteTailAddress = tailAbsoluteAddress.get(currentGroupId);
            nextBlockIndex.set(absoluteTailAddress, insertedBlockIndex);
            nextPosition.set(absoluteTailAddress, insertedPosition);
        }
        tailAbsoluteAddress.set(currentGroupId, totalPositions);

        type.appendTo(block, position, getCurrentBlockBuilder());
        pageBuilder.declarePosition();
        totalPositions++;

        if (pageBuilder.isFull()) {
            valueBlocksRetainedSizeInBytes += getCurrentBlockBuilder().getRetainedSizeInBytes();
            sumPositions.add(sumPositions.get(sumPositions.size() - 1) + getCurrentBlockBuilder().getPositionCount());

            pageBuilder.reset();
            values.add(getCurrentBlockBuilder());
        }
    }

    @Override
    public void forEach(ArrayAggregationStateConsumer consumer)
    {
        short currentBlockId = headBlockIndex.get(getGroupId());
        int currentPosition = headPosition.get(getGroupId());
        while (currentBlockId != NULL) {
            consumer.accept(values.get(currentBlockId), currentPosition);

            long absoluteCurrentAddress = toAbsolutePosition(currentBlockId, currentPosition);
            currentBlockId = nextBlockIndex.get(absoluteCurrentAddress);
            currentPosition = nextPosition.get(absoluteCurrentAddress);
        }
    }

    @Override
    public boolean isEmpty()
    {
        return headBlockIndex.get(getGroupId()) == NULL;
    }

    private BlockBuilder getCurrentBlockBuilder()
    {
        return pageBuilder.getBlockBuilder(0);
    }

    private long toAbsolutePosition(short blockId, int position)
    {
        return sumPositions.get(blockId) + position;
    }
}
