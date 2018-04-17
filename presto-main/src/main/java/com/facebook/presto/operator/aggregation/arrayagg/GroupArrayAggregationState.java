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
import com.facebook.presto.array.ShortBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.Type;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Verify.verify;

/**
 * state object that uses a single BlockBuilder for all groups.
 */
public class GroupArrayAggregationState
        extends AbstractGroupedAccumulatorState
        implements ArrayAggregationState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupArrayAggregationState.class).instanceSize();
    private static final int MAX_BLOCK_SIZE = 1024 * 1024;
    private static final int MAX_NUM_BLOCKS = 30000;
    private static final short NULL = -1;

    private final Type type;

    private final ShortBigArray headBlockIndex;
    private final IntBigArray headPosition;

    private final ShortBigArray nextBlockIndex;
    private final IntBigArray nextPosition;

    private final ShortBigArray tailBlockIndex;
    private final IntBigArray tailPosition;

    private final List<BlockBuilder> values;
    private final LongList sumPositions;
    private BlockBuilder currentBlockBuilder;
    private PageBuilderStatus pageBuilderStatus;

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
        this.tailBlockIndex = new ShortBigArray(NULL);
        this.tailPosition = new IntBigArray(NULL);

        this.pageBuilderStatus = new PageBuilderStatus(MAX_BLOCK_SIZE);
        this.currentBlockBuilder = type.createBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), 16);
        this.values = new ArrayList<>();
        this.sumPositions = new LongArrayList();
        values.add(currentBlockBuilder);
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
        tailBlockIndex.ensureCapacity(size);
        tailPosition.ensureCapacity(size);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                headBlockIndex.sizeOf() +
                headPosition.sizeOf() +
                tailBlockIndex.sizeOf() +
                tailPosition.sizeOf() +
                nextBlockIndex.sizeOf() +
                nextPosition.sizeOf() +
                valueBlocksRetainedSizeInBytes +
                // valueBlocksRetainedSizeInBytes doesn't contain the current block builder
                currentBlockBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void add(Block block, int position)
    {
        long currentGroupId = getGroupId();
        short insertedBlockIndex = (short) (values.size() - 1);
        int insertedPosition = currentBlockBuilder.getPositionCount();

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
            long absoluteTailAddress = toAbsolutePosition(tailBlockIndex.get(currentGroupId), tailPosition.get(currentGroupId));
            nextBlockIndex.set(absoluteTailAddress, insertedBlockIndex);
            nextPosition.set(absoluteTailAddress, insertedPosition);
        }
        tailBlockIndex.set(currentGroupId, insertedBlockIndex);
        tailPosition.set(currentGroupId, insertedPosition);

        type.appendTo(block, position, currentBlockBuilder);
        totalPositions++;

        if (pageBuilderStatus.isFull()) {
            valueBlocksRetainedSizeInBytes += currentBlockBuilder.getRetainedSizeInBytes();
            sumPositions.add(totalPositions);
            pageBuilderStatus = new PageBuilderStatus(MAX_BLOCK_SIZE);
            currentBlockBuilder = currentBlockBuilder.newBlockBuilderLike(pageBuilderStatus.createBlockBuilderStatus());
            values.add(currentBlockBuilder);

            verify(values.size() <= MAX_NUM_BLOCKS);
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

    private long toAbsolutePosition(short blockId, int position)
    {
        return sumPositions.get(blockId) + position;
    }
}
