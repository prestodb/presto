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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TypedMultiChannelHeap
{
    private static final int COMPACT_THRESHOLD_BYTES = 32768;
    private static final int COMPACT_THRESHOLD_RATIO = 3; // when 2/3 of elements in keyBlockBuilder is unreferenced, do compact

    private final int capacity;

    private int positionCount;
    private final int[] heapIndex;
    private BlockBuilder[] channelBlockBuilders;

    private final List<Type> types;
    private final List<Type> sortTypes;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;

    public TypedMultiChannelHeap(List<Type> types, List<Integer> sortChannels, List<SortOrder> sortOrders, int capacity)
    {
        this.types = requireNonNull(types, "types is null");
        checkArgument(!types.isEmpty(), "input types is empty");
        this.sortChannels = requireNonNull(sortChannels, "sortChannels is null");
        ImmutableList.Builder<Type> sortTypesBuilder = ImmutableList.builder();
        for (int channel : sortChannels) {
            sortTypesBuilder.add(types.get(channel));
        }
        this.sortTypes = sortTypesBuilder.build();
        this.sortOrders = requireNonNull(sortOrders, "sortOrders is null");
        this.capacity = capacity;
        this.heapIndex = new int[capacity];

        // TODO estimate the size of this array
        this.channelBlockBuilders = new BlockBuilder[types.size()];
        for (int i = 0; i < types.size(); i++) {
            this.channelBlockBuilders[i] = types.get(i).createBlockBuilder(new BlockBuilderStatus(), capacity);
        }
    }

    /*
    public static Type getSerializedType(Type keyType, Type valueType)
    {
        return new RowType(ImmutableList.of(BIGINT, new ArrayType(keyType), new ArrayType(valueType)), Optional.empty());
    }
    */

    public int getCapacity()
    {
        return capacity;
    }

    public long getEstimatedSize()
    {
        long totalSize = capacity * Integer.BYTES;
        for (BlockBuilder blockBuilder : channelBlockBuilders) {
            totalSize += blockBuilder.getRetainedSizeInBytes();
        }
        return totalSize;
    }

    public boolean isEmpty()
    {
        return positionCount == 0;
    }

    public void popAll(BlockBuilder[] resultBlockBuilders)
    {
        while (positionCount > 0) {
            pop(resultBlockBuilders);
        }
    }

    public Page popPage(PageBuilder pageBuilder)
    {
        while (!pageBuilder.isFull() && !isEmpty()) {
            for (int i = 0; i < types.size(); i++) {
                Type type = types.get(i);
                type.appendTo(channelBlockBuilders[i], heapIndex[0], pageBuilder.getBlockBuilder(i));
            }
        }
        return pageBuilder.build();
    }

    public void pop(BlockBuilder[] resultBlockBuilders)
    {
        for (int i = 0; i < resultBlockBuilders.length; i++) {
            types.get(i).appendTo(channelBlockBuilders[i], heapIndex[0], resultBlockBuilders[i]);
        }
        remove();
    }

    private void remove()
    {
        positionCount--;
        heapIndex[0] = heapIndex[positionCount];
        siftDown();
    }

    private static void appendTo(List<Type> types, Block[] channelBlocks, int position, BlockBuilder[] blockBuilders)
    {
        for (int i = 0; i < channelBlocks.length; i++) {
            types.get(i).appendTo(channelBlocks[i], position, blockBuilders[i]);
        }
    }

    public void add(Block[] channelBlocks, int position)
    {
        if (positionCount == capacity) {
            if (compareTo(this.channelBlockBuilders, heapIndex[0], channelBlocks, position) >= 0) {
                return; // and new element is not larger than heap top: do not add
            }
            heapIndex[0] = channelBlockBuilders[0].getPositionCount();
            appendTo(types, channelBlocks, position, channelBlockBuilders);
            siftDown();
        }
        else {
            heapIndex[positionCount] = channelBlockBuilders[0].getPositionCount();
            positionCount++;
            appendTo(types, channelBlocks, position, channelBlockBuilders);
            siftUp();
        }
        compactIfNecessary(false);
    }

    private int compareTo(Block[] leftBlocks, int leftIndex, Block[] rightBlocks, int rightIndex)
    {
        for (int i = 0; i < sortChannels.size(); i++) {
            Type type = sortTypes.get(i);
            int sortChannel = sortChannels.get(i);
            SortOrder sortOrder = sortOrders.get(i);

            Block leftBlock = leftBlocks[sortChannel];
            Block rightBlock = rightBlocks[sortChannel];

            int compare = -sortOrder.compareBlockValue(type, leftBlock, leftIndex, rightBlock, rightIndex);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    public void addPage(Page page)
    {
        Block[] blocks = page.getBlocks();
        for (int position = 0; position < page.getPositionCount(); position++) {
            add(blocks, position);
        }
    }

    public void addAll(TypedMultiChannelHeap otherHeap)
    {
        addAll(otherHeap.channelBlockBuilders);
    }

    public void addAll(Block[] channelBlocks)
    {
        for (int i = 0; i < channelBlocks[0].getPositionCount(); i++) {
            add(channelBlocks, i);
        }
    }

    private void siftDown()
    {
        int position = 0;
        while (true) {
            int leftPosition = position * 2 + 1;
            if (leftPosition >= positionCount) {
                break;
            }
            int rightPosition = leftPosition + 1;
            int smallerChildPosition;
            if (rightPosition >= positionCount) {
                smallerChildPosition = leftPosition;
            }
            else {
                smallerChildPosition = compareTo(channelBlockBuilders, heapIndex[leftPosition], channelBlockBuilders, heapIndex[rightPosition]) >= 0 ? rightPosition : leftPosition;
            }
            if (compareTo(channelBlockBuilders, heapIndex[smallerChildPosition], channelBlockBuilders, heapIndex[position]) >= 0) {
                break; // child is larger or equal
            }
            int swapTemp = heapIndex[position];
            heapIndex[position] = heapIndex[smallerChildPosition];
            heapIndex[smallerChildPosition] = swapTemp;
            position = smallerChildPosition;
        }
    }

    private void siftUp()
    {
        int position = positionCount - 1;
        while (position != 0) {
            int parentPosition = (position - 1) / 2;
            if (compareTo(channelBlockBuilders, heapIndex[position], channelBlockBuilders, heapIndex[parentPosition]) >= 0) {
                break; // child is larger or equal
            }
            int swapTemp = heapIndex[position];
            heapIndex[position] = heapIndex[parentPosition];
            heapIndex[parentPosition] = swapTemp;
            position = parentPosition;
        }
    }

    private void compactIfNecessary(boolean force)
    {
        // Byte size check is needed. Otherwise, if size * 3 is small, BlockBuilder can be reallocate too often.
        // Position count is needed. Otherwise, for large elements, heap will be compacted every time.
        // Size instead of retained size is needed because default allocation size can be huge for some block builders. And the first check will become useless in such case.
        if (!force && (channelBlockBuilders[0].getSizeInBytes() < COMPACT_THRESHOLD_BYTES || channelBlockBuilders[0].getPositionCount() / positionCount < COMPACT_THRESHOLD_RATIO)) {
            return;
        }
        BlockBuilder[] newHeapBlockBuilders = new BlockBuilder[types.size()];
        for (int i = 0; i < newHeapBlockBuilders.length; i++) {
            newHeapBlockBuilders[i] = types.get(i).createBlockBuilder(new BlockBuilderStatus(), !force ? channelBlockBuilders[i].getPositionCount() : positionCount);
        }
        for (int i = 0; i < positionCount; i++) {
            for (int channel = 0; channel < channelBlockBuilders.length; channel++) {
                types.get(channel).appendTo(channelBlockBuilders[channel], heapIndex[i], newHeapBlockBuilders[channel]);
            }
            heapIndex[i] = i;
        }
        channelBlockBuilders = newHeapBlockBuilders;
    }

    public TypedMultiChannelHeapPageFiller build()
    {
        compactIfNecessary(true);
        return new TypedMultiChannelHeapPageFiller();
    }

    public class TypedMultiChannelHeapPageFiller
    {
        private Iterator<Integer> iterator;

        public TypedMultiChannelHeapPageFiller()
        {
            ImmutableList.Builder<Integer> indexListBuilder = ImmutableList.builder();
            while (!TypedMultiChannelHeap.this.isEmpty()) {
                indexListBuilder.add(heapIndex[0]);
                TypedMultiChannelHeap.this.remove();
            }
            iterator = indexListBuilder.build().reverse().iterator();
        }

        public void fillPage(PageBuilder pageBuilder)
        {
            while (!pageBuilder.isFull() && iterator.hasNext()) {
                int index = iterator.next();
                pageBuilder.declarePosition();
                for (int i = 0; i < types.size(); i++) {
                    Type type = types.get(i);
                    type.appendTo(channelBlockBuilders[i], index, pageBuilder.getBlockBuilder(i));
                }
            }
        }

        public boolean isEmpty()
        {
            return !iterator.hasNext();
        }
    }
}
