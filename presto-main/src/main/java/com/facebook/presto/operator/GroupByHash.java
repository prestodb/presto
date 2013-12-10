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
package com.facebook.presto.operator;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.RandomAccessBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Murmur3;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;

// This implementation assumes arrays used in the hash are always a power of 2
public class GroupByHash
{
    private static final float FILL_RATIO = 0.75f;
    private final List<Type> types;
    private final int[] channels;

    private final List<PageBuilder> pages;

    private long completedPagesMemorySize;

    private int maxFill;
    private int mask;
    private long[] key;
    private int[] value;

    private final LongBigArray groupAddress;

    private int nextGroupId;

    public GroupByHash(List<Type> types, int[] channels, int expectedSize)
    {
        this.types = checkNotNull(types, "types is null");
        this.channels = checkNotNull(channels, "channels is null").clone();
        checkArgument(types.size() == channels.length, "types and channels have different sizes");

        this.pages = ObjectArrayList.wrap(new PageBuilder[1024], 0);
        this.pages.add(new PageBuilder(types));

        // reserve memory for the arrays
        int hashSize = arraySize(expectedSize, FILL_RATIO);

        maxFill = maxFill(hashSize, FILL_RATIO);
        mask = hashSize - 1;
        key = new long[hashSize];
        Arrays.fill(key, -1);

        value = new int[hashSize];

        groupAddress = new LongBigArray();
    }

    public long getEstimatedSize()
    {
        return completedPagesMemorySize + pages.get(pages.size() - 1).getMemorySize() + sizeOf(key) + sizeOf(value);
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public int getGroupCount()
    {
        return nextGroupId;
    }

    public void appendValuesTo(int groupId, BlockBuilder[] builders)
    {
        long address = groupAddress.get(groupId);
        PageBuilder page = pages.get(decodeSliceIndex(address));
        page.appendValuesTo(decodePosition(address), builders);
    }

    public GroupByIdBlock getGroupIds(Page page)
    {
        int positionCount = page.getPositionCount();

        int maxPossibleGroupId = nextGroupId + positionCount;
        groupAddress.ensureCapacity(maxPossibleGroupId);
        if (maxPossibleGroupId > maxFill) {
            rehash(maxPossibleGroupId);
        }

        // we know the exact size required for the block
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(positionCount);

        // open cursors for group blocks
        BlockCursor[] currentRow = new BlockCursor[channels.length];
        for (int i = 0; i < channels.length; i++) {
            currentRow[i] = page.getBlock(channels[i]).cursor();
        }

        // index pages
        for (int position = 0; position < page.getPositionCount(); position++) {
            for (BlockCursor cursor : currentRow) {
                checkState(cursor.advanceNextPosition());
            }

            // get the group for the current row
            int groupId = putIfAbsentInternal(currentRow);

            // output the group id for this row
            blockBuilder.append(groupId);
        }

        RandomAccessBlock block = blockBuilder.build();
        return new GroupByIdBlock(nextGroupId, block);
    }

    public int putIfAbsent(BlockCursor[] cursors)
    {
        int maxPossibleGroupId = nextGroupId + cursors[0].getRemainingPositions() + 1;
        groupAddress.ensureCapacity(maxPossibleGroupId);
        if (maxPossibleGroupId > maxFill) {
            rehash(maxPossibleGroupId);
        }

        return putIfAbsentInternal(cursors);
    }

    private int putIfAbsentInternal(BlockCursor[] cursors)
    {
        int hashPosition = ((int) Murmur3.hash64(hashCursor(cursors))) & mask;

        // look for an empty slot or a slot containing this key
        int groupId = -1;
        while (key[hashPosition] != -1) {
            long address = key[hashPosition];
            if (positionEqualsCurrentRow(decodeSliceIndex(address), decodePosition(address), cursors)) {
                // found an existing slot for this key
                groupId = value[hashPosition];

                break;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        // did we find an existing group?
        if (groupId < 0) {
            groupId = addNewGroup(hashPosition, cursors);
        }
        return groupId;
    }

    private int addNewGroup(int hashPosition, BlockCursor[] cursors)
    {
        // add the row to the open page
        int pageIndex = pages.size() - 1;
        PageBuilder pageBuilder = pages.get(pageIndex);
        pageBuilder.append(cursors);

        // record group id in hash
        int groupId = nextGroupId++;
        long address = encodeSyntheticAddress(pageIndex, pageBuilder.getPositionCount() - 1);

        key[hashPosition] = address;
        value[hashPosition] = groupId;
        groupAddress.set(groupId, address);

        // create new page builder if this page is full
        if (pageBuilder.isFull()) {
            completedPagesMemorySize += pageBuilder.getMemorySize();

            pageBuilder = new PageBuilder(types);
            pages.add(pageBuilder);
        }
        return groupId;
    }

    private void rehash(int size)
    {
        int newSize = arraySize(size + 1, FILL_RATIO);

        int newMask = newSize - 1;
        long[] newKey = new long[newSize];
        Arrays.fill(newKey, -1);
        int[] newValue = new int[newSize];

        int oldIndex = 0;
        for (int groupId = 0; groupId < nextGroupId; groupId++) {
            // seek to the next used slot
            while (key[oldIndex] == -1) {
                oldIndex++;
            }

            // get the address for this slot
            long address = key[oldIndex];

            // find an empty slot for the address
            int pos = ((int) Murmur3.hash64(hashPosition(address))) & newMask;
            while (newKey[pos] != -1) {
                pos = (pos + 1) & newMask;
            }

            // record the mapping
            newKey[pos] = address;
            newValue[pos] = value[oldIndex];
            oldIndex++;
        }

        this.mask = newMask;
        this.maxFill = maxFill(newSize, FILL_RATIO);
        this.key = newKey;
        this.value = newValue;
    }

    private static int hashCursor(BlockCursor... cursors)
    {
        int result = 0;
        for (BlockCursor cursor : cursors) {
            result = result * 31 + cursor.calculateHashCode();
        }
        return result;
    }

    private int hashPosition(long sliceAddress)
    {
        int sliceIndex = decodeSliceIndex(sliceAddress);
        int position = decodePosition(sliceAddress);
        return pages.get(sliceIndex).hashCode(position);
    }

    private boolean positionEqualsCurrentRow(int sliceIndex, int position, BlockCursor... currentRow)
    {
        return pages.get(sliceIndex).equals(position, currentRow);
    }

    private static class PageBuilder
    {
        private final List<BlockBuilder> channels;
        private int positionCount;
        private boolean full;

        public PageBuilder(List<Type> types)
        {
            ImmutableList.Builder<BlockBuilder> builder = ImmutableList.builder();
            for (Type type : types) {
                builder.add(type.createBlockBuilder(new BlockBuilderStatus()));
            }
            channels = builder.build();
        }

        public int getPositionCount()
        {
            return positionCount;
        }

        public long getMemorySize()
        {
            long memorySize = 0;
            for (BlockBuilder channel : channels) {
                memorySize += channel.getSizeInBytes();
            }
            return memorySize;
        }

        private void append(BlockCursor... row)
        {
            // append to each channel
            for (int channel = 0; channel < row.length; channel++) {
                row[channel].appendTo(channels.get(channel));
                full = full || channels.get(channel).isFull();
            }
            positionCount++;
        }

        public void appendValuesTo(int position, BlockBuilder... builders)
        {
            for (int i = 0; i < channels.size(); i++) {
                BlockBuilder channel = channels.get(i);
                channel.appendTo(position, builders[i]);
            }
        }

        public int hashCode(int position)
        {
            int result = 0;
            for (BlockBuilder channel : channels) {
                result = 31 * result + channel.hashCode(position);
            }
            return result;
        }

        public boolean equals(int thisPosition, PageBuilder that, int thatPosition)
        {
            for (int i = 0; i < channels.size(); i++) {
                BlockBuilder thisBlock = this.channels.get(i);
                BlockBuilder thatBlock = that.channels.get(i);
                if (!thisBlock.equals(thisPosition, thatBlock, thatPosition)) {
                    return false;
                }
            }
            return true;
        }

        public boolean equals(int position, BlockCursor... row)
        {
            for (int i = 0; i < channels.size(); i++) {
                BlockBuilder thisBlock = this.channels.get(i);
                if (!thisBlock.equals(position, row[i])) {
                    return false;
                }
            }
            return true;
        }

        public boolean isFull()
        {
            return full;
        }
    }
}
