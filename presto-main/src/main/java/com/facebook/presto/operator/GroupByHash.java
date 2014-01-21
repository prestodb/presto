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

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.FixedWidthBlock;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.longs.Long2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.longs.LongHash;
import it.unimi.dsi.fastutil.longs.LongHash.Strategy;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;

import static com.facebook.presto.block.BlockBuilders.createBlockBuilder;
import static com.facebook.presto.operator.HashStrategyUtils.addToHashCode;
import static com.facebook.presto.operator.HashStrategyUtils.valueHashCode;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;

public class GroupByHash
{
    private static final long CURRENT_ROW_ADDRESS = 0xFF_FF_FF_FF_FF_FF_FF_FFL;

    private final List<Type> types;
    private final int[] channels;

    private final List<PageBuilder> pages;

    private long completedPagesMemorySize;

    private final PageBuilderHashStrategy hashStrategy;
    private final PagePositionToGroupId pagePositionToGroupId;

    private int nextGroupId;

    public GroupByHash(List<Type> types, int[] channels, int expectedSize)
    {
        this.types = checkNotNull(types, "types is null");
        this.channels = checkNotNull(channels, "channels is null").clone();
        checkArgument(types.size() == channels.length, "types and channels have different sizes");

        this.pages = ObjectArrayList.wrap(new PageBuilder[1024], 0);
        this.pages.add(new PageBuilder(types));

        this.hashStrategy = new PageBuilderHashStrategy();
        this.pagePositionToGroupId = new PagePositionToGroupId(expectedSize, hashStrategy);
        this.pagePositionToGroupId.defaultReturnValue(-1);
    }

    public long getEstimatedSize()
    {
        return completedPagesMemorySize + pages.get(pages.size() - 1).getMemorySize() + pagePositionToGroupId.getEstimatedSize();
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public GroupByIdBlock getGroupIds(Page page)
    {
        int positionCount = page.getPositionCount();

        // we know the exact size required for the block
        int groupIdBlockSize = SINGLE_LONG.getFixedSize() * positionCount;
        BlockBuilder blockBuilder = createBlockBuilder(SINGLE_LONG, Slices.allocate(groupIdBlockSize));

        // open cursors for group blocks
        BlockCursor[] cursors = new BlockCursor[channels.length];
        for (int i = 0; i < channels.length; i++) {
            cursors[i] = page.getBlock(channels[i]).cursor();
        }

        // use cursors in hash strategy to provide value for "current" row
        hashStrategy.setCurrentRow(cursors);

        for (int position = 0; position < positionCount; position++) {
            for (BlockCursor cursor : cursors) {
                checkState(cursor.advanceNextPosition());
            }

            int groupId = pagePositionToGroupId.get(CURRENT_ROW_ADDRESS);
            if (groupId < 0) {
                groupId = addNewGroup(cursors);
            }
            blockBuilder.append(groupId);
        }
        FixedWidthBlock block = (FixedWidthBlock) blockBuilder.build();
        return new GroupByIdBlock(nextGroupId, block);
    }

    private int addNewGroup(BlockCursor... row)
    {
        int pageIndex = pages.size() - 1;
        PageBuilder pageBuilder = pages.get(pageIndex);
        pageBuilder.append(row);

        // record group id in hash
        int groupId = nextGroupId++;
        long address = encodeSyntheticAddress(pageIndex, pageBuilder.getPositionCount() - 1);
        pagePositionToGroupId.put(address, groupId);

        // create new page builder if this page is full
        if (pageBuilder.isFull()) {
            completedPagesMemorySize += pageBuilder.getMemorySize();

            pageBuilder = new PageBuilder(types);
            pages.add(pageBuilder);
        }
        return groupId;
    }

    public Long2IntOpenCustomHashMap getPagePositionToGroupId()
    {
        return pagePositionToGroupId;
    }

    public void appendValuesTo(long pagePosition, BlockBuilder[] builders)
    {
        PageBuilder page = pages.get(decodeSliceIndex(pagePosition));
        page.appendValuesTo(decodePosition(pagePosition), builders);
    }

    private class PageBuilderHashStrategy
            implements Strategy
    {
        private BlockCursor[] currentRow;

        public void setCurrentRow(BlockCursor[] currentRow)
        {
            this.currentRow = currentRow;
        }

        public void addPage(PageBuilder page)
        {
            pages.add(page);
        }

        @Override
        public int hashCode(long sliceAddress)
        {
            if (sliceAddress == CURRENT_ROW_ADDRESS) {
                return hashCurrentRow();
            }
            else {
                return hashPosition(sliceAddress);
            }
        }

        private int hashPosition(long sliceAddress)
        {
            int sliceIndex = decodeSliceIndex(sliceAddress);
            int position = decodePosition(sliceAddress);
            return pages.get(sliceIndex).hashCode(position);
        }

        private int hashCurrentRow()
        {
            int result = 0;
            for (int channel = 0; channel < types.size(); channel++) {
                Type type = types.get(channel);
                BlockCursor cursor = currentRow[channel];
                result = addToHashCode(result, valueHashCode(type, cursor.getRawSlice(), cursor.getRawOffset()));
            }
            return result;
        }

        @Override
        public boolean equals(long leftSliceAddress, long rightSliceAddress)
        {
            // current row always equals itself
            if (leftSliceAddress == CURRENT_ROW_ADDRESS && rightSliceAddress == CURRENT_ROW_ADDRESS) {
                return true;
            }

            // current row == position
            if (leftSliceAddress == CURRENT_ROW_ADDRESS) {
                return positionEqualsCurrentRow(decodeSliceIndex(rightSliceAddress), decodePosition(rightSliceAddress));
            }

            // position == current row
            if (rightSliceAddress == CURRENT_ROW_ADDRESS) {
                return positionEqualsCurrentRow(decodeSliceIndex(leftSliceAddress), decodePosition(leftSliceAddress));
            }

            // position == position
            return positionEqualsPosition(
                    decodeSliceIndex(leftSliceAddress), decodePosition(leftSliceAddress),
                    decodeSliceIndex(rightSliceAddress), decodePosition(rightSliceAddress));
        }

        private boolean positionEqualsCurrentRow(int sliceIndex, int position)
        {
            return pages.get(sliceIndex).equals(position, currentRow);
        }

        private boolean positionEqualsPosition(int leftSliceIndex, int leftPosition, int rightSliceIndex, int rightPosition)
        {
            return pages.get(leftSliceIndex).equals(leftPosition, pages.get(rightSliceIndex), rightPosition);
        }
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
                builder.add(createBlockBuilder(new TupleInfo(type)));
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
                memorySize += channel.getDataSize().toBytes();
            }
            return memorySize;
        }

        private void append(BlockCursor... row)
        {
            // append to each channel
            for (int channel = 0; channel < row.length; channel++) {
                row[channel].appendTupleTo(channels.get(channel));
                full = full || channels.get(channel).isFull();
            }
            positionCount++;
        }

        public void appendValuesTo(int position, BlockBuilder... builders)
        {
            for (int i = 0; i < channels.size(); i++) {
                BlockBuilder channel = channels.get(i);
                channel.appendTupleTo(position, builders[i]);
            }
        }

        public int hashCode(int position)
        {
            int result = 0;
            for (BlockBuilder channel : channels) {
                result = addToHashCode(result, channel.hashCode(position));
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

    private static class PagePositionToGroupId
            extends Long2IntOpenCustomHashMap
    {
        private PagePositionToGroupId(int expected, LongHash.Strategy strategy)
        {
            super(expected, strategy);
            defaultReturnValue(-1);
        }

        public long getEstimatedSize()
        {
            return sizeOf(this.key) + sizeOf(this.value) + sizeOf(this.used);
        }
    }
}
