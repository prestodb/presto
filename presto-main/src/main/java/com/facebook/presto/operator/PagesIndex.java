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
import com.facebook.presto.block.RandomAccessBlock;
import com.google.common.collect.ImmutableList;
import com.facebook.presto.type.Type;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.sizeOf;

/**
 * PagesIndex a low-level data structure which contains the address of every value position of every channel.
 * This data structure is not general purpose and is designed for a few specific uses:
 * <ul>
 * <li>Sort via the {@link #sort} method</li>
 * <li>Hash build via the {@link #equals} and {@link #hashCode} methods</li>
 * <li>Positional output via the {@link #appendTo} method</li>
 * </ul>
 */
public class PagesIndex
        implements Swapper
{
    private final List<Type> types;
    private final OperatorContext operatorContext;
    private final LongArrayList valueAddresses;
    private final ObjectArrayList<RandomAccessBlock>[] channels;

    private int positionCount;
    private long pagesMemorySize;
    private long estimatedSize;

    public PagesIndex(List<Type> types, int expectedPositions, OperatorContext operatorContext)
    {
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.valueAddresses = new LongArrayList(expectedPositions);

        channels = (ObjectArrayList<RandomAccessBlock>[]) new ObjectArrayList[types.size()];
        for (int i = 0; i < channels.length; i++) {
            channels[i] = ObjectArrayList.wrap(new RandomAccessBlock[1024], 0);
        }
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public void addPage(Page page)
    {
        positionCount += page.getPositionCount();

        int pageIndex = channels[0].size();
        RandomAccessPage randomAccessPage = page.toRandomAccessPage();
        for (int i = 0; i < channels.length; i++) {
            RandomAccessBlock block = randomAccessPage.getBlock(i);
            channels[i].add(block);
            pagesMemorySize += block.getDataSize().toBytes();
        }

        for (int position = 0; position < page.getPositionCount(); position++) {
            long sliceAddress = encodeSyntheticAddress(pageIndex, position);
            valueAddresses.add(sliceAddress);
        }

        estimatedSize = operatorContext.setMemoryReservation(calculateEstimatedSize());
    }

    public DataSize getEstimatedSize()
    {
        return new DataSize(estimatedSize, Unit.BYTE);
    }

    private long calculateEstimatedSize()
    {
        long channelsArraySize = sizeOf(channels[0].elements()) * channels.length;
        long addressesArraySize = sizeOf(valueAddresses.elements());
        return pagesMemorySize + channelsArraySize + addressesArraySize;
    }

    public Type getType(int channel)
    {
        return types.get(channel);
    }

    @Override
    public void swap(int a, int b)
    {
        long[] elements = valueAddresses.elements();
        long temp = elements[a];
        elements[a] = elements[b];
        elements[b] = temp;
    }

    public int buildPage(int position, int[] outputChannels, PageBuilder pageBuilder)
    {
        while (!pageBuilder.isFull() && position < positionCount) {
            long pageAddress = valueAddresses.getLong(position);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);

            // append the row
            for (int i = 0; i < outputChannels.length; i++) {
                int outputChannel = outputChannels[i];
                RandomAccessBlock block = this.channels[outputChannel].get(blockIndex);
                block.appendTo(blockPosition, pageBuilder.getBlockBuilder(i));
            }

            position++;
        }

        return position;
    }

    public void appendTo(int channel, int position, BlockBuilder output)
    {
        long pageAddress = valueAddresses.getLong(position);

        RandomAccessBlock block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        block.appendTo(blockPosition, output);
    }

    public boolean equals(int[] channels, int leftPosition, int rightPosition)
    {
        if (leftPosition == rightPosition) {
            return true;
        }

        long leftPageAddress = valueAddresses.getLong(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = valueAddresses.getLong(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        for (int channel : channels) {
            RandomAccessBlock leftBlock = this.channels[channel].get(leftBlockIndex);
            RandomAccessBlock rightBlock = this.channels[channel].get(rightBlockIndex);

            if (!leftBlock.equals(leftBlockPosition, rightBlock, rightBlockPosition)) {
                return false;
            }
        }
        return true;
    }

    public boolean equals(int[] channels, int position, BlockCursor[] cursors)
    {
        long pageAddress = valueAddresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        for (int i = 0; i < channels.length; i++) {
            int channel = channels[i];
            BlockCursor cursor = cursors[i];

            RandomAccessBlock block = this.channels[channel].get(blockIndex);

            if (!block.equals(blockPosition, cursor)) {
                return false;
            }
        }
        return true;
    }

    public int hashCode(int[] channels, int position)
    {
        long pageAddress = valueAddresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        int result = 0;
        for (int channel : channels) {
            RandomAccessBlock block = this.channels[channel].get(blockIndex);
            result = 31 * result + block.hashCode(blockPosition);
        }
        return result;
    }

    private int compareTo(int[] sortChannels, SortOrder[] sortOrders, int leftPosition, int rightPosition)
    {
        long leftPageAddress = valueAddresses.getLong(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = valueAddresses.getLong(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        for (int i = 0; i < sortChannels.length; i++) {
            int sortChannel = sortChannels[i];
            SortOrder sortOrder = sortOrders[i];

            RandomAccessBlock leftBlock = this.channels[sortChannel].get(leftBlockIndex);
            RandomAccessBlock rightBlock = this.channels[sortChannel].get(rightBlockIndex);

            int compare = leftBlock.compareTo(sortOrder, leftBlockPosition, rightBlock, rightBlockPosition);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    public void sort(int[] sortChannels, SortOrder[] sortOrders)
    {
        RowComparator comparator = new RowComparator(this, sortChannels, sortOrders);
        Arrays.quickSort(0, positionCount, comparator, this);
    }

    public static class RowComparator
            extends AbstractIntComparator
    {
        private final PagesIndex pagesIndex;
        private final int[] sortChannels;
        private final SortOrder[] sortOrders;

        public RowComparator(PagesIndex pagesIndex, int[] sortChannels, SortOrder[] sortOrders)
        {
            this.pagesIndex = pagesIndex;
            this.sortChannels = sortChannels;
            this.sortOrders = sortOrders;
        }

        @Override
        public int compare(int leftPosition, int rightPosition)
        {
            return pagesIndex.compareTo(sortChannels, sortOrders, leftPosition, rightPosition);
        }
    }
}
