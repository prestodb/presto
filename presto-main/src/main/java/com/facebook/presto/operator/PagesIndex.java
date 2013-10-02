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
import com.facebook.presto.tuple.TupleInfo;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;

import static com.facebook.presto.operator.HashStrategyUtils.addToHashCode;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static io.airlift.slice.SizeOf.sizeOf;

/**
 * PagesIndex a low-level data structure which contains the address of every value position of every channel.
 * This data structure is not general purpose and is designed for a few specific uses:
 * <ul>
 * <li>Sort via the {@link #sort} method</li>
 * <li>Hash build via the {@link #equals} and {@link #hashCode} methods</li>
 * <li>Positional output via the {@link #appendTupleTo} method</li>
 * </ul>
 */
public class PagesIndex
        implements Swapper
{
    private final List<TupleInfo> tupleInfos;
    private final OperatorContext operatorContext;
    private final LongArrayList valueAddresses;
    private final ObjectArrayList<RandomAccessPage> pages;

    private int positionCount;
    private long pagesMemorySize;
    private long estimatedSize;

    public PagesIndex(List<TupleInfo> tupleInfos, int expectedPositions, OperatorContext operatorContext)
    {
        this.tupleInfos = tupleInfos;
        this.operatorContext = operatorContext;
        this.valueAddresses = new LongArrayList(expectedPositions);
        this.pages = ObjectArrayList.wrap(new RandomAccessPage[1024], 0);
    }

    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public void addPage(Page page)
    {
        positionCount += page.getPositionCount();

        int pageIndex = pages.size();
        RandomAccessPage randomAccessPage = page.toRandomAccessPage();
        pages.add(randomAccessPage);

        for (int position = 0; position < page.getPositionCount(); position++) {
            long sliceAddress = encodeSyntheticAddress(pageIndex, position);
            valueAddresses.add(sliceAddress);
        }

        pagesMemorySize += randomAccessPage.getDataSize().toBytes();
        estimatedSize = operatorContext.setMemoryReservation(calculateEstimatedSize());
    }

    public DataSize getEstimatedSize()
    {
        return new DataSize(estimatedSize, Unit.BYTE);
    }

    private long calculateEstimatedSize()
    {
        // assumes 64bit addresses
        long sliceArraySize = sizeOf(pages.elements());
        long addressesArraySize = sizeOf(valueAddresses.elements());
        return pagesMemorySize + sliceArraySize + addressesArraySize;
    }

    public TupleInfo getTupleInfo(int channel)
    {
        return tupleInfos.get(channel);
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
            // get slice an offset for the position
            long sliceAddress = valueAddresses.getLong(position);
            int pageIndex = decodeSliceIndex(sliceAddress);
            RandomAccessPage page = pages.get(pageIndex);
            int blockPosition = decodePosition(sliceAddress);

            // if page has been fully consumed, get next page
            if (blockPosition >= page.getPositionCount()) {
                pageIndex++;
                if (pageIndex >= pages.size()) {
                    break;
                }
                page = pages.get(pageIndex);
                blockPosition = 0;
            }

            // append the row
            for (int i = 0; i < outputChannels.length; i++) {
                RandomAccessBlock leftBlock = page.getBlock(outputChannels[i]);
                leftBlock.appendTupleTo(blockPosition, pageBuilder.getBlockBuilder(i));
            }

            position++;
        }

        return position;
    }

    public void appendTupleTo(int[] outputChannels, int position, PageBuilder pageBuilder)
    {
        // get slice an offset for the position
        long sliceAddress = valueAddresses.getLong(position);
        RandomAccessPage page = getPageForSyntheticAddress(sliceAddress);
        int blockPosition = decodePosition(sliceAddress);

        for (int i = 0; i < outputChannels.length; i++) {
            RandomAccessBlock leftBlock = page.getBlock(outputChannels[i]);
            leftBlock.appendTupleTo(blockPosition, pageBuilder.getBlockBuilder(i));
        }
    }

    public void appendTupleTo(int channel, int position, BlockBuilder output)
    {
        // get slice an offset for the position
        long sliceAddress = valueAddresses.getLong(position);
        RandomAccessPage page = getPageForSyntheticAddress(sliceAddress);
        int blockPosition = decodePosition(sliceAddress);

        RandomAccessBlock leftBlock = page.getBlock(channel);

        leftBlock.appendTupleTo(blockPosition, output);
    }

    public boolean equals(int[] channels, int leftPosition, int rightPosition)
    {
        if (leftPosition == rightPosition) {
            return true;
        }

        long leftPageAddress = valueAddresses.getLong(leftPosition);
        RandomAccessPage leftPage = getPageForSyntheticAddress(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = valueAddresses.getLong(rightPosition);
        RandomAccessPage rightPage = getPageForSyntheticAddress(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        for (int channel : channels) {
            RandomAccessBlock leftBlock = leftPage.getBlock(channel);
            RandomAccessBlock rightBlock = rightPage.getBlock(channel);

            if (!leftBlock.equals(leftBlockPosition, rightBlock, rightBlockPosition)) {
                return false;
            }
        }
        return true;
    }

    public boolean equals(int[] channels, int position, BlockCursor[] cursors)
    {
        long pageAddress = valueAddresses.getLong(position);
        RandomAccessPage page = getPageForSyntheticAddress(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        for (int i = 0; i < channels.length; i++) {
            int channel = channels[i];
            BlockCursor cursor = cursors[i];

            RandomAccessBlock block = page.getBlock(channel);

            if (!block.equals(blockPosition, cursor)) {
                return false;
            }
        }
        return true;
    }

    public int hashCode(int[] channels, int position)
    {
        long pageAddress = valueAddresses.getLong(position);
        RandomAccessPage page = getPageForSyntheticAddress(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        int result = 0;
        for (int channel : channels) {
            RandomAccessBlock block = page.getBlock(channel);

            result = addToHashCode(result, block.hashCode(blockPosition));
        }
        return result;
    }

    private int compareTo(int[] sortChannels, SortOrder[] sortOrders, int leftPosition, int rightPosition)
    {
        long leftSliceAddress = valueAddresses.getLong(leftPosition);
        RandomAccessPage leftPage = getPageForSyntheticAddress(leftSliceAddress);
        int leftBlockPosition = decodePosition(leftSliceAddress);

        long rightSliceAddress = valueAddresses.getLong(rightPosition);
        RandomAccessPage rightPage = getPageForSyntheticAddress(rightSliceAddress);
        int rightBlockPosition = decodePosition(rightSliceAddress);

        for (int i = 0; i < sortChannels.length; i++) {
            int sortChannel = sortChannels[i];
            SortOrder sortOrder = sortOrders[i];

            RandomAccessBlock leftBlock = leftPage.getBlock(sortChannel);
            RandomAccessBlock rightBlock = rightPage.getBlock(sortChannel);

            int compare = leftBlock.compareTo(sortOrder, leftBlockPosition, rightBlock, rightBlockPosition);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    public void sort(int[] sortChannels, SortOrder[] sortOrders)
    {
        MultiSliceFieldOrderedTupleComparator comparator = new MultiSliceFieldOrderedTupleComparator(this, sortChannels, sortOrders);
        Arrays.quickSort(0, positionCount, comparator, this);
    }

    private RandomAccessPage getPageForSyntheticAddress(long sliceAddress)
    {
        return pages.get(decodeSliceIndex(sliceAddress));
    }

    public static class MultiSliceFieldOrderedTupleComparator
            extends AbstractIntComparator
    {
        private final PagesIndex pagesIndex;
        private final int[] sortChannels;
        private final SortOrder[] sortOrders;

        public MultiSliceFieldOrderedTupleComparator(PagesIndex pagesIndex, int[] sortChannels, SortOrder[] sortOrders)
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
