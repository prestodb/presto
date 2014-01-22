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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.tuple.TupleInfo;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;

import java.util.List;

/**
 * PagesIndex a low-level data structure which contains the address of every value position of every channel.
 * This data structure is not general purpose and is designed for a few specific uses:
 * <ul>
 * <li>Sort via the {@link #sort} method</li>
 * <li>Hash build via the {@link #getIndex} method</li>
 * <li>Positional output via the {@link #appendTupleTo} method</li>
 * </ul>
 */
public class PagesIndex
        implements Swapper
{
    private final ChannelIndex[] indexes;
    private final List<TupleInfo> tupleInfos;
    private final OperatorContext operatorContext;

    private int positionCount;
    private long estimatedSize;

    public PagesIndex(List<TupleInfo> tupleInfos, int expectedPositions, OperatorContext operatorContext)
    {
        this.tupleInfos = tupleInfos;
        this.operatorContext = operatorContext;
        this.indexes = new ChannelIndex[tupleInfos.size()];
        for (int channel = 0; channel < indexes.length; channel++) {
            indexes[channel] = new ChannelIndex(expectedPositions, tupleInfos.get(channel));
        }
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
        Block[] blocks = page.getBlocks();
        for (int channel = 0; channel < indexes.length; channel++) {
            indexes[channel].indexBlock((UncompressedBlock) blocks[channel]);
        }

        estimatedSize = operatorContext.setMemoryReservation(calculateEstimatedSize());
    }

    public DataSize getEstimatedSize()
    {
        return new DataSize(estimatedSize, Unit.BYTE);
    }

    private long calculateEstimatedSize()
    {
        long size = 0;
        for (ChannelIndex channelIndex : indexes) {
            size += channelIndex.getEstimatedSize().toBytes();
        }
        return size;
    }

    public TupleInfo getTupleInfo(int channel)
    {
        return indexes[channel].getTupleInfo();
    }

    public ChannelIndex getIndex(int channel)
    {
        return indexes[channel];
    }

    @Override
    public void swap(int a, int b)
    {
        for (ChannelIndex index : indexes) {
            index.swap(a, b);
        }
    }

    public void appendTupleTo(int channel, int position, BlockBuilder output)
    {
        indexes[channel].appendTo(position, output);
    }

    public void sort(int[] sortChannels, SortOrder[] sortOrders)
    {
        MultiSliceFieldOrderedTupleComparator comparator = new MultiSliceFieldOrderedTupleComparator(this, sortChannels, sortOrders);
        Arrays.quickSort(0, positionCount, comparator, this);
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
            for (int i = 0; i < sortChannels.length; i++) {
                ChannelIndex index = pagesIndex.getIndex(sortChannels[i]);
                int compare = index.compare(sortOrders[i], leftPosition, rightPosition);
                if (compare != 0) {
                    return compare;
                }
            }
            return 0;
        }
    }
}
