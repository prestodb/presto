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
import com.facebook.presto.tuple.TupleInfo.Type;
import io.airlift.slice.Slice;
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

    public void sort(int orderByChannel, int[] sortFields, boolean[] sortOrder)
    {
        ChannelIndex index = indexes[orderByChannel];
        MultiSliceFieldOrderedTupleComparator comparator = new MultiSliceFieldOrderedTupleComparator(sortFields, sortOrder, index);
        Arrays.quickSort(0, indexes[0].getValueAddresses().size(), comparator, this);
    }

    public static class MultiSliceFieldOrderedTupleComparator
            extends AbstractIntComparator
    {
        private final TupleInfo tupleInfo;
        private final long[] sliceAddresses;
        private final Slice[] slices;
        private final Type[] types;
        private final int[] sortFields;
        private final boolean[] sortOrder;

        public MultiSliceFieldOrderedTupleComparator(int[] sortFields, boolean[] sortOrder, ChannelIndex index)
        {
            this(sortFields, sortOrder, index.getTupleInfo(), index.getValueAddresses().elements(), index.getSlices().elements());
        }

        public MultiSliceFieldOrderedTupleComparator(int[] sortFields, boolean[] sortOrder, TupleInfo tupleInfo, long[] sliceAddresses, Slice... slices)
        {
            this.sortFields = sortFields;
            this.sortOrder = sortOrder;
            this.tupleInfo = tupleInfo;
            this.sliceAddresses = sliceAddresses;
            this.slices = slices;
            List<Type> types = tupleInfo.getTypes();
            this.types = types.toArray(new Type[types.size()]);
        }

        @Override
        public int compare(int leftPosition, int rightPosition)
        {
            long leftSliceAddress = sliceAddresses[leftPosition];
            Slice leftSlice = slices[((int) (leftSliceAddress >> 32))];
            int leftOffset = (int) leftSliceAddress;

            long rightSliceAddress = sliceAddresses[rightPosition];
            Slice rightSlice = slices[((int) (rightSliceAddress >> 32))];
            int rightOffset = (int) rightSliceAddress;

            for (int i = 0; i < sortFields.length; i++) {
                int field = sortFields[i];
                Type type = types[field];

                // todo add support for nulls first, nulls last
                int comparison;
                switch (type) {
                    case BOOLEAN:
                        comparison = Boolean.compare(
                                tupleInfo.getBoolean(leftSlice, leftOffset, field),
                                tupleInfo.getBoolean(rightSlice, rightOffset, field));
                        break;
                    case FIXED_INT_64:
                        comparison = Long.compare(
                                tupleInfo.getLong(leftSlice, leftOffset, field),
                                tupleInfo.getLong(rightSlice, rightOffset, field));
                        break;
                    case DOUBLE:
                        comparison = Double.compare(
                                tupleInfo.getDouble(leftSlice, leftOffset, field),
                                tupleInfo.getDouble(rightSlice, rightOffset, field));
                        break;
                    case VARIABLE_BINARY:
                        comparison = tupleInfo.getSlice(leftSlice, leftOffset, field)
                                .compareTo(tupleInfo.getSlice(rightSlice, rightOffset, field));
                        break;
                    default:
                        throw new AssertionError("unimplemented type: " + type);
                }
                if (comparison != 0) {
                    return sortOrder[i] ? comparison : -comparison;
                }
            }
            return 0;
        }
    }
}
