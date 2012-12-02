/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;

import java.util.List;

public class PagesIndex
        implements Swapper
{
    private final BlocksIndex[] indexes;
    private final int channelCount;
    private final int positionCount;

    public PagesIndex(Operator source, int expectedPositions)
    {
        channelCount = source.getChannelCount();
        indexes = new BlocksIndex[channelCount];
        List<TupleInfo> tupleInfos = source.getTupleInfos();
        for (int channel = 0; channel < indexes.length; channel++) {
            indexes[channel] = new BlocksIndex(expectedPositions, tupleInfos.get(channel));
        }

        int positionCount = 0;
        for (Page page : source) {
            positionCount += page.getPositionCount();
            Block[] blocks = page.getBlocks();
            for (int channel = 0; channel < indexes.length; channel++) {
                indexes[channel].indexBlock((UncompressedBlock) blocks[channel]);
            }
        }

        this.positionCount = positionCount;
    }

    public int getChannelCount()
    {
        return channelCount;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public TupleInfo getTupleInfo(int channel)
    {
        return indexes[channel].getTupleInfo();
    }

    public BlocksIndex getIndex(int channel)
    {
        return indexes[channel];
    }

    @Override
    public void swap(int a, int b)
    {
        for (BlocksIndex index : indexes) {
            index.swap(a, b);
        }
    }

    public void appendTupleTo(int channel, int position, BlockBuilder output)
    {
        indexes[channel].appendTo(position, output);
    }

    public void sort(int orderByChannel, int[] sortFields, boolean[] sortOrder)
    {
        BlocksIndex index = indexes[orderByChannel];
        MultiSliceFieldOrderedTupleComparator comparator = new MultiSliceFieldOrderedTupleComparator(sortFields, sortOrder, index);
        Arrays.quickSort(0, indexes[0].getOffsets().size(), comparator, this);
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

        public MultiSliceFieldOrderedTupleComparator(int[] sortFields, boolean[] sortOrder, BlocksIndex index)
        {
            this(sortFields, sortOrder, index.getTupleInfo(), index.getOffsets().elements(), index.getSlices().elements());
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

                int comparison;
                switch (type) {
                    case FIXED_INT_64:
                        comparison = Long.compare(tupleInfo.getLong(leftSlice, leftOffset, field), tupleInfo.getLong(rightSlice, rightOffset, field));
                        break;
                    case DOUBLE:
                        comparison = Double.compare(tupleInfo.getDouble(leftSlice, leftOffset, field), tupleInfo.getDouble(rightSlice, rightOffset, field));
                        break;
                    case VARIABLE_BINARY:
                        comparison = tupleInfo.getSlice(leftSlice, leftOffset, field).compareTo(tupleInfo.getSlice(rightSlice, rightOffset, field));
                        break;
                    default:
                        throw new AssertionError("unimplemented type: " + type);
                }
                if (comparison != 0) {
                    if (sortOrder[i]) {
                        return comparison;
                    }
                    else {
                        return -comparison;
                    }
                }
            }
            return 0;
        }
    }
}
