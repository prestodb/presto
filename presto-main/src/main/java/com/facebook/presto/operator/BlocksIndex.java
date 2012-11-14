/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterable;
import it.unimi.dsi.fastutil.longs.LongListIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import static com.facebook.presto.hive.shaded.com.google.common.base.Preconditions.checkState;

public class BlocksIndex
        implements LongIterable, Swapper
{
    private int positionCount;
    private final LongArrayList offsets;
    private final ObjectArrayList<Slice> slices;
    private final TupleInfo tupleInfo;

    public BlocksIndex(int expectedPositions, TupleInfo tupleInfo)
    {
        this.tupleInfo = tupleInfo;
        offsets = new LongArrayList(expectedPositions);
        slices = ObjectArrayList.wrap(new Slice[1024], 0);
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    public ObjectArrayList<Slice> getSlices()
    {
        return slices;
    }

    public LongArrayList getOffsets()
    {
        return offsets;
    }

    public void swap(int a, int b)
    {
        long[] elements = offsets.elements();
        long temp = elements[a];
        elements[a] = elements[b];
        elements[b] = temp;
    }

    public LongListIterator iterator()
    {
        return offsets.iterator();
    }

    public void indexBlock(UncompressedBlock block)
    {
        positionCount += block.getPositionCount();

        // index the block
        int blockIndex = slices.size();
        slices.add(blockIndex, block.getSlice());
        BlockCursor cursor = block.cursor();
        for (int position = 0; position < block.getPositionCount(); position++) {
            checkState(cursor.advanceNextPosition());
            int offset = cursor.getRawOffset();

            long sliceAddress = (((long) blockIndex) << 32) | offset;

            Preconditions.checkState((int) (sliceAddress >> 32) == blockIndex);
            Preconditions.checkState((int) sliceAddress == offset);

            offsets.add(sliceAddress);
        }
    }

    public void appendTo(int position, BlockBuilder output)
    {
        // get slice an offset for the position
        long sliceAddress = offsets.getLong(position);
        Slice slice = slices.get(((int) (sliceAddress >> 32)));
        int offset = (int) sliceAddress;

        // append the tuple
        output.appendTuple(slice, offset);
    }
}
