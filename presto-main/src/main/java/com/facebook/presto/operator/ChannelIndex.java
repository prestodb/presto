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
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterable;
import it.unimi.dsi.fastutil.longs.LongListIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceOffset;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.facebook.presto.slice.SizeOf.sizeOf;
import static com.google.common.base.Preconditions.checkState;

/**
 * ChannelIndex a low-level data structure which contains the address of every value position with a channel.
 * This data structure is not general purpose and is designed for a few specific uses:
 *   Sort via the swap method
 *   Hash build via the iterator method
 *   Positional output via the appendTo method
 */
public class ChannelIndex
        implements LongIterable, Swapper
{
    private int positionCount;
    private final LongArrayList valueAddresses;
    private final ObjectArrayList<Slice> slices;
    private final TupleInfo tupleInfo;
    private long slicesMemorySize;

    public ChannelIndex(int expectedPositions, TupleInfo tupleInfo)
    {
        this.tupleInfo = tupleInfo;
        valueAddresses = new LongArrayList(expectedPositions);
        slices = ObjectArrayList.wrap(new Slice[1024], 0);
    }

    public DataSize getEstimatedSize()
    {
        // assumes 64bit addresses
        long sliceArraySize = sizeOf(slices.elements());
        long addressesArraySize = sizeOf(valueAddresses.elements());
        return new DataSize(slicesMemorySize + sliceArraySize + addressesArraySize, Unit.BYTE);
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

    public LongArrayList getValueAddresses()
    {
        return valueAddresses;
    }

    public void swap(int a, int b)
    {
        long[] elements = valueAddresses.elements();
        long temp = elements[a];
        elements[a] = elements[b];
        elements[b] = temp;
    }

    public LongListIterator iterator()
    {
        return valueAddresses.iterator();
    }

    public void indexBlock(UncompressedBlock block)
    {
        positionCount += block.getPositionCount();

        // index the block
        int blockIndex = slices.size();
        slices.add(blockIndex, block.getSlice());
        slicesMemorySize += block.getSlice().length();
        BlockCursor cursor = block.cursor();
        for (int position = 0; position < block.getPositionCount(); position++) {
            checkState(cursor.advanceNextPosition());
            int offset = cursor.getRawOffset();

            long sliceAddress = encodeSyntheticAddress(blockIndex, offset);

            Preconditions.checkState((int) (sliceAddress >> 32) == blockIndex);
            Preconditions.checkState((int) sliceAddress == offset);

            valueAddresses.add(sliceAddress);
        }
    }

    public void appendTo(int position, BlockBuilder output)
    {
        // get slice an offset for the position
        long sliceAddress = valueAddresses.getLong(position);
        Slice slice = getSliceForSyntheticAddress(sliceAddress);
        int offset = decodeSliceOffset(sliceAddress);

        // append the tuple
        output.appendTuple(slice, offset);
    }

    public Slice getSliceForSyntheticAddress(long sliceAddress)
    {
        return slices.get(decodeSliceIndex(sliceAddress));
    }
}
