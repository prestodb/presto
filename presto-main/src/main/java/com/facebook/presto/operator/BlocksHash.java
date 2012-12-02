/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.TupleInfo;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.longs.LongHash.Strategy;

import java.util.Arrays;

public class BlocksHash
{
    private final SliceHashStrategy hashStrategy;
    private final Long2IntOpenCustomHashMap joinChannelHash;

    private final IntArrayList positionLinks;

    public BlocksHash(BlocksIndex joinChannelIndex)
    {
        hashStrategy = new SliceHashStrategy(joinChannelIndex.getTupleInfo(), joinChannelIndex.getSlices().elements());
        joinChannelHash = new Long2IntOpenCustomHashMap(joinChannelIndex.getPositionCount(), hashStrategy);
        joinChannelHash.defaultReturnValue(-1);
        positionLinks = new IntArrayList(new int[joinChannelIndex.getOffsets().size()]);
        Arrays.fill(positionLinks.elements(), -1);
        for (int position = 0; position < joinChannelIndex.getOffsets().size(); position++) {
            long sliceAddress = joinChannelIndex.getOffsets().elements()[position];
            int oldPosition = joinChannelHash.put(sliceAddress, position);
            if (oldPosition >= 0) {
                // link the new position to the old position
                positionLinks.set(position, oldPosition);
            }
        }

    }

    public BlocksHash(BlocksHash hash)
    {
        // hash strategy can not be shared across threads, but everything else can
        this.hashStrategy = new SliceHashStrategy(hash.hashStrategy.tupleInfo, hash.hashStrategy.slices);
        this.joinChannelHash = new Long2IntOpenCustomHashMap(hash.joinChannelHash, hashStrategy);
        joinChannelHash.defaultReturnValue(-1);
        this.positionLinks = hash.positionLinks;
    }

    public void setProbeSlice(Slice slice)
    {
        hashStrategy.setProbeSlice(slice);
    }

    public int get(BlockCursor cursor)
    {
        int joinPosition = joinChannelHash.get(0xFF_FF_FF_FF_00_00_00_00L | cursor.getRawOffset());
        return joinPosition;
    }

    public int getNextPosition(int currentPosition)
    {
        return positionLinks.getInt(currentPosition);
    }

    public static class SliceHashStrategy
            implements Strategy
    {
        private final TupleInfo tupleInfo;
        private final Slice[] slices;
        private Slice probeSlice;

        public SliceHashStrategy(TupleInfo tupleInfo, Slice[] slices)
        {
            this.tupleInfo = tupleInfo;
            this.slices = slices;
        }

        public void setProbeSlice(Slice probeSlice)
        {
            this.probeSlice = probeSlice;
        }

        @Override
        public int hashCode(long sliceAddress)
        {
            Slice slice = getSlice(sliceAddress);
            int offset = (int) sliceAddress;
            int length = tupleInfo.size(slice, offset);
            int hashCode = slice.hashCode(offset, length);
            return hashCode;
        }

        @Override
        public boolean equals(long leftSliceAddress, long rightSliceAddress)
        {
            Slice leftSlice = getSlice(leftSliceAddress);
            int leftOffset = (int) leftSliceAddress;
            int leftLength = tupleInfo.size(leftSlice, leftOffset);

            Slice rightSlice = getSlice(rightSliceAddress);
            int rightOffset = (int) rightSliceAddress;
            int rightLength = tupleInfo.size(rightSlice, rightOffset);

            return leftSlice.equals(leftOffset, leftLength, rightSlice, rightOffset, rightLength);

        }

        private Slice getSlice(long sliceAddress)
        {
            int sliceIndex = (int) (sliceAddress >> 32);
            Slice slice;
            if (sliceIndex == 0xFF_FF_FF_FF) {
                slice = probeSlice;
            }
            else {
                slice = slices[sliceIndex];
            }
            return slice;
        }
    }
}
