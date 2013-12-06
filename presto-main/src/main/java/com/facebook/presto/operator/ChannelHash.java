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

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.longs.LongHash;
import it.unimi.dsi.fastutil.longs.LongHash.Strategy;

import java.util.Arrays;

import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static io.airlift.slice.SizeOf.sizeOf;

public class ChannelHash
{
    //
    // This class is effectively a Multimap<KeyAddress,Position>.
    //
    // The key address is a SyntheticAddress and the position is the position of the key withing the
    // channel index.
    //
    // The multimap itself is formed out of a regular map and position chaining array.  To perform a
    // lookup, the "lookup" slice is set in the hash, and a synthetic address within the "lookup" slice
    // is created. The "lookup" slice is given index -1 as to not conflict with any slices
    // in the channel index.  Then first position is retrieved from the main address to position map.
    // If a position was found, the remaining value positions are located using the position links array.
    //

    private static final int LOOKUP_SLICE_INDEX = 0xFF_FF_FF_FF;

    private final SliceHashStrategy hashStrategy;
    private final AddressToPositionMap addressToPositionMap;
    private final IntArrayList positionLinks;

    public ChannelHash(ChannelIndex channelIndex, OperatorContext operatorContext)
    {
        hashStrategy = new SliceHashStrategy(channelIndex.getTupleInfo(), channelIndex.getSlices().elements());
        addressToPositionMap = new AddressToPositionMap(channelIndex.getPositionCount(), hashStrategy);
        addressToPositionMap.defaultReturnValue(-1);
        positionLinks = new IntArrayList(new int[channelIndex.getValueAddresses().size()]);
        Arrays.fill(positionLinks.elements(), -1);
        for (int position = 0; position < channelIndex.getValueAddresses().size(); position++) {
            operatorContext.setMemoryReservation(getEstimatedSize());
            long sliceAddress = channelIndex.getValueAddresses().elements()[position];
            int oldPosition = addressToPositionMap.put(sliceAddress, position);
            if (oldPosition >= 0) {
                // link the new position to the old position
                positionLinks.set(position, oldPosition);
            }
        }
    }

    public ChannelHash(ChannelHash hash)
    {
        // hash strategy can not be shared across threads, but everything else can
        this.hashStrategy = new SliceHashStrategy(hash.hashStrategy.tupleInfo, hash.hashStrategy.slices);
        this.addressToPositionMap = new AddressToPositionMap(hash.addressToPositionMap, hashStrategy);
        addressToPositionMap.defaultReturnValue(-1);
        this.positionLinks = hash.positionLinks;
    }

    private long getEstimatedSize()
    {
        long addressToPositionSize = addressToPositionMap.getEstimatedSize().toBytes();
        long positionLinksSize = sizeOf(positionLinks.elements());
        return addressToPositionSize + positionLinksSize;
    }

    public void setLookupSlice(Slice lookupSlice)
    {
        hashStrategy.setLookupSlice(lookupSlice);
    }

    public int get(BlockCursor cursor)
    {
        int position = addressToPositionMap.get(encodeSyntheticAddress(LOOKUP_SLICE_INDEX, cursor.getRawOffset()));
        return position;
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
        private Slice lookupSlice;

        public SliceHashStrategy(TupleInfo tupleInfo, Slice[] slices)
        {
            this.tupleInfo = tupleInfo;
            this.slices = slices;
        }

        public void setLookupSlice(Slice lookupSlice)
        {
            this.lookupSlice = lookupSlice;
        }

        @Override
        public int hashCode(long sliceAddress)
        {
            Slice slice = getSliceForSyntheticAddress(sliceAddress);
            int offset = (int) sliceAddress;
            int length = tupleInfo.size(slice, offset);
            int hashCode = slice.hashCode(offset, length);
            return hashCode;
        }

        @Override
        public boolean equals(long leftSliceAddress, long rightSliceAddress)
        {
            Slice leftSlice = getSliceForSyntheticAddress(leftSliceAddress);
            int leftOffset = decodePosition(leftSliceAddress);
            int leftLength = tupleInfo.size(leftSlice, leftOffset);

            Slice rightSlice = getSliceForSyntheticAddress(rightSliceAddress);
            int rightOffset = decodePosition(rightSliceAddress);
            int rightLength = tupleInfo.size(rightSlice, rightOffset);

            return leftSlice.equals(leftOffset, leftLength, rightSlice, rightOffset, rightLength);
        }

        private Slice getSliceForSyntheticAddress(long sliceAddress)
        {
            int sliceIndex = decodeSliceIndex(sliceAddress);
            Slice slice;
            if (sliceIndex == LOOKUP_SLICE_INDEX) {
                slice = lookupSlice;
            }
            else {
                slice = slices[sliceIndex];
            }
            return slice;
        }
    }

    private static class AddressToPositionMap
            extends Long2IntOpenCustomHashMap
    {
        private AddressToPositionMap(int expected, LongHash.Strategy strategy)
        {
            super(expected, strategy);
        }

        private AddressToPositionMap(Long2IntMap m, LongHash.Strategy strategy)
        {
            super(m, strategy);
        }

        public DataSize getEstimatedSize()
        {
            return new DataSize(sizeOf(this.key) + sizeOf(this.value) + sizeOf(this.used), Unit.BYTE);
        }
    }
}
