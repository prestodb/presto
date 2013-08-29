/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.execution.TaskMemoryManager;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.longs.LongHash;

import java.util.Arrays;

import static com.facebook.presto.operator.SliceHashStrategy.LOOKUP_SLICE_INDEX;
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
    // is created. Then first position is retrieved from the main address to position map.
    // If a position was found, the remaining value positions are located using the position links array.
    //
    private final SliceHashStrategy hashStrategy;
    private final AddressToPositionMap addressToPositionMap;
    private final IntArrayList positionLinks;

    public ChannelHash(ChannelIndex channelIndex, TaskMemoryManager taskMemoryManager)
    {
        hashStrategy = new SliceHashStrategy(channelIndex.getTupleInfo());
        hashStrategy.addSlices(channelIndex.getSlices());
        addressToPositionMap = new AddressToPositionMap(channelIndex.getPositionCount(), hashStrategy);
        addressToPositionMap.defaultReturnValue(-1);
        positionLinks = new IntArrayList(new int[channelIndex.getValueAddresses().size()]);
        Arrays.fill(positionLinks.elements(), -1);
        long currentHashSize = 0;
        for (int position = 0; position < channelIndex.getValueAddresses().size(); position++) {
            currentHashSize = taskMemoryManager.updateOperatorReservation(currentHashSize, getEstimatedSize());
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
        this.hashStrategy = new SliceHashStrategy(hash.hashStrategy);
        this.addressToPositionMap = new AddressToPositionMap(hash.addressToPositionMap, hashStrategy);
        addressToPositionMap.defaultReturnValue(-1);
        this.positionLinks = hash.positionLinks;
    }

    /**
     * Size of this hash alone without the underlying slices.
     */
    public DataSize getEstimatedSize()
    {
        long addressToPositionSize = addressToPositionMap.getEstimatedSize().toBytes();
        long positionLinksSize = sizeOf(positionLinks.elements());
        return new DataSize(addressToPositionSize + positionLinksSize, Unit.BYTE);
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

    private static class AddressToPositionMap
            extends Long2IntOpenCustomHashMap {
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
