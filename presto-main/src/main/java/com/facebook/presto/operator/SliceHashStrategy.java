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

import com.facebook.presto.tuple.TupleInfo;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.longs.LongHash;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.google.common.base.Preconditions.checkNotNull;

public class SliceHashStrategy
        implements LongHash.Strategy
{
    // The sliceAddress is a SyntheticAddress.
    //
    // To perform a lookup using this strategy, the "lookup" slice is set in the strategy, and a synthetic address
    // within the "lookup" slice is created. The "lookup" slice is given an index of -1 so that it does not conflict
    // with any other slices that are stored in this strategy.
    public static final int LOOKUP_SLICE_INDEX = 0xFF_FF_FF_FF;

    private final TupleInfo tupleInfo;
    private final List<Slice> slices;
    private Slice lookupSlice;
    private long memorySize;

    public SliceHashStrategy(TupleInfo tupleInfo)
    {
        this.tupleInfo = checkNotNull(tupleInfo, "tupleInfo is null");
        this.slices = ObjectArrayList.wrap(new Slice[1024], 0);
    }

    public SliceHashStrategy(SliceHashStrategy strategy)
    {
        checkNotNull(strategy, "strategy is null");
        this.tupleInfo = strategy.tupleInfo;
        this.slices = strategy.slices;
    }

    public DataSize getEstimatedSize()
    {
        return new DataSize(memorySize, DataSize.Unit.BYTE);
    }

    public void setLookupSlice(Slice lookupSlice)
    {
        checkNotNull(lookupSlice, "lookupSlice is null");
        this.lookupSlice = lookupSlice;
    }

    public void addSlices(Iterable<Slice> slices)
    {
        for (Slice slice : slices) {
            addSlice(slice);
        }
    }

    public void addSlice(Slice slice)
    {
        memorySize += slice.length();
        slices.add(slice);
    }

    @Override
    public int hashCode(long sliceAddress)
    {
        Slice slice = getSliceForSyntheticAddress(sliceAddress);
        int offset = (int) sliceAddress;
        int length = tupleInfo.size(slice, offset);
        return slice.hashCode(offset, length);
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
        return sliceIndex == LOOKUP_SLICE_INDEX ? lookupSlice : slices.get(sliceIndex);
    }
}
