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
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterable;
import it.unimi.dsi.fastutil.longs.LongListIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;

/**
 * ChannelIndex a low-level data structure which contains the address of every value position with a channel.
 * This data structure is not general purpose and is designed for a few specific uses:
 * <ul>
 * <li>Sort via the {@link #swap} method</li>
 * <li>Hash build via the {@link #iterator} method</li>
 * <li>Positional output via the {@link #appendTo} method</li>
 * </ul>
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
        int offset = decodePosition(sliceAddress);

        // append the tuple
        output.appendTuple(slice, offset);
    }

    public Slice getSliceForSyntheticAddress(long sliceAddress)
    {
        return slices.get(decodeSliceIndex(sliceAddress));
    }
}
