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
package com.facebook.presto.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.facebook.presto.spi.block.BlockUtil.checkValidPositions;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.EMPTY_SLICE;

@Deprecated
public class SliceArrayBlock
        extends AbstractVariableWidthBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceArrayBlock.class).instanceSize();
    private static final int SLICE_INSTANCE_SIZE = ClassLayout.parseClass(Slice.class).instanceSize();
    private final int positionCount;
    private final Slice[] values;
    private final long sizeInBytes;
    private final long retainedSizeInBytes;

    public SliceArrayBlock(int positionCount, Slice[] values)
    {
        this(positionCount, values, false);
    }

    public SliceArrayBlock(int positionCount, Slice[] values, boolean valueSlicesAreDistinct)
    {
        this.positionCount = positionCount;

        if (values.length < positionCount) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }
        this.values = values;

        sizeInBytes = getSliceArraySizeInBytes(values, 0, values.length);
        // We use IdentityHashMap for reference counting below to do proper memory accounting.
        // Since IdentityHashMap uses linear probing, depending on the load factor threads going through the
        // retained size calculation method will make multiple probes in the hash table, which will consume some cycles.
        // We see that many threads spend cycles probing the hash table when they read the dictionary data and
        // wrap that in a SliceArrayBlock (in SliceDictionaryStreamReader). Therefore, we avoid going through
        // the IdentityHashMap for that code path with the valueSlicesAreDistinct flag.
        retainedSizeInBytes = INSTANCE_SIZE + getSliceArrayRetainedSizeInBytes(values, valueSlicesAreDistinct);
    }

    public Slice[] getValues()
    {
        return values;
    }

    @Override
    protected Slice getRawSlice(int position)
    {
        return values[position];
    }

    @Override
    protected int getPositionOffset(int position)
    {
        return 0;
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return values[position] == null;
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new SliceArrayBlockEncoding();
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        checkValidPositions(positions, positionCount);

        Slice[] newValues = new Slice[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            if (!isEntryNull(positions.get(i))) {
                newValues[i] = Slices.copyOf(values[positions.get(i)]);
            }
        }
        return new SliceArrayBlock(positions.size(), newValues);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSliceLength(int position)
    {
        return values[position].length();
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(values, retainedSizeInBytes - INSTANCE_SIZE);
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        Slice[] newValues = Arrays.copyOfRange(values, positionOffset, positionOffset + length);
        return new SliceArrayBlock(length, newValues);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        return new SliceArrayBlock(length, deepCopyAndCompact(values, positionOffset, length));
    }

    static Slice[] deepCopyAndCompact(Slice[] values, int positionOffset, int length)
    {
        Slice[] newValues = Arrays.copyOfRange(values, positionOffset, positionOffset + length);
        // Compact the slices. Use an IdentityHashMap because this could be very expensive otherwise.
        Map<Slice, Slice> distinctValues = new IdentityHashMap<>();
        for (int i = 0; i < newValues.length; i++) {
            Slice slice = newValues[i];
            if (slice == null || slice.isCompact()) {
                continue;
            }
            Slice distinct = distinctValues.get(slice);
            if (distinct == null) {
                distinct = Slices.copyOf(slice);
                distinctValues.put(slice, distinct);
            }
            newValues[i] = distinct;
        }
        return newValues;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("SliceArrayBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    private static long getSliceArraySizeInBytes(Slice[] values, int offset, int length)
    {
        long sizeInBytes = 0;
        for (int i = offset; i < offset + length; i++) {
            Slice value = values[i];
            if (value != null) {
                sizeInBytes += value.length();
            }
        }
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset == 0 && length == positionCount) {
            // Calculation of getRegionSizeInBytes is expensive in this class.
            // On the other hand, getSizeInBytes result is pre-computed.
            return getSizeInBytes();
        }
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        return getSliceArraySizeInBytes(values, positionOffset, length);
    }

    private static long getSliceArrayRetainedSizeInBytes(Slice[] values, boolean valueSlicesAreDistinct)
    {
        if (valueSlicesAreDistinct) {
            return getDistinctSliceArrayRetainedSize(values);
        }
        return getSliceArrayRetainedSizeInBytes(values);
    }

    // when the slices are not distinct we need to do reference counting to calculate the total retained size
    private static long getSliceArrayRetainedSizeInBytes(Slice[] values)
    {
        long sizeInBytes = sizeOf(values);
        Map<Object, Boolean> uniqueRetained = new IdentityHashMap<>();
        for (Slice value : values) {
            if (value == null) {
                continue;
            }
            if (value.getBase() != null && uniqueRetained.put(value.getBase(), true) == null) {
                sizeInBytes += value.getRetainedSize();
            }
            else if (value != EMPTY_SLICE) {
                // EMPTY_SLICE is a singleton, so we don't account for the memory held onto by that instance.
                // Otherwise, we will be counting it multiple times.
                sizeInBytes += SLICE_INSTANCE_SIZE;
            }
        }
        return sizeInBytes;
    }

    private static long getDistinctSliceArrayRetainedSize(Slice[] values)
    {
        long sizeInBytes = sizeOf(values);
        for (Slice value : values) {
            // The check (value.getBase() == null) skips empty slices to be consistent with getSliceArrayRetainedSizeInBytes(values)
            if (value == null || value.getBase() == null) {
                continue;
            }
            sizeInBytes += value.getRetainedSize();
        }
        return sizeInBytes;
    }
}
