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

import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;

public class SliceArrayBlock
        extends AbstractVariableWidthBlock
{
    private final int positionCount;
    private final Slice[] values;
    private final int sizeInBytes;
    private final int retainedSizeInBytes;

    public SliceArrayBlock(int positionCount, Slice[] values)
    {
        this.positionCount = positionCount;

        if (values.length < positionCount) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }
        this.values = values;

        sizeInBytes = getSliceArraySizeInBytes(values);
        retainedSizeInBytes = getSliceArrayRetainedSizeInBytes(values);
    }

    Slice[] getValues()
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
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getLength(int position)
    {
        return values[position].length();
    }

    @Override
    public int getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
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
            if (slice == null) {
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

    static int getSliceArraySizeInBytes(Slice[] values)
    {
        long sizeInBytes = SizeOf.sizeOf(values);
        for (Slice value : values) {
            if (value != null) {
                sizeInBytes += value.length();
            }
        }
        if (sizeInBytes > Integer.MAX_VALUE) {
            sizeInBytes = Integer.MAX_VALUE;
        }
        return (int) sizeInBytes;
    }

    static int getSliceArrayRetainedSizeInBytes(Slice[] values)
    {
        long sizeInBytes = SizeOf.sizeOf(values);
        Map<Object, Boolean> uniqueRetained = new IdentityHashMap<>();
        for (Slice value : values) {
            if (value != null && value.getBase() != null && uniqueRetained.put(value.getBase(), true) == null) {
                sizeInBytes += value.getRetainedSize();
            }
        }
        if (sizeInBytes > Integer.MAX_VALUE) {
            sizeInBytes = Integer.MAX_VALUE;
        }
        return (int) sizeInBytes;
    }
}
