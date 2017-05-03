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
package com.facebook.presto.array;

import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

public final class SliceBigArray
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceBigArray.class).instanceSize();
    private static final int SLICE_INSTANCE_SIZE = ClassLayout.parseClass(Slice.class).instanceSize();
    private final ObjectBigArray<Slice> array;
    private long sizeOfSlices;

    public SliceBigArray()
    {
        array = new ObjectBigArray<>();
    }

    public SliceBigArray(Slice slice)
    {
        array = new ObjectBigArray<>(slice);
    }

    /**
     * Returns the size of this big array in bytes.
     */
    public long sizeOf()
    {
        return INSTANCE_SIZE + array.sizeOf() + sizeOfSlices;
    }

    /**
     * Returns the element of this big array at specified index.
     *
     * @param index a position in this big array.
     * @return the element of this big array at the specified position.
     */
    public Slice get(long index)
    {
        return array.get(index);
    }

    /**
     * Sets the element of this big array at specified index.
     *
     * @param index a position in this big array.
     */
    public void set(long index, Slice value)
    {
        Slice currentValue = array.get(index);
        if (currentValue != null) {
            sizeOfSlices -= getSize(currentValue);
        }
        if (value != null) {
            sizeOfSlices += getSize(value);
        }
        array.set(index, value);
    }

    // For now we approximate the retained size of a slice by object overhead plus length.
    // In general this is more complicated than that as there may be multiple "view" slices
    // pointing to the same backing memory of a slice.
    private long getSize(Slice slice)
    {
        return slice.length() + SLICE_INSTANCE_SIZE;
    }

    /**
     * Ensures this big array is at least the specified length.  If the array is smaller, segments
     * are added until the array is larger then the specified length.
     */
    public void ensureCapacity(long length)
    {
        array.ensureCapacity(length);
    }
}
