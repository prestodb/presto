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

public final class SliceBigArray
{
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
        return array.sizeOf() + sizeOfSlices;
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
            sizeOfSlices -= currentValue.length();
        }
        if (value != null) {
            sizeOfSlices += value.length();
        }
        array.set(index, value);
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
