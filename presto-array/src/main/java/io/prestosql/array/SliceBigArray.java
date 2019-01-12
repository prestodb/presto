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
package io.prestosql.array;

import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

public final class SliceBigArray
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceBigArray.class).instanceSize();
    private static final int SLICE_INSTANCE_SIZE = ClassLayout.parseClass(Slice.class).instanceSize();
    private final ObjectBigArray<Slice> array;
    private final ReferenceCountMap trackedSlices = new ReferenceCountMap();
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
        return INSTANCE_SIZE + array.sizeOf() + sizeOfSlices + trackedSlices.sizeOf();
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
        updateRetainedSize(index, value);
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

    private void updateRetainedSize(long index, Slice value)
    {
        Slice currentValue = array.get(index);
        if (currentValue != null) {
            int baseReferenceCount = trackedSlices.decrementAndGet(currentValue.getBase());
            int sliceReferenceCount = trackedSlices.decrementAndGet(currentValue);
            if (baseReferenceCount == 0) {
                // it is the last referenced base
                sizeOfSlices -= currentValue.getRetainedSize();
            }
            else if (sliceReferenceCount == 0) {
                // it is the last referenced slice
                sizeOfSlices -= SLICE_INSTANCE_SIZE;
            }
        }
        if (value != null) {
            int baseReferenceCount = trackedSlices.incrementAndGet(value.getBase());
            int sliceReferenceCount = trackedSlices.incrementAndGet(value);
            if (baseReferenceCount == 1) {
                // it is the first referenced base
                sizeOfSlices += value.getRetainedSize();
            }
            else if (sliceReferenceCount == 1) {
                // it is the first referenced slice
                sizeOfSlices += SLICE_INSTANCE_SIZE;
            }
        }
    }
}
