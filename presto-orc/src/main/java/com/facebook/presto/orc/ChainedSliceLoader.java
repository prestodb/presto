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
package com.facebook.presto.orc;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.ChunkedSliceInput.BufferReference;
import io.airlift.slice.ChunkedSliceInput.SliceLoader;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ChainedSliceLoader
        implements SliceLoader<BufferReference>
{
    private final List<Slice> slices;
    private final long totalLength;

    public ChainedSliceLoader(List<Slice> slices)
    {
        this.slices = ImmutableList.copyOf(requireNonNull(slices));
        this.totalLength = slices.stream().mapToLong(Slice::length).sum();
    }

    @Override
    public BufferReference createBuffer(int bufferSize)
    {
        Slice slice = Slices.allocate(bufferSize);
        return () -> slice;
    }

    @Override
    public long getSize()
    {
        return totalLength;
    }

    @Override
    public void load(long position, BufferReference bufferReference, int length)
    {
        if (position < 0 || position + length > totalLength) {
            throw new IllegalArgumentException();
        }

        int currentSliceIndex = 0;
        long globalOffset = 0;
        while (globalOffset + slices.get(currentSliceIndex).length() <= position) {
            globalOffset += slices.get(currentSliceIndex).length();
            currentSliceIndex++;
        }

        int currentSliceOffset = toIntExact(position - globalOffset);
        Slice outputSlice = bufferReference.getSlice();
        int outputSliceOffset = 0;
        while (length > 0) {
            Slice currentSlice = slices.get(currentSliceIndex);
            int loadSize = Math.min(currentSlice.length() - currentSliceOffset, length);

            currentSlice.getBytes(currentSliceOffset, outputSlice, outputSliceOffset, loadSize);
            length -= loadSize;
            outputSliceOffset += loadSize;

            // advance to the next slice
            currentSliceIndex++;
            currentSliceOffset = 0;
        }
    }

    @Override
    public void close()
    {
    }
}
