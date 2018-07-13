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
import io.airlift.slice.ChunkedSliceInput;
import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.io.OutputStream;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ChainedSliceInput
        extends FixedLengthSliceInput
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ChainedSliceInput.class).instanceSize();

    private final List<Slice> slices;
    private int sliceIndex;
    private int slicePosition;
    private long globalPosition;
    private final long totalLength;

    private final long retainedSize;

    public ChainedSliceInput(List<Slice> slices)
    {
        this.slices = ImmutableList.copyOf(requireNonNull(slices));

        this.sliceIndex = 0;
        this.slicePosition = 0;
        this.globalPosition = 0;
        this.totalLength = slices.stream().mapToLong(Slice::length).sum();

        this.retainedSize = INSTANCE_SIZE + slices.stream().mapToLong(Slice::getRetainedSize).sum();
    }

    @Override
    public long length()
    {
        return totalLength;
    }

    @Override
    public long position()
    {
        return globalPosition;
    }

    @Override
    public void setPosition(long position)
    {
        if (position < 0 || position > totalLength) {
            throw new IndexOutOfBoundsException();
        }

        sliceIndex = 0;
        while (position >= slices.get(sliceIndex).length()) {
            position -= slices.get(sliceIndex).length();
            sliceIndex++;
        }
        slicePosition = toIntExact(position);
        globalPosition = position;
    }

    @Override
    public boolean isReadable()
    {
        return globalPosition < totalLength;
    }

    @Override
    public int available()
    {
        return toIntExact(totalLength - globalPosition);
    }

    @Override
    public int read()
    {
        if (globalPosition >= totalLength) {
            return -1;
        }

        int result = slices.get(sliceIndex).getByte(slicePosition) & 0xFF;
        slicePosition++;
        globalPosition++;
        advanceToNextSliceIfFeasible();
        return result;
    }

    @Override
    public boolean readBoolean()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte readByte()
    {
        int value = read();
        if (value == -1) {
            throw new IndexOutOfBoundsException();
        }
        return (byte) value;
    }

    @Override
    public int readUnsignedByte()
    {
        return readByte() & 0xFF;
    }

    @Override
    public short readShort()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readUnsignedShort()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readInt()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long readLong()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public float readFloat()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double readDouble()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice readSlice(int length)
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }

        // simple case, return part of the current slice
        if (slicePosition + length <= slices.get(sliceIndex).length()) {
            Slice newSlice = slices.get(sliceIndex).slice(slicePosition, length);
            slicePosition += length;
            globalPosition += length;
            advanceToNextSliceIfFeasible();
            return newSlice;
        }

        Slice newSlice = Slices.allocate(length);
        readBytes(newSlice, 0, length);
        return newSlice;
    }

    @Override
    public int read(byte[] destination, int destinationIndex, int length)
    {
        if (available() == 0) {
            return -1;
        }

        Slice currentSlice = slices.get(sliceIndex);
        int bytesRead = Math.min(currentSlice.length() - slicePosition, length);

        currentSlice.getBytes(slicePosition, destination, destinationIndex, bytesRead);
        slicePosition += bytesRead;
        globalPosition += bytesRead;
        advanceToNextSliceIfFeasible();

        return bytesRead;
    }

    @Override
    public void readBytes(byte[] destination, int destinationIndex, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        if (length > available()) {
            throw new IndexOutOfBoundsException();
        }

        while (length > 0) {
            Slice currentSlice = slices.get(sliceIndex);
            int batchSize = Math.min(currentSlice.length() - slicePosition, length);

            currentSlice.getBytes(slicePosition, destination, destinationIndex, batchSize);
            slicePosition += batchSize;
            globalPosition += batchSize;
            advanceToNextSliceIfFeasible();
            destinationIndex += batchSize;
            length -= batchSize;
        }
    }

    @Override
    public void readBytes(OutputStream out, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long skip(long length)
    {
        length = Math.min(length, available());

        int remainedLength = toIntExact(length);
        while (remainedLength > 0) {
            Slice currentSlice = slices.get(sliceIndex);
            int batchSize = toIntExact(Math.min(currentSlice.length() - slicePosition, length));

            sliceIndex++;
            slicePosition = 0;
            globalPosition += batchSize;
            remainedLength -= batchSize;
        }
        return length;
    }

    @Override
    public int skipBytes(int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getRetainedSize()
    {
        return retainedSize;
    }

    private void advanceToNextSliceIfFeasible()
    {
        checkState(slicePosition <= slices.get(sliceIndex).length());
        if (slicePosition == slices.get(sliceIndex).length()) {
            sliceIndex++;
            slicePosition = 0;
        }
    }
}
