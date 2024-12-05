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
package com.facebook.presto.parquet.batchreader;

import io.airlift.slice.Slice;
import io.airlift.slice.UnsafeSlice;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static java.util.Objects.requireNonNull;

/**
 * Basic input stream based on a given Slice object.
 * This is a simpler version of BasicSliceInput with a few additional methods.
 * <p>
 * Note that methods starting with 'read' modify the underlying offset, while 'get' methods return
 * value without modifying the state
 */
public final class SimpleSliceInputStream
{
    private final Slice slice;
    private int offset;

    public SimpleSliceInputStream(Slice slice)
    {
        this(slice, 0);
    }

    public SimpleSliceInputStream(Slice slice, int offset)
    {
        this.slice = requireNonNull(slice, "slice is null");
        checkArgument(slice.length() == 0 || slice.hasByteArray(), "SimpleSliceInputStream only supports slices backed by byte array");
        this.offset = offset;
    }

    public byte readByte()
    {
        return slice.getByte(offset++);
    }

    public short readShort()
    {
        short value = slice.getShort(offset);
        offset += Short.BYTES;
        return value;
    }

    public int readInt()
    {
        int value = slice.getInt(offset);
        offset += Integer.BYTES;
        return value;
    }

    public long readLong()
    {
        long value = slice.getLong(offset);
        offset += Long.BYTES;
        return value;
    }

    public byte[] readBytes()
    {
        byte[] bytes = slice.getBytes();
        offset = slice.length();
        return bytes;
    }

    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        slice.getBytes(offset, destination, destinationIndex, length);
        offset += length;
    }

    public void skip(int n)
    {
        offset += n;
    }

    public Slice asSlice()
    {
        return slice.slice(offset, slice.length() - offset);
    }

    /**
     * Returns the byte array wrapped by this Slice.
     * Callers should take care to use {@link SimpleSliceInputStream#getByteArrayOffset()}
     * since the contents of this Slice may not start at array index 0.
     */
    public byte[] getByteArray()
    {
        return slice.byteArray();
    }

    /**
     * Returns the start index the content of this slice within the byte array wrapped by this slice.
     */
    public int getByteArrayOffset()
    {
        return offset + slice.byteArrayOffset();
    }

    public void ensureBytesAvailable(int bytes)
    {
        checkPositionIndexes(offset, offset + bytes, slice.length());
    }

    /**
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public int readIntUnsafe()
    {
        int value = UnsafeSlice.getIntUnchecked(slice, offset);
        offset += Integer.BYTES;
        return value;
    }

    /**
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public long readLongUnsafe()
    {
        long value = UnsafeSlice.getLongUnchecked(slice, offset);
        offset += Long.BYTES;
        return value;
    }

    /**
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public byte getByteUnsafe(int index)
    {
        return UnsafeSlice.getByteUnchecked(slice, offset + index);
    }

    /**
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public int getIntUnsafe(int index)
    {
        return UnsafeSlice.getIntUnchecked(slice, offset + index);
    }

    /**
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public long getLongUnsafe(int index)
    {
        return UnsafeSlice.getLongUnchecked(slice, offset + index);
    }
}
