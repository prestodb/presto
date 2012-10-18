/*
 * Copyright 2009 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.facebook.presto.slice;

import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

import static com.facebook.presto.SizeOf.SIZE_OF_BYTE;
import static com.facebook.presto.SizeOf.SIZE_OF_INT;
import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.SizeOf.SIZE_OF_SHORT;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static java.lang.Math.min;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Little Endian slice of a byte array.
 */
public final class ByteArraySlice
        extends AbstractSlice
{
    private final byte[] data;
    private final int offset;
    private final int length;

    private int hash;

    public ByteArraySlice(int length)
    {
        data = new byte[length];
        this.offset = 0;
        this.length = length;
    }

    public ByteArraySlice(byte[] data)
    {
        Preconditions.checkNotNull(data, "array is null");
        this.data = data;
        this.offset = 0;
        this.length = data.length;
    }

    public ByteArraySlice(byte[] data, int offset, int length)
    {
        Preconditions.checkNotNull(data, "array is null");
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public int length()
    {
        return length;
    }

    /**
     * Gets the array underlying this slice.
     */
    public byte[] getRawArray()
    {
        return data;
    }

    /**
     * Gets the offset of this slice in the underlying array.
     */
    public int getRawOffset()
    {
        return offset;
    }

    @Override
    public byte getByte(int index)
    {
        checkIndexLength(index, SIZE_OF_BYTE);
        index += offset;
        return data[index];
    }

    @Override
    public short getUnsignedByte(int index)
    {
        return (short) (getByte(index) & 0xFF);
    }

    @Override
    public short getShort(int index)
    {
        checkIndexLength(index, SIZE_OF_SHORT);
        index += offset;
        return (short) (data[index] & 0xFF | data[index + 1] << 8);
    }

    @Override
    public int getInt(int index)
    {
        checkIndexLength(index, SIZE_OF_INT);
        index += offset;
        return (data[index] & 0xff) |
                (data[index + 1] & 0xff) << 8 |
                (data[index + 2] & 0xff) << 16 |
                (data[index + 3] & 0xff) << 24;
    }

    @Override
    public long getLong(int index)
    {
        checkIndexLength(index, SIZE_OF_LONG);
        index += offset;
        return ((long) data[index] & 0xff) |
                ((long) data[index + 1] & 0xff) << 8 |
                ((long) data[index + 2] & 0xff) << 16 |
                ((long) data[index + 3] & 0xff) << 24 |
                ((long) data[index + 4] & 0xff) << 32 |
                ((long) data[index + 5] & 0xff) << 40 |
                ((long) data[index + 6] & 0xff) << 48 |
                ((long) data[index + 7] & 0xff) << 56;
    }

    @Override
    public double getDouble(int index)
    {
        return Double.longBitsToDouble(getLong(index));
    }

    @Override
    public void getBytes(int index, Slice destination, int destinationIndex, int length)
    {
        destination.setBytes(destinationIndex, this, index, length);
    }

    @Override
    public void getBytes(int index, byte[] destination, int destinationIndex, int length)
    {
        checkIndexLength(index, length);
        checkPositionIndexes(destinationIndex, destinationIndex + length, destination.length);
        index += offset;
        System.arraycopy(data, index, destination, destinationIndex, length);
    }

    @Override
    public byte[] getBytes(int index, int length)
    {
        checkIndexLength(index, length);
        index += offset;
        if (index == 0) {
            return Arrays.copyOf(data, length);
        }
        byte[] value = new byte[length];
        System.arraycopy(data, index, value, 0, length);
        return value;
    }

    @Override
    public void getBytes(int index, ByteBuffer destination)
    {
        Preconditions.checkPositionIndex(index, this.length);
        index += offset;
        destination.put(data, index, min(length, destination.remaining()));
    }

    @Override
    public void getBytes(int index, SliceOutput out, int length)
    {
        checkIndexLength(index, length);
        index += offset;
        out.writeBytes(this, index, length);
    }

    @Override
    public void getBytes(int index, OutputStream out, int length)
            throws IOException
    {
        checkIndexLength(index, length);
        index += offset;
        out.write(data, index, length);
    }

    @Override
    public int getBytes(int index, WritableByteChannel out, int length)
            throws IOException
    {
        checkIndexLength(index, length);
        index += offset;
        return out.write(ByteBuffer.wrap(data, index, length));
    }

    @Override
    public void setShort(int index, int value)
    {
        checkIndexLength(index, SIZE_OF_SHORT);
        index += offset;
        data[index] = (byte) (value);
        data[index + 1] = (byte) (value >>> 8);
    }

    @Override
    public void setInt(int index, int value)
    {
        checkIndexLength(index, SIZE_OF_INT);
        index += offset;
        data[index] = (byte) (value);
        data[index + 1] = (byte) (value >>> 8);
        data[index + 2] = (byte) (value >>> 16);
        data[index + 3] = (byte) (value >>> 24);
    }

    @Override
    public void setLong(int index, long value)
    {
        checkIndexLength(index, SIZE_OF_LONG);
        index += offset;
        data[index] = (byte) (value);
        data[index + 1] = (byte) (value >>> 8);
        data[index + 2] = (byte) (value >>> 16);
        data[index + 3] = (byte) (value >>> 24);
        data[index + 4] = (byte) (value >>> 32);
        data[index + 5] = (byte) (value >>> 40);
        data[index + 6] = (byte) (value >>> 48);
        data[index + 7] = (byte) (value >>> 56);
    }

    @Override
    public void setDouble(int index, double value)
    {
        setLong(index, Double.doubleToLongBits(value));
    }

    @Override
    public void setByte(int index, int value)
    {
        checkIndexLength(index, SIZE_OF_BYTE);
        index += offset;
        data[index] = (byte) value;
    }

    @Override
    public void setBytes(int index, Slice source, int sourceIndex, int length)
    {
        if (source instanceof ByteArraySlice) {
            ByteArraySlice src = (ByteArraySlice) source;
            setBytes(index, src.data, src.offset + sourceIndex, length);
        }
        else {
            ByteBuffer src = source.toByteBuffer(sourceIndex, length);
            ByteBuffer dst = toByteBuffer(index, length);
            dst.put(src);
        }
    }

    @Override
    public void setBytes(int index, byte[] source, int sourceIndex, int length)
    {
        checkIndexLength(index, length);
        checkPositionIndexes(sourceIndex, sourceIndex + length, source.length);
        index += offset;
        System.arraycopy(source, sourceIndex, data, index, length);
    }

    @Override
    public void setBytes(int index, ByteBuffer source)
    {
        checkIndexLength(index, source.remaining());
        index += offset;
        source.get(data, index, source.remaining());
    }

    @Override
    public int setBytes(int index, InputStream in, int length)
            throws IOException
    {
        checkIndexLength(index, length);
        index += offset;
        int readBytes = 0;
        do {
            int localReadBytes = in.read(data, index, length);
            if (localReadBytes < 0) {
                if (readBytes == 0) {
                    return -1;
                }
                break;
            }
            readBytes += localReadBytes;
            index += localReadBytes;
            length -= localReadBytes;
        }
        while (length > 0);

        return readBytes;
    }

    @Override
    public int setBytes(int index, ReadableByteChannel in, int length)
            throws IOException
    {
        checkIndexLength(index, length);
        index += offset;
        ByteBuffer buf = ByteBuffer.wrap(data, index, length);
        int readBytes = 0;

        do {
            int localReadBytes = channelRead(in, buf);
            if (localReadBytes < 0) {
                if (readBytes == 0) {
                    return -1;
                }
                break;
            }
            else if (localReadBytes == 0) {
                break;
            }
            readBytes += localReadBytes;
        }
        while (readBytes < length);

        return readBytes;
    }

    @Override
    public int setBytes(int index, FileChannel in, int position, int length)
            throws IOException
    {
        checkIndexLength(index, length);
        index += offset;
        ByteBuffer buf = ByteBuffer.wrap(data, index, length);
        int readBytes = 0;

        do {
            int localReadBytes = channelRead(in, buf, position + readBytes);
            if (localReadBytes < 0) {
                if (readBytes == 0) {
                    return -1;
                }
                break;
            }
            else if (localReadBytes == 0) {
                break;
            }
            readBytes += localReadBytes;
        }
        while (readBytes < length);

        return readBytes;
    }

    @Override
    public Slice copySlice(int index, int length)
    {
        checkIndexLength(index, length);

        index += offset;
        byte[] copiedArray = new byte[length];
        System.arraycopy(data, index, copiedArray, 0, length);
        return new ByteArraySlice(copiedArray);
    }

    @Override
    public ByteArraySlice slice(int index, int length)
    {
        if (index == 0 && length == this.length) {
            return this;
        }

        checkIndexLength(index, length);
        if (index >= 0 && length == 0) {
            return Slices.EMPTY_SLICE;
        }
        return new ByteArraySlice(data, offset + index, length);
    }

    @Override
    public ByteBuffer toByteBuffer(int index, int length)
    {
        checkIndexLength(index, length);
        index += offset;
        return ByteBuffer.wrap(data, index, length).slice().order(LITTLE_ENDIAN);
    }

    @Override
    public int compareTo(Slice that)
    {
        if (this == that) {
            return 0;
        }
        if (that instanceof ByteArraySlice) {
            return compareTo((ByteArraySlice) that);
        }
        return compareByteBuffers(toByteBuffer(), that.toByteBuffer());
    }

    private int compareTo(ByteArraySlice that)
    {
        if ((this.data == that.data) && (length == that.length) && (offset == that.offset)) {
            return 0;
        }

        int length = min(this.length, that.length);
        for (int i = 0; i < length; i++) {
            byte a = this.data[this.offset + i];
            byte b = that.data[this.offset + i];
            int v = UnsignedBytes.compare(a, b);
            if (v != 0) {
                return v;
            }
        }
        return this.length - that.length;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Slice)) {
            return false;
        }

        if (o instanceof ByteArraySlice) {
            return equals((ByteArraySlice) o);
        }

        Slice slice = (Slice) o;
        if (length == slice.length()) {
            return true;
        }

        return toByteBuffer().equals(slice.toByteBuffer());
    }

    private boolean equals(ByteArraySlice slice)
    {
        // do lengths match
        if (length != slice.length) {
            return false;
        }

        // if arrays have same base offset, some optimizations can be taken...
        if (offset == slice.offset && data == slice.data) {
            return true;
        }
        for (int i = 0; i < length; i++) {
            if (data[offset + i] != slice.data[slice.offset + i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        if (hash != 0) {
            return hash;
        }

        // see definition in interface
        int result = 1;
        for (int i = offset; i < offset + length; i++) {
            result = 31 * result + data[i];
        }
        if (result == 0) {
            result = 1;
        }

        hash = result;
        return hash;
    }

    @Override
    public boolean equals(int offset, int length, Slice other, int otherOffset, int otherLength)
    {
        if (length != otherLength) {
            return false;
        }

        if (!(other instanceof ByteArraySlice)) {
            throw new UnsupportedOperationException("not yet implemented");
        }

        ByteArraySlice that = (ByteArraySlice) other;

        while (length > 0) {
            byte thisByte = this.getByte(offset);
            byte thatByte = that.getByte(otherOffset);
            if (thisByte != thatByte) {
                return false;
            }
            offset++;
            otherOffset++;
            length--;
        }

        return true;
    }
}
