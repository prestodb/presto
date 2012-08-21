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
package com.facebook.presto;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;

import static com.facebook.presto.SizeOf.SIZE_OF_BYTE;
import static com.facebook.presto.SizeOf.SIZE_OF_INT;
import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.SizeOf.SIZE_OF_SHORT;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Little Endian slice of a byte array.
 */
public final class ByteArraySlice
        implements Slice
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
        Preconditions.checkPositionIndexes(index, index + SIZE_OF_BYTE, this.length);
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
        Preconditions.checkPositionIndexes(index, index + SIZE_OF_SHORT, this.length);
        index += offset;
        return (short) (data[index] & 0xFF | data[index + 1] << 8);
    }

    @Override
    public int getInt(int index)
    {
        Preconditions.checkPositionIndexes(index, index + SIZE_OF_INT, this.length);
        index += offset;
        return (data[index] & 0xff) |
                (data[index + 1] & 0xff) << 8 |
                (data[index + 2] & 0xff) << 16 |
                (data[index + 3] & 0xff) << 24;
    }

    @Override
    public long getLong(int index)
    {
        Preconditions.checkPositionIndexes(index, index + SIZE_OF_LONG, this.length);
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
    public void getBytes(int index, Slice destination, int destinationIndex, int length)
    {
        destination.setBytes(destinationIndex, this, index, length);
    }

    @Override
    public void getBytes(int index, byte[] destination, int destinationIndex, int length)
    {
        Preconditions.checkPositionIndexes(index, index + length, this.length);
        Preconditions.checkPositionIndexes(destinationIndex, destinationIndex + length, destination.length);
        index += offset;
        System.arraycopy(data, index, destination, destinationIndex, length);
    }

    @Override
    public byte[] getBytes()
    {
        return getBytes(0, length);
    }

    @Override
    public byte[] getBytes(int index, int length)
    {
        Preconditions.checkPositionIndexes(index, index + length, this.length);
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
        destination.put(data, index, Math.min(length, destination.remaining()));
    }

    @Override
    public void getBytes(int index, OutputStream out, int length)
            throws IOException
    {
        Preconditions.checkPositionIndexes(index, index + length, this.length);
        index += offset;
        out.write(data, index, length);
    }

    @Override
    public int getBytes(int index, GatheringByteChannel out, int length)
            throws IOException
    {
        Preconditions.checkPositionIndexes(index, index + length, this.length);
        index += offset;
        return out.write(ByteBuffer.wrap(data, index, length));
    }

    @Override
    public void setShort(int index, int value)
    {
        Preconditions.checkPositionIndexes(index, index + SIZE_OF_SHORT, this.length);
        index += offset;
        data[index] = (byte) (value);
        data[index + 1] = (byte) (value >>> 8);
    }

    @Override
    public void setInt(int index, int value)
    {
        Preconditions.checkPositionIndexes(index, index + SIZE_OF_INT, this.length);
        index += offset;
        data[index] = (byte) (value);
        data[index + 1] = (byte) (value >>> 8);
        data[index + 2] = (byte) (value >>> 16);
        data[index + 3] = (byte) (value >>> 24);
    }

    @Override
    public void setLong(int index, long value)
    {
        Preconditions.checkPositionIndexes(index, index + SIZE_OF_LONG, this.length);
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
    public void setByte(int index, int value)
    {
        Preconditions.checkPositionIndexes(index, index + SIZE_OF_BYTE, this.length);
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
            // TODO: this is slow
            setBytes(index, source.getBytes(), sourceIndex, length);
        }
    }

    @Override
    public void setBytes(int index, byte[] source, int sourceIndex, int length)
    {
        Preconditions.checkPositionIndexes(index, index + length, this.length);
        Preconditions.checkPositionIndexes(sourceIndex, sourceIndex + length, source.length);
        index += offset;
        System.arraycopy(source, sourceIndex, data, index, length);
    }

    @Override
    public void setBytes(int index, ByteBuffer source)
    {
        Preconditions.checkPositionIndexes(index, index + source.remaining(), this.length);
        index += offset;
        source.get(data, index, source.remaining());
    }

    @Override
    public int setBytes(int index, InputStream in, int length)
            throws IOException
    {
        Preconditions.checkPositionIndexes(index, index + length, this.length);
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
    public int setBytes(int index, ScatteringByteChannel in, int length)
            throws IOException
    {
        Preconditions.checkPositionIndexes(index, index + length, this.length);
        index += offset;
        ByteBuffer buf = ByteBuffer.wrap(data, index, length);
        int readBytes = 0;

        do {
            int localReadBytes;
            try {
                localReadBytes = in.read(buf);
            }
            catch (ClosedChannelException e) {
                localReadBytes = -1;
            }
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
        Preconditions.checkPositionIndexes(index, index + length, this.length);
        index += offset;
        ByteBuffer buf = ByteBuffer.wrap(data, index, length);
        int readBytes = 0;

        do {
            int localReadBytes;
            try {
                localReadBytes = in.read(buf, position + readBytes);
            }
            catch (ClosedChannelException e) {
                localReadBytes = -1;
            }
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
    public Slice copySlice()
    {
        return copySlice(0, length);
    }

    @Override
    public Slice copySlice(int index, int length)
    {
        Preconditions.checkPositionIndexes(index, index + length, this.length);

        index += offset;
        byte[] copiedArray = new byte[length];
        System.arraycopy(data, index, copiedArray, 0, length);
        return new ByteArraySlice(copiedArray);
    }

    @Override
    public Slice slice()
    {
        return slice(0, length);
    }

    @Override
    public ByteArraySlice slice(int index, int length)
    {
        if (index == 0 && length == this.length) {
            return this;
        }

        Preconditions.checkPositionIndexes(index, index + length, this.length);
        if (index >= 0 && length == 0) {
            return Slices.EMPTY_SLICE;
        }
        return new ByteArraySlice(data, offset + index, length);
    }

    @Override
    public SliceInput input()
    {
        return new SliceInput(this);
    }

    @Override
    public SliceOutput output()
    {
        return new BasicSliceOutput(this);
    }

    @Override
    public ByteBuffer toByteBuffer()
    {
        return toByteBuffer(0, length);
    }

    @Override
    public ByteBuffer toByteBuffer(int index, int length)
    {
        Preconditions.checkPositionIndexes(index, index + length, this.length);
        index += offset;
        return ByteBuffer.wrap(data, index, length).order(LITTLE_ENDIAN);
    }

    @Override
    public String toString(Charset charset)
    {
        return toString(0, length, charset);
    }

    @Override
    public String toString(int index, int length, Charset charset)
    {
        if (length == 0) {
            return "";
        }
        return Slices.decodeString(toByteBuffer(index, length), charset);
    }

    @Override
    public int compareTo(Slice that)
    {
        if (that instanceof ByteArraySlice) {
            return compareTo((ByteArraySlice) that);
        }
        // TODO: this is slow
        return compareTo(new ByteArraySlice(that.getBytes()));
    }

    private int compareTo(ByteArraySlice that)
    {
        if (this == that) {
            return 0;
        }
        if (this.data == that.data && length == that.length && offset == that.offset) {
            return 0;
        }

        int minLength = Math.min(this.length, that.length);
        for (int i = 0; i < minLength; i++) {
            int thisByte = 0xFF & this.data[this.offset + i];
            int thatByte = 0xFF & that.data[that.offset + i];
            if (thisByte != thatByte) {
                return (thisByte) - (thatByte);
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

        // TODO: this is slow
        Slice slice = (Slice) o;
        return (length == slice.length()) &&
                Arrays.equals(getBytes(), slice.getBytes());
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
    public String toString()
    {
        return getClass().getSimpleName() + '(' + "length=" + length() + ')';
    }
}
