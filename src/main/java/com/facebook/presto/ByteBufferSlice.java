package com.facebook.presto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import static com.facebook.presto.SizeOf.SIZE_OF_BYTE;
import static com.facebook.presto.SizeOf.SIZE_OF_INT;
import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.SizeOf.SIZE_OF_SHORT;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static java.lang.Math.min;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Little Endian slice of a {@link ByteBuffer}.
 */
public final class ByteBufferSlice
        extends AbstractSlice
{
    private final ByteBuffer buffer;

    private int hash;

    public ByteBufferSlice(ByteBuffer buffer)
    {
        this.buffer = buffer.slice().order(LITTLE_ENDIAN);
    }

    @Override
    public int length()
    {
        return buffer.capacity();
    }

    @Override
    public byte getByte(int index)
    {
        checkIndexLength(index, SIZE_OF_BYTE);
        return buffer.get(index);
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
        return buffer.getShort(index);
    }

    @Override
    public int getInt(int index)
    {
        checkIndexLength(index, SIZE_OF_INT);
        return buffer.getInt(index);
    }

    @Override
    public long getLong(int index)
    {
        checkIndexLength(index, SIZE_OF_LONG);
        return buffer.getLong(index);
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
        buffer.get(destination, destinationIndex, length);
    }

    @Override
    public byte[] getBytes(int index, int length)
    {
        byte[] bytes = new byte[length];
        toByteBuffer(index, length).get(bytes);
        return bytes;
    }

    @Override
    public void getBytes(int index, ByteBuffer destination)
    {
        int length = min(length() - index, destination.remaining());
        destination.put(toByteBuffer(index, length));
    }

    @Override
    public void getBytes(int index, OutputStream out, int length)
            throws IOException
    {
        checkIndexLength(index, length);
        if (buffer.hasArray()) {
            out.write(buffer.array(), buffer.arrayOffset() + index, length);
        }
        else {
            writeBuffer(toByteBuffer(index, length), out);
        }
    }

    private static void writeBuffer(ByteBuffer source, OutputStream out)
            throws IOException
    {
        byte[] bytes = new byte[4096];
        while (source.remaining() > 0) {
            int count = min(source.remaining(), bytes.length);
            source.get(bytes, 0, count);
            out.write(bytes, 0, count);
        }
    }

    @Override
    public int getBytes(int index, WritableByteChannel out, int length)
            throws IOException
    {
        return out.write(toByteBuffer(index, length));
    }

    @Override
    public void setShort(int index, int value)
    {
        buffer.putShort(index, (short) (value & 0xFFFF));
    }

    @Override
    public void setInt(int index, int value)
    {
        buffer.putInt(index, value);
    }

    @Override
    public void setLong(int index, long value)
    {
        buffer.putLong(index, value);
    }

    @Override
    public void setByte(int index, int value)
    {
        buffer.put(index, (byte) (value & 0xFF));
    }

    @Override
    public void setBytes(int index, Slice source, int sourceIndex, int length)
    {
        ByteBuffer src = source.toByteBuffer(sourceIndex, length);
        ByteBuffer dst = toByteBuffer(index, length);
        dst.put(src);
    }

    @Override
    public void setBytes(int index, byte[] source, int sourceIndex, int length)
    {
        checkPositionIndexes(sourceIndex, sourceIndex + length, source.length);
        toByteBuffer(index, length).put(source, sourceIndex, length);
    }

    @Override
    public void setBytes(int index, ByteBuffer source)
    {
        checkIndexLength(index, source.remaining());
        ByteBuffer dst = buffer.duplicate();
        dst.position(index);
        dst.put(source);
    }

    @Override
    public int setBytes(int index, InputStream in, int length)
            throws IOException
    {
        checkIndexLength(index, length);
        byte[] bytes = new byte[4096];
        ByteBuffer dst = toByteBuffer(index, length);
        while (dst.remaining() > 0) {
            int n = in.read(bytes);
            if (n < 0) {
                if (dst.position() == 0) {
                    return -1;
                }
                break;
            }
            dst.put(bytes, 0, n);
        }
        return dst.position();
    }

    @Override
    public int setBytes(int index, ReadableByteChannel in, int length)
            throws IOException
    {
        ByteBuffer dst = toByteBuffer(index, length);
        while (dst.remaining() > 0) {
            int n = channelRead(in, dst);
            if (n < 0) {
                if (dst.position() == 0) {
                    return -1;
                }
                break;
            }
            else if (n == 0) {
                break;
            }
        }
        return dst.position();
    }

    @Override
    public int setBytes(int index, FileChannel in, int position, int length)
            throws IOException
    {
        ByteBuffer dst = toByteBuffer(index, length);
        while (dst.remaining() > 0) {
            int n = channelRead(in, dst, position + dst.position());
            if (n < 0) {
                if (dst.position() == 0) {
                    return -1;
                }
                break;
            }
            else if (n == 0) {
                break;
            }
        }
        return dst.position();
    }

    @Override
    public Slice copySlice(int index, int length)
    {
        ByteBuffer src = toByteBuffer(index, length);
        ByteBuffer dst = allocateSame(src);
        dst.put(src);
        return new ByteBufferSlice(dst);
    }

    private static ByteBuffer allocateSame(ByteBuffer source)
    {
        if (source.isDirect()) {
            return ByteBuffer.allocateDirect(source.capacity());
        }
        return ByteBuffer.allocate(source.capacity());
    }

    @Override
    public Slice slice(int index, int length)
    {
        if ((index == 0) && (length == length())) {
            return this;
        }
        checkIndexLength(index, length);
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        return new ByteBufferSlice(toByteBuffer(index, length));
    }

    @Override
    public ByteBuffer toByteBuffer(int index, int length)
    {
        checkIndexLength(index, length);
        ByteBuffer out = buffer.duplicate();
        out.position(index);
        out.limit(index + length);
        return out.slice().order(LITTLE_ENDIAN);
    }

    @Override
    public int compareTo(Slice that)
    {
        if (this == that) {
            return 0;
        }
        return compareByteBuffers(toByteBuffer(), that.toByteBuffer());
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

        Slice slice = (Slice) o;
        if (length() != slice.length()) {
            return false;
        }

        return buffer.equals(slice.toByteBuffer());
    }

    @Override
    public int hashCode()
    {
        if (hash != 0) {
            return hash;
        }

        // see definition in interface
        int result = 1;
        for (int i = 0; i < length(); i++) {
            result = (31 * result) + buffer.get(i);
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
        throw new UnsupportedOperationException("not yet implemented");
    }
}
