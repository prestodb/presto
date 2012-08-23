package com.facebook.presto;

import com.google.common.base.Objects;
import com.google.common.primitives.UnsignedBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkPositionIndexes;
import static java.lang.Math.min;

public abstract class AbstractSlice
        implements Slice
{
    @Override
    public byte[] getBytes()
    {
        return getBytes(0, length());
    }

    @Override
    public Slice copySlice()
    {
        return copySlice(0, length());
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
        return toByteBuffer(0, length());
    }

    @Override
    public String toString(Charset charset)
    {
        return toString(0, length(), charset);
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
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("length", length())
                .toString();
    }

    protected void checkIndexLength(int index, int length)
    {
        checkPositionIndexes(index, index + length, length());
    }

    protected static int channelRead(ReadableByteChannel in, ByteBuffer dst)
            throws IOException
    {
        try {
            return in.read(dst);
        }
        catch (ClosedChannelException e) {
            return -1;
        }
    }

    protected static int channelRead(FileChannel in, ByteBuffer dst, int position)
            throws IOException
    {
        try {
            return in.read(dst, position);
        }
        catch (ClosedChannelException e) {
            return -1;
        }
    }

    protected static int compareByteBuffers(ByteBuffer a, ByteBuffer b)
    {
        int length = min(a.capacity(), b.capacity());
        for (int i = 0; i < length; i++) {
            int v = UnsignedBytes.compare(a.get(i), b.get(i));
            if (v != 0) {
                return v;
            }
        }
        return a.capacity() - b.capacity();
    }
}
