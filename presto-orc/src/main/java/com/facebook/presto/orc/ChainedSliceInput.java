package com.facebook.presto.orc;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ChainedSliceInput
        extends FixedLengthSliceInput
{
    private final List<Slice> slices;

    private final int sliceIndex;
    private final int slicePosition;

    private final long globalPosition;
    private final long length;


    public ChainedSliceInput(List<Slice> slices)
    {
        this.slices = ImmutableList.copyOf(requireNonNull(slices));
        this.length = slices.stream().mapToLong(Slice::length).sum();
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public long position()
    {
        return globalPosition;
    }

    @Override
    public void setPosition(long position)
    {
        checkArgument(position >= 0 && position < length);
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadable()
    {
        return globalPosition < length;
    }

    @Override
    public int available()
    {
        return toIntExact(length - globalPosition);
    }

    @Override
    public int read()
    {
        if (globalPosition >= length) {
            return -1;
        }

    }

    @Override
    public boolean readBoolean()
    {
        return false;
    }

    @Override
    public byte readByte()
    {
        return 0;
    }

    @Override
    public int readUnsignedByte()
    {
        return 0;
    }

    @Override
    public short readShort()
    {
        return 0;
    }

    @Override
    public int readUnsignedShort()
    {
        return 0;
    }

    @Override
    public int readInt()
    {
        return 0;
    }

    @Override
    public long readLong()
    {
        return 0;
    }

    @Override
    public float readFloat()
    {
        return 0;
    }

    @Override
    public double readDouble()
    {
        return 0;
    }

    @Override
    public Slice readSlice(int length)
    {
        return null;
    }

    @Override
    public int read(byte[] destination, int destinationIndex, int length)
    {
        return 0;
    }

    @Override
    public void readBytes(byte[] destination, int destinationIndex, int length)
    {

    }

    @Override
    public void readBytes(Slice destination, int destinationIndex, int length)
    {

    }

    @Override
    public void readBytes(OutputStream out, int length)
            throws IOException
    {

    }

    @Override
    public long skip(long length)
    {
        return 0;
    }

    @Override
    public int skipBytes(int length)
    {
        return 0;
    }

    @Override
    public long getRetainedSize()
    {
        return 0;
    }
}
