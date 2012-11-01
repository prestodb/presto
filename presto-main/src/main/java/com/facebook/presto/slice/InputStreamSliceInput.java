package com.facebook.presto.slice;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import com.google.common.io.LimitInputStream;
import com.google.common.io.LittleEndianDataInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public final class InputStreamSliceInput extends SliceInput
{
    private final PushbackInputStream pushbackInputStream;
    private final CountingInputStream countingInputStream;
    private final LittleEndianDataInputStream dataInputStream;

    public InputStreamSliceInput(InputStream inputStream)
    {
        pushbackInputStream = new PushbackInputStream(inputStream);
        countingInputStream = new CountingInputStream(pushbackInputStream);
        dataInputStream = new LittleEndianDataInputStream(countingInputStream);
    }

    @Override
    public int position()
    {
        return (int) countingInputStream.getCount();
    }

    @Override
    public void setPosition(int position)
    {
        throw new UnsupportedOperationException();
    }

    public boolean isReadable()
    {
        try {
            int value = pushbackInputStream.read();
            if (value == -1) {
                return false;
            }
            pushbackInputStream.unread(value);
            return true;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public int skipBytes(int n)
    {
        try {
            return dataInputStream.skipBytes(n);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public int readUnsignedByte()
    {
        try {
            return dataInputStream.readUnsignedByte();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public int readUnsignedShort()
    {
        try {
            return dataInputStream.readUnsignedShort();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public int readInt()
    {
        try {
            return dataInputStream.readInt();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public long readLong()
    {
        try {
            return dataInputStream.readLong();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public short readShort()

    {
        try {
            return dataInputStream.readShort();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public byte readByte()

    {
        try {
            return dataInputStream.readByte();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean readBoolean()

    {
        try {
            return dataInputStream.readBoolean();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public int read()

    {
        try {
            return dataInputStream.read();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public int read(byte[] b)

    {
        try {
            return dataInputStream.read(b);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len)

    {
        try {
            return dataInputStream.read(b, off, len);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public long skip(long n)

    {
        try {
            return dataInputStream.skip(n);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public int available()

    {
        try {
            return countingInputStream.available();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void close()

    {
        try {
            dataInputStream.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void readBytes(byte[] destination, int destinationIndex, int length)
    {
        readFully(destination, destinationIndex, length);
    }

    @Override
    public Slice readSlice(int length)
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        try {
            Slice newSlice = Slices.allocate(length);
            newSlice.setBytes(0, countingInputStream, length);
            return newSlice;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        try {
            destination.setBytes(destinationIndex, countingInputStream, length);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void readBytes(OutputStream out, int length)
            throws IOException
    {
        ByteStreams.copy(new LimitInputStream(countingInputStream, length), out);
    }

    @Override
    public void readBytes(ByteBuffer destination)
    {
        // this is too annoying to implement
        throw new UnsupportedOperationException();
    }

    @Override
    public int readBytes(GatheringByteChannel out, int length)
            throws IOException
    {
        // this is too annoying to implement
        throw new UnsupportedOperationException();
    }
}
