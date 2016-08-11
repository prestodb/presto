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
package com.facebook.presto.server;

import io.airlift.slice.RuntimeIOException;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.primitives.Ints.checkedCast;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;

public final class InputStreamSliceInputAdapter
        extends SliceInput
{
    private static final int MINIMUM_CHUNK_SIZE = 4096;

    private final InputStream inputStream;

    private final byte[] buffer;
    private final Slice slice;
    /**
     * Offset of buffer within stream.
     */
    private long bufferOffset;
    /**
     * Current position for reading from buffer.
     */
    private int bufferPosition;

    private int bufferFill;

    public InputStreamSliceInputAdapter(InputStream inputStream, int bufferSize)
    {
        checkArgument(bufferSize >= MINIMUM_CHUNK_SIZE, "minimum buffer size of " + MINIMUM_CHUNK_SIZE + " required");
        if (inputStream == null) {
            throw new NullPointerException("inputStream is null");
        }

        this.inputStream = inputStream;
        this.buffer = new byte[bufferSize];
        this.slice = Slices.wrappedBuffer(buffer);
    }

    @Override
    public long position()
    {
        return checkedCast(bufferOffset + bufferPosition);
    }

    @Override
    public void setPosition(long position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadable()
    {
        return available() > 0;
    }

    @Override
    public int skipBytes(int n)
    {
        return (int) skip(n);
    }

    @Override
    public boolean readBoolean()
    {
        return readByte() != 0;
    }

    @Override
    public byte readByte()
    {
        ensureAvailable(SIZE_OF_BYTE);
        byte v = slice.getByte(bufferPosition);
        bufferPosition += SIZE_OF_BYTE;
        return v;
    }

    @Override
    public int readUnsignedByte()
    {
        return readByte() & 0xFF;
    }

    @Override
    public short readShort()
    {
        ensureAvailable(SIZE_OF_SHORT);
        short v = slice.getShort(bufferPosition);
        bufferPosition += SIZE_OF_SHORT;
        return v;
    }

    @Override
    public int readUnsignedShort()
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public int readInt()
    {
        ensureAvailable(SIZE_OF_INT);
        int v = slice.getInt(bufferPosition);
        bufferPosition += SIZE_OF_INT;
        return v;
    }

    @Override
    public long readLong()
    {
        ensureAvailable(SIZE_OF_LONG);
        long v = slice.getLong(bufferPosition);
        bufferPosition += SIZE_OF_LONG;
        return v;
    }

    @Override
    public float readFloat()
    {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble()
    {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public int read()
    {
        if (available() == 0) {
            return -1;
        }

        assert availableBytes() > 0;
        int v = slice.getByte(bufferPosition) & 0xFF;
        bufferPosition += SIZE_OF_BYTE;
        return v;
    }

    @Override
    public int read(byte[] destination, int destinationIndex, int length)
    {
        if (available() == 0) {
            return -1;
        }

        assert availableBytes() > 0;
        int batch = Math.min(availableBytes(), length);
        slice.getBytes(bufferPosition, destination, destinationIndex, batch);
        bufferPosition += batch;
        return batch;
    }

    @Override
    public long skip(long length)
    {
        int batch = availableBytes();
        // is skip within the current buffer?
        if (batch >= length) {
            bufferPosition += length;
            return length;
        }

        // drop current buffer
        bufferPosition = bufferFill;

        try {
            return batch + inputStream.skip(length - batch);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public int available()
    {
        if (bufferPosition < bufferFill) {
            return availableBytes();
        }

        return fillBuffer();
    }

    @Override
    public void close()
    {
        try {
            inputStream.close();
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public void readBytes(byte[] destination, int destinationIndex, int length)
    {
        while (length > 0) {
            int batch = Math.min(availableBytes(), length);
            slice.getBytes(bufferPosition, destination, destinationIndex, batch);

            bufferPosition += batch;
            destinationIndex += batch;
            length -= batch;

            ensureAvailable(Math.min(length, MINIMUM_CHUNK_SIZE));
        }
    }

    @Override
    public Slice readSlice(int length)
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }

        Slice newSlice = Slices.allocate(length);
        readBytes(newSlice, 0, length);
        return newSlice;
    }

    @Override
    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        while (length > 0) {
            int batch = Math.min(availableBytes(), length);
            slice.getBytes(bufferPosition, destination, destinationIndex, batch);

            bufferPosition += batch;
            destinationIndex += batch;
            length -= batch;

            ensureAvailable(Math.min(length, MINIMUM_CHUNK_SIZE));
        }
    }

    @Override
    public void readBytes(OutputStream out, int length)
            throws IOException
    {
        while (length > 0) {
            int batch = Math.min(availableBytes(), length);
            out.write(buffer, bufferPosition, batch);

            bufferPosition += batch;
            length -= batch;

            ensureAvailable(Math.min(length, MINIMUM_CHUNK_SIZE));
        }
    }

    private int availableBytes()
    {
        return bufferFill - bufferPosition;
    }

    private void ensureAvailable(int size)
    {
        if (bufferPosition + size < bufferFill) {
            return;
        }

        if (fillBuffer() < size) {
            throw new IndexOutOfBoundsException("End of stream");
        }
    }

    private int fillBuffer()
    {
        // Keep the rest
        int rest = bufferFill - bufferPosition;
        // Use System.arraycopy for small copies
        System.arraycopy(buffer, bufferPosition, buffer, 0, rest);

        bufferFill = rest;
        bufferOffset += bufferPosition;
        bufferPosition = 0;
        // Fill buffer with a minimum of bytes
        while (bufferFill < MINIMUM_CHUNK_SIZE) {
            try {
                int bytesRead = inputStream.read(buffer, bufferFill, buffer.length - bufferFill);
                if (bytesRead < 0) {
                    break;
                }

                bufferFill += bytesRead;
            }
            catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }

        return bufferFill;
    }
}
