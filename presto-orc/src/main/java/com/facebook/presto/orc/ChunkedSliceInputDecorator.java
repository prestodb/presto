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

import com.google.common.base.Preconditions;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.ChunkedSliceInput;
import io.airlift.slice.ChunkedSliceInput.SliceLoader;
import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Decorator from ChunkedSliceInput since its marked final.
 */
public class ChunkedSliceInputDecorator
        extends FixedLengthSliceInput
{
    private final SliceLoader<?> loader;
    private final int bufferSize;
    private final ChunkedSliceInput input;

    public ChunkedSliceInputDecorator(SliceLoader<?> loader, int bufferSize)
    {
        this.loader = loader;
        this.bufferSize = bufferSize;
        this.input = new ChunkedSliceInput(loader, bufferSize);
    }

    @Override
    public long length()
    {
        return input.length();
    }

    @Override
    public long position()
    {
        return input.position();
    }

    @Override
    public void setPosition(long position)
    {
        input.setPosition(position);
    }

    @Override
    public boolean isReadable()
    {
        return input.isReadable();
    }

    @Override
    public int available()
    {
        return input.available();
    }

    public void ensureAvailable(int size)
    {
        input.ensureAvailable(size);
    }

    @Override
    public boolean readBoolean()
    {
        return input.readBoolean();
    }

    @Override
    public int read()
    {
        return input.read();
    }

    @Override
    public byte readByte()
    {
        return input.readByte();
    }

    @Override
    public int readUnsignedByte()
    {
        return input.readUnsignedByte();
    }

    @Override
    public short readShort()
    {
        return input.readShort();
    }

    @Override
    public int readUnsignedShort()
    {
        return input.readUnsignedShort();
    }

    @Override
    public int readInt()
    {
        return input.readInt();
    }

    @Override
    public long readLong()
    {
        return input.readLong();
    }

    @Override
    public float readFloat()
    {
        return input.readFloat();
    }

    @Override
    public double readDouble()
    {
        return input.readDouble();
    }

    @Override
    public Slice readSlice(int length)
    {
        return input.readSlice(length);
    }

    @Override
    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        readBytes(destination, destinationIndex, length);
    }

    @Override
    public int read(byte[] destination, int destinationIndex, int length)
    {
        return read(destination, destinationIndex, length);
    }

    @Override
    public void readBytes(byte[] destination, int destinationIndex, int length)
    {
        readBytes(destination, destinationIndex, length);
    }

    @Override
    public void readBytes(OutputStream out, int length) throws IOException
    {
        readBytes(out, length);
    }

    @Override
    public long skip(long length)
    {
        return input.skip(length);
    }

    @Override
    public int skipBytes(int length)
    {
        return input.skipBytes(length);
    }

    @Override
    public void close()
    {
        input.close();
    }

    @Override
    public String toString()
    {
        return input.toString();
    }

    public static FixedLengthSliceInput clone(FixedLengthSliceInput item)
    {
        if (item instanceof BasicSliceInput) {
            return new BasicSliceInput(((BasicSliceInput) item).slice());
        }

        Preconditions.checkArgument(item instanceof ChunkedSliceInputDecorator, "Unknown sliceInput type, not implemented.");
        ChunkedSliceInputDecorator csi = (ChunkedSliceInputDecorator) item;
        return new ChunkedSliceInputDecorator(csi.loader, csi.bufferSize);
    }
}
