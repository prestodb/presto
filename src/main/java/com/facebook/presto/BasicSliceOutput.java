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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

public class BasicSliceOutput extends SliceOutput
{
    private final Slice slice;
    private int size;

    protected BasicSliceOutput(Slice slice)
    {
        this.slice = slice;
    }

    @Override
    public void reset()
    {
        size = 0;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public boolean isWritable()
    {
        return writableBytes() > 0;
    }

    @Override
    public int writableBytes()
    {
        return slice.length() - size;
    }

    @Override
    public void writeByte(int value)
    {
        slice.setByte(size++, value);
    }

    @Override
    public void writeShort(int value)
    {
        slice.setShort(size, value);
        size += 2;
    }

    @Override
    public void writeInt(int value)
    {
        slice.setInt(size, value);
        size += 4;
    }

    @Override
    public void writeLong(long value)
    {
        slice.setLong(size, value);
        size += 8;
    }

    @Override
    public void writeBytes(byte[] source, int sourceIndex, int length)
    {
        slice.setBytes(size, source, sourceIndex, length);
        size += length;
    }

    @Override
    public void writeBytes(byte[] source)
    {
        writeBytes(source, 0, source.length);
    }

    @Override
    public void writeBytes(Slice source)
    {
        writeBytes(source, 0, source.length());
    }

    @Override
    public void writeBytes(SliceInput source, int length)
    {
        if (length > source.available()) {
            throw new IndexOutOfBoundsException();
        }
        writeBytes(source.readBytes(length));
    }

    @Override
    public void writeBytes(Slice source, int sourceIndex, int length)
    {
        slice.setBytes(size, source, sourceIndex, length);
        size += length;
    }

    @Override
    public void writeBytes(ByteBuffer source)
    {
        int length = source.remaining();
        slice.setBytes(size, source);
        size += length;
    }

    @Override
    public int writeBytes(InputStream in, int length)
            throws IOException
    {
        int writtenBytes = slice.setBytes(size, in, length);
        if (writtenBytes > 0) {
            size += writtenBytes;
        }
        return writtenBytes;
    }

    @Override
    public int writeBytes(ScatteringByteChannel in, int length)
            throws IOException
    {
        int writtenBytes = slice.setBytes(size, in, length);
        if (writtenBytes > 0) {
            size += writtenBytes;
        }
        return writtenBytes;
    }

    @Override
    public int writeBytes(FileChannel in, int position, int length)
            throws IOException
    {
        int writtenBytes = slice.setBytes(size, in, position, length);
        if (writtenBytes > 0) {
            size += writtenBytes;
        }
        return writtenBytes;
    }

    @Override
    public void writeZero(int length)
    {
        if (length == 0) {
            return;
        }
        if (length < 0) {
            throw new IllegalArgumentException(
                    "length must be 0 or greater than 0.");
        }
        int nLong = length >>> 3;
        int nBytes = length & 7;
        for (int i = nLong; i > 0; i--) {
            writeLong(0);
        }
        if (nBytes == 4) {
            writeInt(0);
        }
        else if (nBytes < 4) {
            for (int i = nBytes; i > 0; i--) {
                writeByte((byte) 0);
            }
        }
        else {
            writeInt(0);
            for (int i = nBytes - 4; i > 0; i--) {
                writeByte((byte) 0);
            }
        }
    }

    @Override
    public Slice slice()
    {
        return slice.slice(0, size);
    }

    @Override
    public ByteBuffer toByteBuffer()
    {
        return slice.toByteBuffer(0, size);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + '(' +
                "size=" + size + ", " +
                "capacity=" + slice.length() +
                ')';
    }

    public String toString(Charset charset)
    {
        return slice.toString(0, size, charset);
    }
}
