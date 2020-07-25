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
package com.facebook.presto.cache.alluxio;

import alluxio.exception.ExceptionMessage;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

class ByteArraySeekableStream
        extends InputStream
        implements Seekable, PositionedReadable
{
    private final ByteArrayInputStream inputStream;
    private final int length;

    public ByteArraySeekableStream(byte[] bytes)
    {
        inputStream = new ByteArrayInputStream(requireNonNull(bytes, "Input byte array is null"));
        length = bytes.length;
    }

    @Override
    public int read()
    {
        return inputStream.read();
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        return read(buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        int totalBytesRead = 0;
        while (totalBytesRead < length) {
            int bytesRead = read(position + totalBytesRead, buffer, offset + totalBytesRead, length - totalBytesRead);
            if (bytesRead == -1) {
                throw new EOFException();
            }
            totalBytesRead += bytesRead;
        }
    }

    @Override
    public void readFully(long position, byte[] buffer)
            throws IOException
    {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        try {
            inputStream.reset();
            inputStream.skip(position);
        }
        catch (IllegalArgumentException e) {
            // convert back to IOException
            throw new IOException(e);
        }
    }

    @Override
    public long getPos()
    {
        return length - inputStream.available();
    }

    @Override
    public boolean seekToNewSource(long targetPosition)
            throws IOException
    {
        throw new IOException(ExceptionMessage.NOT_SUPPORTED.getMessage());
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
        super.close();
    }
}
