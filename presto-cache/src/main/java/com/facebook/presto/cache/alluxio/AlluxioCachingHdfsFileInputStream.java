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

import alluxio.client.file.FileInStream;
import alluxio.exception.ExceptionMessage;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class AlluxioCachingHdfsFileInputStream
        extends InputStream
        implements Seekable, PositionedReadable
{
    private final FileInStream inputStream;
    private final Optional<FSDataInputStream> dataTierInputStream;
    private final boolean cacheValidationEnabled;

    public AlluxioCachingHdfsFileInputStream(FileInStream inputStream)
    {
        this(inputStream, Optional.empty(), false);
    }

    public AlluxioCachingHdfsFileInputStream(
            FileInStream inputStream,
            Optional<FSDataInputStream> dataTierInputStream,
            boolean cacheValidationEnabled)
    {
        this.inputStream = requireNonNull(inputStream, "InputStream is null");
        this.dataTierInputStream = requireNonNull(dataTierInputStream, "DataTierInputStream is null");
        verify(!cacheValidationEnabled || dataTierInputStream.isPresent(), "data tier input need to be non-null for data validation.");
        this.cacheValidationEnabled = cacheValidationEnabled;
    }

    @Override
    public int read()
            throws IOException
    {
        int outByte = inputStream.read();
        if (cacheValidationEnabled) {
            verify(dataTierInputStream.get().read() == outByte, "corrupted buffer at position " + getPos());
        }
        return outByte;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        int bytes = inputStream.positionedRead(position, buffer, offset, length);
        if (cacheValidationEnabled) {
            byte[] validationBuffer = new byte[bytes];
            dataTierInputStream.get().read(position, validationBuffer, 0, bytes);
            for (int i = 0; i < bytes; i++) {
                verify(buffer[offset + i] == validationBuffer[i], "corrupted buffer at position " + i);
            }
        }
        return bytes;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        int totalBytesRead = 0;
        while (totalBytesRead < length) {
            int bytesRead = read(
                    position + totalBytesRead,
                    buffer,
                    offset + totalBytesRead,
                    length - totalBytesRead);
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
        inputStream.seek(position);
        if (cacheValidationEnabled) {
            dataTierInputStream.get().seek(position);
        }
    }

    @Override
    public long getPos()
            throws IOException
    {
        return inputStream.getPos();
    }

    @Override
    public boolean seekToNewSource(long target)
            throws IOException
    {
        throw new IOException(ExceptionMessage.NOT_SUPPORTED.getMessage());
    }
}
