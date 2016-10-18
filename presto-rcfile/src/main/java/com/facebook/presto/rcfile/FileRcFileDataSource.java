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
package com.facebook.presto.rcfile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static java.util.Objects.requireNonNull;

public class FileRcFileDataSource
        implements RcFileDataSource
{
    private final File path;
    private final long size;
    private final RandomAccessFile input;
    private long readTimeNanos;
    private long readBytes;

    public FileRcFileDataSource(File path)
            throws IOException
    {
        this.path = requireNonNull(path, "path is null");
        this.size = path.length();
        this.input = new RandomAccessFile(path, "r");
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }

    @Override
    public long getReadBytes()
    {
        return readBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getSize()
    {
        return size;
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        long start = System.nanoTime();

        input.seek(position);
        input.readFully(buffer, bufferOffset, bufferLength);

        readTimeNanos += System.nanoTime() - start;
        readBytes += bufferLength;
    }

    @Override
    public String toString()
    {
        return path.getPath();
    }
}
