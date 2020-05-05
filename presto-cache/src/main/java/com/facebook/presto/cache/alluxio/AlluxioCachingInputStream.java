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
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class AlluxioCachingInputStream
        extends FileInStream
{
    private final FSDataInputStream input;

    public AlluxioCachingInputStream(FSDataInputStream input)
    {
        this.input = requireNonNull(input, "input is null");
    }

    @Override
    public int read(byte[] bytes)
            throws IOException
    {
        return input.read(bytes);
    }

    @Override
    public int read(byte[] bytes, int offset, int length)
            throws IOException
    {
        return input.read(bytes, offset, length);
    }

    @Override
    public long skip(long length)
            throws IOException
    {
        return input.skip(length);
    }

    @Override
    public int available()
            throws IOException
    {
        return input.available();
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }

    @Override
    public synchronized void mark(int limit)
    {
        input.mark(limit);
    }

    @Override
    public synchronized void reset()
            throws IOException
    {
        input.reset();
    }

    @Override
    public boolean markSupported()
    {
        return input.markSupported();
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        input.seek(position);
    }

    @Override
    public long getPos()
            throws IOException
    {
        return input.getPos();
    }

    @Override
    public long remaining()
    {
        throw new UnsupportedOperationException("Remaining is not supported");
    }

    @Override
    public int positionedRead(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        return input.read(position, buffer, offset, length);
    }

    @Override
    public int read()
            throws IOException
    {
        return input.read();
    }
}
