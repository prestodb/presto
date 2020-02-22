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

public class AlluxioCachingInputStream
        extends FileInStream
{
    private final FSDataInputStream mIn;

    public AlluxioCachingInputStream(FSDataInputStream in)
    {
        mIn = in;
    }

    @Override
    public int read(byte[] b)
            throws IOException
    {
        return mIn.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        return mIn.read(b, off, len);
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        return mIn.skip(n);
    }

    @Override
    public int available()
            throws IOException
    {
        return mIn.available();
    }

    @Override
    public void close()
            throws IOException
    {
        mIn.close();
    }

    @Override
    public synchronized void mark(int readlimit)
    {
        mIn.mark(readlimit);
    }

    @Override
    public synchronized void reset()
            throws IOException
    {
        mIn.reset();
    }

    @Override
    public boolean markSupported()
    {
        return mIn.markSupported();
    }

    @Override
    public void seek(long pos)
            throws IOException
    {
        mIn.seek(pos);
    }

    @Override
    public long getPos()
            throws IOException
    {
        return mIn.getPos();
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
        return mIn.read(position, buffer, offset, length);
    }

    @Override
    public int read()
            throws IOException
    {
        return mIn.read();
    }
}
