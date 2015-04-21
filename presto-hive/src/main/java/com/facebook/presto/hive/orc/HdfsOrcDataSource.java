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
package com.facebook.presto.hive.orc;

import com.facebook.presto.orc.AbstractOrcDataSource;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.EOFException;

public class HdfsOrcDataSource
        extends AbstractOrcDataSource
{
    private final FSDataInputStream inputStream;

    public HdfsOrcDataSource(String name, long size, DataSize maxMergeDistance, DataSize maxReadSize, DataSize streamBufferSize, FSDataInputStream inputStream)
    {
        super(name, size, maxMergeDistance, maxReadSize, streamBufferSize);
        this.inputStream = inputStream;
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        long start = System.nanoTime();
        inputStream.seek(position);
        int nread = 0;
        while (nread < bufferLength) {
            int nbytes = inputStream.read(buffer, bufferOffset + nread, bufferLength - nread);
            if (nbytes < 0) {
                throw new EOFException("End of file reached before reading fully.");
            }
            nread += nbytes;
        }
    }
}
