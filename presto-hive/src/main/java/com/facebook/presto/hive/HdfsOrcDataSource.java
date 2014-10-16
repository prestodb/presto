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
package com.facebook.presto.hive;

import com.facebook.presto.orc.DiskRange;
import com.facebook.presto.orc.OrcDataSource;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;

public class HdfsOrcDataSource
        implements OrcDataSource
{
    private final FSDataInputStream inputStream;
    private final String path;
    private long readTimeNanos;
    private long size;

    public HdfsOrcDataSource(String path, FSDataInputStream inputStream, long size)
            throws IOException
    {
        this.path = checkNotNull(path, "path is null");
        this.inputStream = checkNotNull(inputStream, "inputStream is null");
        this.size = size;
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
    }

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
    public void readFully(long position, byte[] buffer)
            throws IOException
    {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        long start = System.nanoTime();

        inputStream.readFully(position, buffer, bufferOffset, bufferLength);

        readTimeNanos += System.nanoTime() - start;
    }

    @Override
    public <K> Map<K, Slice> readFully(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        checkNotNull(diskRanges, "diskRanges is null");

        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        // merge ranges
        DiskRange fullRange = diskRanges.values().iterator().next();
        for (DiskRange diskRange : diskRanges.values()) {
            fullRange = fullRange.span(diskRange);
        }

        // read full range in one request
        byte[] buffer = new byte[fullRange.getLength()];
        readFully(fullRange.getOffset(), buffer);

        ImmutableMap.Builder<K, Slice> slices = ImmutableMap.builder();
        for (Entry<K, DiskRange> entry : diskRanges.entrySet()) {
            DiskRange diskRange = entry.getValue();
            int offset = Ints.checkedCast(diskRange.getOffset() - fullRange.getOffset());
            slices.put(entry.getKey(), Slices.wrappedBuffer(buffer, offset, diskRange.getLength()));
        }
        return slices.build();
    }

    @Override
    public String toString()
    {
        return path;
    }
}
