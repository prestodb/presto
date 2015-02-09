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

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.orc.OrcDataSourceUtils.getDiskRangeSlice;
import static com.facebook.presto.orc.OrcDataSourceUtils.mergeAdjacentDiskRanges;
import static com.google.common.base.Preconditions.checkNotNull;

public class FileOrcDataSource
        implements OrcDataSource
{
    private final File path;
    private final long size;
    private final RandomAccessFile input;
    private final DataSize maxMergeDistance;
    private long readTimeNanos;

    public FileOrcDataSource(File path, DataSize maxMergeDistance)
            throws IOException
    {
        this.path = checkNotNull(path, "path is null");
        this.size = path.length();
        this.input = new RandomAccessFile(path, "r");

        this.maxMergeDistance = checkNotNull(maxMergeDistance, "maxMergeDistance is null");
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
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

        input.seek(position);
        input.readFully(buffer, bufferOffset, bufferLength);

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

        // TODO: benchmark alternatively strategies:
        // 1) sort ranges and perform one read per range
        // 2) single read with transferTo() using custom WritableByteChannel

        Iterable<DiskRange> mergedRanges = mergeAdjacentDiskRanges(diskRanges.values(), maxMergeDistance, new DataSize(8, Unit.MEGABYTE));

        // read ranges
        Map<DiskRange, byte[]> buffers = new LinkedHashMap<>();
        for (DiskRange mergedRange : mergedRanges) {
            // read full range in one request
            byte[] buffer = new byte[mergedRange.getLength()];
            readFully(mergedRange.getOffset(), buffer);
            buffers.put(mergedRange, buffer);
        }

        ImmutableMap.Builder<K, Slice> slices = ImmutableMap.builder();
        for (Entry<K, DiskRange> entry : diskRanges.entrySet()) {
            slices.put(entry.getKey(), getDiskRangeSlice(entry.getValue(), buffers));
        }
        return slices.build();
    }

    @Override
    public String toString()
    {
        return path.getPath();
    }
}
