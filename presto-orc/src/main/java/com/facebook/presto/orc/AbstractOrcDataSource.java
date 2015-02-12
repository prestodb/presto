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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.orc.OrcDataSourceUtils.getDiskRangeSlice;
import static com.facebook.presto.orc.OrcDataSourceUtils.mergeAdjacentDiskRanges;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractOrcDataSource
        implements OrcDataSource
{
    private final String name;
    private final long size;
    private final DataSize maxMergeDistance;
    private final DataSize maxReadSize;
    private long readTimeNanos;

    public AbstractOrcDataSource(String name, long size, DataSize maxMergeDistance, DataSize maxReadSize)
    {
        this.name = checkNotNull(name, "name is null");

        this.size = size;
        checkArgument(size >= 0, "size is negative");

        this.maxMergeDistance = checkNotNull(maxMergeDistance, "maxMergeDistance is null");
        this.maxReadSize = checkNotNull(maxReadSize, "maxReadSize is null");
    }

    protected abstract void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException;

    @Override
    public final long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public final long getSize()
    {
        return size;
    }

    @Override
    public final void readFully(long position, byte[] buffer)
            throws IOException
    {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public final void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        long start = System.nanoTime();

        readInternal(position, buffer, bufferOffset, bufferLength);

        readTimeNanos += System.nanoTime() - start;
    }

    @Override
    public final <K> Map<K, Slice> readFully(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        checkNotNull(diskRanges, "diskRanges is null");

        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        Iterable<DiskRange> mergedRanges = mergeAdjacentDiskRanges(diskRanges.values(), maxMergeDistance, maxReadSize);

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
    public final String toString()
    {
        return name;
    }
}
