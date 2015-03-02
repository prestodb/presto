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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class CachingOrcDataSource
        implements OrcDataSource
{
    private final OrcDataSource dataSource;
    private final RegionFinder regionFinder;

    private long cachePosition;
    private int cacheLength;
    private byte[] cache;

    public CachingOrcDataSource(OrcDataSource dataSource, RegionFinder regionFinder)
    {
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.regionFinder = requireNonNull(regionFinder, "regionFinder is null");
        this.cache = new byte[0];
    }

    @Override
    public long getReadBytes()
    {
        return dataSource.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return dataSource.getReadTimeNanos();
    }

    @Override
    public long getSize()
    {
        return dataSource.getSize();
    }

    @VisibleForTesting
    void readCacheAt(long offset)
            throws IOException
    {
        DiskRange newCacheRange = regionFinder.getRangeFor(offset);
        cachePosition = newCacheRange.getOffset();
        cacheLength = newCacheRange.getLength();
        if (cache.length < cacheLength) {
            cache = new byte[cacheLength];
        }
        dataSource.readFully(newCacheRange.getOffset(), cache, 0, cacheLength);
    }

    @Override
    public void readFully(long position, byte[] buffer)
            throws IOException
    {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int length)
            throws IOException
    {
        if (position < cachePosition) {
            throw new IllegalArgumentException(String.format("read request (offset %d length %d) is before cache (offset %d length %d)", position, length, cachePosition, cacheLength));
        }
        if (position >= cachePosition + cacheLength) {
            readCacheAt(position);
        }
        if (position + length > cachePosition + cacheLength) {
            throw new IllegalArgumentException(String.format("read request (offset %d length %d) partially overlaps cache (offset %d length %d)", position, length, cachePosition, cacheLength));
        }
        System.arraycopy(cache, Ints.checkedCast(position - cachePosition), buffer, bufferOffset, length);
    }

    @Override
    public <K> Map<K, FixedLengthSliceInput> readFully(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        ImmutableMap.Builder<K, FixedLengthSliceInput> builder = ImmutableMap.builder();

        // Assumption here: all disk ranges are in the same region. Therefore, serving them in arbitrary order
        // will not result in eviction of cache that otherwise could have served any of the DiskRanges provided.
        for (Map.Entry<K, DiskRange> entry : diskRanges.entrySet()) {
            DiskRange diskRange = entry.getValue();
            byte[] buffer = new byte[diskRange.getLength()];
            readFully(diskRange.getOffset(), buffer);
            builder.put(entry.getKey(), Slices.wrappedBuffer(buffer).getInput());
        }
        return builder.build();
    }

    @Override
    public void close()
            throws IOException
    {
        dataSource.close();
    }

    public interface RegionFinder
    {
        DiskRange getRangeFor(long desiredOffset);
    }
}
