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

import com.facebook.presto.orc.stream.AbstractDiskOrcDataReader;
import com.facebook.presto.orc.stream.MemoryOrcDataReader;
import com.facebook.presto.orc.stream.OrcDataReader;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.orc.OrcDataSourceUtils.getDiskRangeSlice;
import static com.facebook.presto.orc.OrcDataSourceUtils.mergeAdjacentDiskRanges;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public abstract class AbstractOrcDataSource
        implements OrcDataSource
{
    private final OrcDataSourceId id;
    private final long size;
    private final DataSize maxMergeDistance;
    private final DataSize maxBufferSize;
    private final DataSize streamBufferSize;
    private final boolean lazyReadSmallRanges;
    private long readTimeNanos;
    private long readBytes;

    public AbstractOrcDataSource(OrcDataSourceId id, long size, DataSize maxMergeDistance, DataSize maxBufferSize, DataSize streamBufferSize, boolean lazyReadSmallRanges)
    {
        this.id = requireNonNull(id, "id is null");

        this.size = size;
        checkArgument(size > 0, "size must be at least 1");

        this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        this.maxBufferSize = requireNonNull(maxBufferSize, "maxBufferSize is null");
        this.streamBufferSize = requireNonNull(streamBufferSize, "streamBufferSize is null");
        this.lazyReadSmallRanges = lazyReadSmallRanges;
    }

    protected abstract void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException;

    @Override
    public OrcDataSourceId getId()
    {
        return id;
    }

    @Override
    public final long getReadBytes()
    {
        return readBytes;
    }

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
    public final Slice readFully(long position, int length)
            throws IOException
    {
        byte[] buffer = new byte[length];
        readFully(position, buffer, 0, length);
        return Slices.wrappedBuffer(buffer);
    }

    private void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        long start = System.nanoTime();

        readInternal(position, buffer, bufferOffset, bufferLength);

        readTimeNanos += System.nanoTime() - start;
        readBytes += bufferLength;
    }

    @Override
    public final <K> Map<K, OrcDataReader> readFully(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        requireNonNull(diskRanges, "diskRanges is null");

        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        //
        // Note: this code does not use the Java 8 stream APIs to avoid any extra object allocation
        //

        // split disk ranges into "big" and "small"
        long maxReadSizeBytes = maxBufferSize.toBytes();
        ImmutableMap.Builder<K, DiskRange> smallRangesBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<K, DiskRange> largeRangesBuilder = ImmutableMap.builder();
        for (Entry<K, DiskRange> entry : diskRanges.entrySet()) {
            if (entry.getValue().getLength() <= maxReadSizeBytes) {
                smallRangesBuilder.put(entry);
            }
            else {
                largeRangesBuilder.put(entry);
            }
        }
        Map<K, DiskRange> smallRanges = smallRangesBuilder.build();
        Map<K, DiskRange> largeRanges = largeRangesBuilder.build();

        // read ranges
        ImmutableMap.Builder<K, OrcDataReader> slices = ImmutableMap.builder();
        slices.putAll(readSmallDiskRanges(smallRanges));
        slices.putAll(readLargeDiskRanges(largeRanges));

        return slices.build();
    }

    private <K> Map<K, OrcDataReader> readSmallDiskRanges(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        Iterable<DiskRange> mergedRanges = mergeAdjacentDiskRanges(diskRanges.values(), maxMergeDistance, maxBufferSize);

        ImmutableMap.Builder<K, OrcDataReader> slices = ImmutableMap.builder();
        if (lazyReadSmallRanges) {
            for (DiskRange mergedRange : mergedRanges) {
                LazyBufferLoader mergedRangeLazyLoader = new LazyBufferLoader(mergedRange);
                for (Entry<K, DiskRange> diskRangeEntry : diskRanges.entrySet()) {
                    DiskRange diskRange = diskRangeEntry.getValue();
                    if (mergedRange.contains(diskRange)) {
                        slices.put(diskRangeEntry.getKey(), new MergedOrcDataReader(id, diskRange, mergedRangeLazyLoader));
                    }
                }
            }
        }
        else {
            Map<DiskRange, Slice> buffers = new LinkedHashMap<>();
            for (DiskRange mergedRange : mergedRanges) {
                // read full range in one request
                Slice buffer = readFully(mergedRange.getOffset(), mergedRange.getLength());
                buffers.put(mergedRange, buffer);
            }

            for (Entry<K, DiskRange> entry : diskRanges.entrySet()) {
                slices.put(entry.getKey(), new MemoryOrcDataReader(id, getDiskRangeSlice(entry.getValue(), buffers), entry.getValue().getLength()));
            }
        }

        Map<K, OrcDataReader> sliceStreams = slices.build();
        verify(sliceStreams.keySet().equals(diskRanges.keySet()));
        return sliceStreams;
    }

    private <K> Map<K, OrcDataReader> readLargeDiskRanges(Map<K, DiskRange> diskRanges)
    {
        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<K, OrcDataReader> slices = ImmutableMap.builder();
        for (Entry<K, DiskRange> entry : diskRanges.entrySet()) {
            DiskRange diskRange = entry.getValue();
            slices.put(entry.getKey(), new DiskOrcDataReader(diskRange));
        }
        return slices.build();
    }

    @Override
    public final String toString()
    {
        return id.toString();
    }

    private final class LazyBufferLoader
    {
        private final DiskRange diskRange;
        private Slice bufferSlice;

        public LazyBufferLoader(DiskRange diskRange)
        {
            this.diskRange = requireNonNull(diskRange, "diskRange is null");
        }

        public Slice loadNestedDiskRangeBuffer(DiskRange nestedDiskRange)
        {
            load();

            checkArgument(diskRange.contains(nestedDiskRange));
            int offset = toIntExact(nestedDiskRange.getOffset() - diskRange.getOffset());
            return bufferSlice.slice(offset, nestedDiskRange.getLength());
        }

        private void load()
        {
            if (bufferSlice != null) {
                return;
            }
            try {
                bufferSlice = readFully(diskRange.getOffset(), diskRange.getLength());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private final class MergedOrcDataReader
            implements OrcDataReader
    {
        private final OrcDataSourceId orcDataSourceId;
        private final DiskRange diskRange;
        private final LazyBufferLoader lazyBufferLoader;
        private Slice data;

        public MergedOrcDataReader(OrcDataSourceId orcDataSourceId, DiskRange diskRange, LazyBufferLoader lazyBufferLoader)
        {
            this.orcDataSourceId = requireNonNull(orcDataSourceId, "orcDataSourceId is null");
            this.diskRange = requireNonNull(diskRange, "diskRange is null");
            this.lazyBufferLoader = requireNonNull(lazyBufferLoader, "lazyBufferLoader is null");
        }

        @Override
        public OrcDataSourceId getOrcDataSourceId()
        {
            return orcDataSourceId;
        }

        @Override
        public long getRetainedSize()
        {
            return data == null ? 0 : diskRange.getLength();
        }

        @Override
        public int getSize()
        {
            return diskRange.getLength();
        }

        @Override
        public int getMaxBufferSize()
        {
            return diskRange.getLength();
        }

        @Override
        public Slice seekBuffer(int newPosition)
                throws IOException
        {
            if (data == null) {
                data = lazyBufferLoader.loadNestedDiskRangeBuffer(diskRange);
                if (data == null) {
                    throw new OrcCorruptionException(id, "Data loader returned null");
                }
                if (data.length() != diskRange.getLength()) {
                    throw new OrcCorruptionException(id, "Expected to load %s bytes, but %s bytes were loaded", diskRange.getLength(), data.length());
                }
            }
            return data.slice(newPosition, data.length() - newPosition);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("orcDataSourceId", orcDataSourceId)
                    .add("diskRange", diskRange)
                    .toString();
        }
    }

    private class DiskOrcDataReader
            extends AbstractDiskOrcDataReader
    {
        private final DiskRange diskRange;

        public DiskOrcDataReader(DiskRange diskRange)
        {
            super(id, requireNonNull(diskRange, "diskRange is null").getLength(), toIntExact(streamBufferSize.toBytes()));
            this.diskRange = diskRange;
        }

        @Override
        public void read(long position, byte[] buffer, int bufferOffset, int length)
                throws IOException
        {
            readFully(diskRange.getOffset() + position, buffer, bufferOffset, length);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("orcDataSourceId", getOrcDataSourceId())
                    .add("diskRange", diskRange)
                    .add("maxBufferSize", getMaxBufferSize())
                    .toString();
        }
    }
}
