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
import io.airlift.slice.ChunkedSliceInput;
import io.airlift.slice.ChunkedSliceInput.BufferReference;
import io.airlift.slice.ChunkedSliceInput.SliceLoader;
import io.airlift.slice.FixedLengthSliceInput;
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
        readBytes += bufferLength;
    }

    @Override
    public final <K> Map<K, FixedLengthSliceInput> readFully(Map<K, DiskRange> diskRanges)
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
        ImmutableMap.Builder<K, FixedLengthSliceInput> slices = ImmutableMap.builder();
        slices.putAll(readSmallDiskRanges(smallRanges));
        slices.putAll(readLargeDiskRanges(largeRanges));

        return slices.build();
    }

    private <K> Map<K, FixedLengthSliceInput> readSmallDiskRanges(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        Iterable<DiskRange> mergedRanges = mergeAdjacentDiskRanges(diskRanges.values(), maxMergeDistance, maxBufferSize);

        ImmutableMap.Builder<K, FixedLengthSliceInput> slices = ImmutableMap.builder();
        if (lazyReadSmallRanges) {
            for (DiskRange mergedRange : mergedRanges) {
                LazyBufferLoader mergedRangeLazyLoader = new LazyBufferLoader(mergedRange);
                for (Entry<K, DiskRange> diskRangeEntry : diskRanges.entrySet()) {
                    DiskRange diskRange = diskRangeEntry.getValue();
                    if (mergedRange.contains(diskRange)) {
                        slices.put(diskRangeEntry.getKey(), new ChunkedSliceInput(new LazySliceLoader(diskRange, mergedRangeLazyLoader), diskRange.getLength()));
                    }
                }
            }
        }
        else {
            Map<DiskRange, byte[]> buffers = new LinkedHashMap<>();
            for (DiskRange mergedRange : mergedRanges) {
                // read full range in one request
                byte[] buffer = new byte[mergedRange.getLength()];
                readFully(mergedRange.getOffset(), buffer);
                buffers.put(mergedRange, buffer);
            }

            for (Entry<K, DiskRange> entry : diskRanges.entrySet()) {
                slices.put(entry.getKey(), getDiskRangeSlice(entry.getValue(), buffers).getInput());
            }
        }

        Map<K, FixedLengthSliceInput> sliceStreams = slices.build();
        verify(sliceStreams.keySet().equals(diskRanges.keySet()));
        return sliceStreams;
    }

    private <K> Map<K, FixedLengthSliceInput> readLargeDiskRanges(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<K, FixedLengthSliceInput> slices = ImmutableMap.builder();
        for (Entry<K, DiskRange> entry : diskRanges.entrySet()) {
            ChunkedSliceInput sliceInput = new ChunkedSliceInput(new ChunkedSliceLoader(entry.getValue()), toIntExact(streamBufferSize.toBytes()));
            slices.put(entry.getKey(), sliceInput);
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
        private final byte[] buffer;
        private final Slice bufferSlice;
        private boolean loaded;

        public LazyBufferLoader(DiskRange diskRange)
        {
            this.diskRange = requireNonNull(diskRange, "diskRange is null");
            this.buffer = new byte[diskRange.getLength()];
            this.bufferSlice = Slices.wrappedBuffer(buffer);
        }

        public Slice getNestedDiskRangeBuffer(DiskRange nestedDiskRange)
        {
            checkArgument(diskRange.contains(nestedDiskRange));
            int offset = toIntExact(nestedDiskRange.getOffset() - diskRange.getOffset());
            return bufferSlice.slice(offset, nestedDiskRange.getLength());
        }

        public void load()
        {
            if (loaded) {
                return;
            }
            try {
                readFully(diskRange.getOffset(), buffer);
                loaded = true;
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private final class LazySliceLoader
            implements SliceLoader<BufferReference>
    {
        private final DiskRange diskRange;
        private final LazyBufferLoader lazyBufferLoader;
        private final Slice buffer;

        public LazySliceLoader(DiskRange diskRange, LazyBufferLoader lazyBufferLoader)
        {
            this.diskRange = requireNonNull(diskRange, "diskRange is null");
            this.lazyBufferLoader = requireNonNull(lazyBufferLoader, "lazyBufferLoader is null");
            this.buffer = lazyBufferLoader.getNestedDiskRangeBuffer(diskRange);
        }

        @Override
        public BufferReference createBuffer(int bufferSize)
        {
            checkArgument(bufferSize == buffer.length(), "Buffer size must be same size as the existing buffer");
            return () -> buffer;
        }

        @Override
        public long getSize()
        {
            return diskRange.getLength();
        }

        @Override
        public void load(long position, BufferReference bufferReference, int length)
        {
            checkArgument(position == 0, "Invalid read request");
            checkArgument(length == getSize(), "Invalid read request");
            lazyBufferLoader.load();
        }

        @Override
        public void close()
        {
        }
    }

    private class ChunkedSliceLoader
            implements SliceLoader<SliceBufferReference>
    {
        private final DiskRange diskRange;

        public ChunkedSliceLoader(DiskRange diskRange)
        {
            this.diskRange = diskRange;
        }

        @Override
        public SliceBufferReference createBuffer(int bufferSize)
        {
            return new SliceBufferReference(bufferSize);
        }

        @Override
        public long getSize()
        {
            return diskRange.getLength();
        }

        @Override
        public void load(long position, SliceBufferReference bufferReference, int length)
        {
            try {
                readFully(diskRange.getOffset() + position, bufferReference.getBuffer(), 0, length);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close()
        {
        }
    }

    private static class SliceBufferReference
            implements BufferReference
    {
        private final byte[] buffer;
        private final Slice slice;

        public SliceBufferReference(int bufferSize)
        {
            this.buffer = new byte[bufferSize];
            this.slice = Slices.wrappedBuffer(buffer);
        }

        public byte[] getBuffer()
        {
            return buffer;
        }

        @Override
        public Slice getSlice()
        {
            return slice;
        }
    }
}
