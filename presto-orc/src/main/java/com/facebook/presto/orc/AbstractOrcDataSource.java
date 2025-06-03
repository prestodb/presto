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

import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.BasicSliceInput;
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;

import static com.facebook.presto.orc.OrcDataSourceUtils.getDiskRangeSlice;
import static com.facebook.presto.orc.OrcDataSourceUtils.mergeAdjacentDiskRanges;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public abstract class AbstractOrcDataSource
        implements OrcDataSource
{
    private static final Logger log = Logger.get(AbstractOrcDataSource.class);
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
    public final <K> Map<K, OrcDataSourceInput> readFully(Map<K, DiskRange> diskRanges)
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
        ImmutableMap.Builder<K, OrcDataSourceInput> slices = ImmutableMap.builder();
        slices.putAll(readSmallDiskRanges(smallRanges));
        slices.putAll(readLargeDiskRanges(largeRanges));

        long smallRangeSize = smallRanges.values().stream().mapToLong(DiskRange::getLength).sum();
        List<DiskRange> mergedRanges;
        if (!smallRanges.isEmpty()) {
            mergedRanges = mergeAdjacentDiskRanges(smallRanges.values(), maxMergeDistance, maxBufferSize);
        } else {
            mergedRanges = emptyList();
        }
        long mergedRangeSize = mergedRanges.stream().mapToLong(DiskRange::getLength).sum();
        long largeRangeSize = largeRanges.values().stream().mapToLong(DiskRange::getLength).sum();
        log.info("smallRanges %d %d",smallRanges.size(), smallRangeSize);
        log.info("mergedRanges %d %d",mergedRanges.size(), mergedRangeSize);
        log.info("largeRanges %d %d", largeRanges.size(), largeRangeSize);
        long readSize = mergedRangeSize + largeRangeSize;
        log.info("readSize %d", readSize);

        try{
            long prevMergedRangeSize = Long.parseLong(System.getProperty("READ_SMALL_RANGE_SIZE"));
            if (prevMergedRangeSize < mergedRangeSize) {
                System.setProperty("READ_SMALL_RANGE_SIZE", String.valueOf(mergedRangeSize));
            }
        }
        catch (Exception e){
            e.printStackTrace();
            System.setProperty("READ_SMALL_RANGE_SIZE", String.valueOf(mergedRangeSize));
        }

        try{
            long prevLargeRangeSize = Long.parseLong(System.getProperty("READ_LARGE_RANGE_SIZE"));
            if (prevLargeRangeSize < largeRangeSize) {
                System.setProperty("READ_LARGE_RANGE_SIZE", String.valueOf(largeRangeSize));
            }
        }
        catch (Exception e){
            e.printStackTrace();
            System.setProperty("READ_LARGE_RANGE_SIZE", String.valueOf(largeRangeSize));
        }

        try{
            long prevReadSize = Long.parseLong(System.getProperty("READ_RANGE_SIZE"));
            if (prevReadSize < readSize) {
                System.setProperty("READ_RANGE_SIZE", String.valueOf(readSize));
            }
        }
        catch (Exception e){
            e.printStackTrace();
            System.setProperty("READ_RANGE_SIZE", String.valueOf(readSize));
        }

        return slices.build();
    }

    private <K> Map<K, OrcDataSourceInput> readSmallDiskRanges(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        Iterable<DiskRange> mergedRanges = mergeAdjacentDiskRanges(diskRanges.values(), maxMergeDistance, maxBufferSize);

        ImmutableMap.Builder<K, OrcDataSourceInput> slices = ImmutableMap.builder();
        if (lazyReadSmallRanges) {
            for (DiskRange mergedRange : mergedRanges) {
                LazyBufferLoader mergedRangeLazyLoader = new LazyBufferLoader(mergedRange);
                for (Entry<K, DiskRange> diskRangeEntry : diskRanges.entrySet()) {
                    DiskRange diskRange = diskRangeEntry.getValue();
                    if (mergedRange.contains(diskRange)) {
                        FixedLengthSliceInput sliceInput = new LazySliceInput(diskRange.getLength(), new LazyMergedSliceLoader(diskRange, mergedRangeLazyLoader));
                        slices.put(diskRangeEntry.getKey(), new OrcDataSourceInput(sliceInput, diskRange.getLength()));
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
                slices.put(entry.getKey(), new OrcDataSourceInput(getDiskRangeSlice(entry.getValue(), buffers).getInput(), entry.getValue().getLength()));
            }
        }

        Map<K, OrcDataSourceInput> sliceStreams = slices.build();
        verify(sliceStreams.keySet().equals(diskRanges.keySet()));
        return sliceStreams;
    }

    private <K> Map<K, OrcDataSourceInput> readLargeDiskRanges(Map<K, DiskRange> diskRanges)
    {
        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<K, OrcDataSourceInput> slices = ImmutableMap.builder();
        for (Entry<K, DiskRange> entry : diskRanges.entrySet()) {
            DiskRange diskRange = entry.getValue();
            int bufferSize = toIntExact(streamBufferSize.toBytes());
            FixedLengthSliceInput sliceInput = new LazySliceInput(diskRange.getLength(), new LazyChunkedSliceLoader(diskRange, bufferSize));
            slices.put(entry.getKey(), new OrcDataSourceInput(sliceInput, bufferSize));
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
                byte[] buffer = new byte[diskRange.getLength()];
                readFully(diskRange.getOffset(), buffer);
                bufferSlice = Slices.wrappedBuffer(buffer);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private final class LazyMergedSliceLoader
            implements Supplier<FixedLengthSliceInput>
    {
        private final DiskRange diskRange;
        private final LazyBufferLoader lazyBufferLoader;

        public LazyMergedSliceLoader(DiskRange diskRange, LazyBufferLoader lazyBufferLoader)
        {
            this.diskRange = requireNonNull(diskRange, "diskRange is null");
            this.lazyBufferLoader = requireNonNull(lazyBufferLoader, "lazyBufferLoader is null");
        }

        @Override
        public FixedLengthSliceInput get()
        {
            Slice buffer = lazyBufferLoader.loadNestedDiskRangeBuffer(diskRange);
            return new BasicSliceInput(buffer);
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

    private final class LazyChunkedSliceLoader
            implements Supplier<FixedLengthSliceInput>
    {
        private final DiskRange diskRange;
        private final int bufferSize;

        public LazyChunkedSliceLoader(DiskRange diskRange, int bufferSize)
        {
            this.diskRange = requireNonNull(diskRange, "diskRange is null");
            checkArgument(bufferSize > 0, "bufferSize must be greater than 0");
            this.bufferSize = bufferSize;
        }

        @Override
        public FixedLengthSliceInput get()
        {
            return new ChunkedSliceInput(new ChunkedSliceLoader(diskRange), bufferSize);
        }
    }
}
