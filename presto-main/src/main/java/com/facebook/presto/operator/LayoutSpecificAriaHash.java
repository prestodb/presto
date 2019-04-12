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
package com.facebook.presto.operator;

import com.facebook.presto.Session;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockDecoder;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler;
import io.airlift.slice.Slice;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.slice.UnsafeSlice.getLongUnchecked;

// TODO: this needs to work for arbitrary input types
public class LayoutSpecificAriaHash
{
    private static final long M = 0xc6a4a7935bd1e995L;

    private final AriaLookupSource table = new AriaLookupSource();
    private final BlockDecoder k1 = new BlockDecoder();
    private final BlockDecoder k2 = new BlockDecoder();
    private final BlockDecoder d1 = new BlockDecoder();
    private final int[] hashChannels;
    private final int[] outputChannels;

    private int entryCount;

    private boolean[] nullsInReserve;
    private boolean[] nullsInBatch;

    public LayoutSpecificAriaHash(List<Integer> hashChannels, List<Integer> outputChannels)
    {
        this.hashChannels = hashChannels.stream().mapToInt(Integer::intValue).toArray();
        this.outputChannels = outputChannels.stream().mapToInt(Integer::intValue).toArray();
    }

    private long hashRow(long row)
    {
        AriaLookupSource table = this.table;
        Slice[] slices = table.slices;
        Slice slice = slices[(int) ((row) >> 17)];
        int offset = (int) (row) & 0x1ffff;
        long h;
        long k1 = getLongUnchecked(slice, offset);
        k1 *= M;
        k1 ^= k1 >> 47;
        h = k1 * M;

        long k0 = getLongUnchecked(slice, offset + 8);
        k0 *= M;
        k0 ^= k0 >> 47;
        k0 *= M;
        h ^= k0;
        h *= M;
        return h;
    }

    public void addInput(Page page)
    {
        k1.decodeBlock(page.getBlock(hashChannels[0]));
        k2.decodeBlock(page.getBlock(hashChannels[1]));
        d1.decodeBlock(page.getBlock(outputChannels[0]));
        int positionCount = page.getPositionCount();
        nullsInBatch = null;
        int[] k1Map = k1.getRowNumberMap();
        int[] k2Map = k2.getRowNumberMap();
        int[] d1Map = d1.getRowNumberMap();
        addNullFlags(k1.getValueIsNull(), k1.isIdentityMap() ? null : k1Map, positionCount);
        addNullFlags(k2.getValueIsNull(), k2.isIdentityMap() ? null : k2Map, positionCount);

        AriaLookupSource table = this.table;
        Slice[] slices;

        for (int i = 0; i < positionCount; ++i) {
            if (nullsInBatch == null || !nullsInBatch[i]) {
                ++entryCount;
                long row = table.allocBytes(32);

                slices = table.slices;

                Slice slice;
                int koffset;
                {
                    slice = slices[(int) ((row) >> 17)];
                    koffset = (int) (row) & 0x1ffff;
                }
                slice.setLong(koffset, k1.getValues(long[].class)[k1Map[i]]);
                slice.setLong(koffset + 8, k2.getValues(long[].class)[k1Map[i]]);
                slice.setLong(koffset + 16, d1.getValues(long[].class)[d1Map[i]]);
                slice.setLong(koffset + 24, -1);
            }
        }

        k1.release();
        k2.release();
        d1.release();
    }

    private static void boolArrayOr(boolean[] target, boolean[] source, int[] map, int positionCount)
    {
        if (map == null) {
            for (int i = 0; i < positionCount; ++i) {
                target[i] |= source[i];
            }
        }
        else {
            for (int i = 0; i < positionCount; ++i) {
                target[i] |= source[map[i]];
            }
        }
    }

    private void addNullFlags(boolean[] nullFlags, int[] map, int positionCount)
    {
        if (nullFlags != null) {
            if (nullsInBatch == null && map == null) {
                nullsInBatch = nullFlags;
            }
            else {
                boolean[] newNulls;
                if (nullsInReserve != null && nullsInReserve.length >= positionCount) {
                    newNulls = nullsInReserve;
                }
                else {
                    newNulls = new boolean[positionCount];
                    nullsInReserve = newNulls;
                }
                if (nullsInBatch != null) {
                    System.arraycopy(nullsInBatch, 0, newNulls, 0, positionCount);
                }
                nullsInBatch = newNulls;
                boolArrayOr(nullsInBatch, nullFlags, map, positionCount);
            }
        }
    }

    public static class AriaLookupSourceSupplier
            implements LookupSourceSupplier
    {
        final AriaLookupSource lookupSource;

        public AriaLookupSourceSupplier(AriaLookupSource lookupSource)
        {
            this.lookupSource = lookupSource;
        }

        @Override
        public LookupSource get()
        {
            return lookupSource;
        }

        public long getHashCollisions()
        {
            return 0;
        }

        public double getExpectedHashCollisions()
        {
            return 0;
        }

        public long checksum()
        {
            return 1234;
        }
    }

    public LookupSourceSupplier createLookupSourceSupplier(
            Session session,
            List<Integer> joinChannels,
            OptionalInt hashChannel,
            Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> searchFunctionFactories,
            Optional<List<Integer>> outputChannels)
    {
        build();
        return new AriaLookupSourceSupplier(table);
    }

    private void build()
    {
        AriaLookupSource table = this.table;
        table.setSize(entryCount);
        int batch = 1024;
        long[] hashes = new long[batch];
        long[] entries = new long[batch];
        int fill = 0;
        for (int slab = 0; slab <= table.currentSlab; ++slab) {
            int slabFill = table.fill[slab];
            for (int offset = 0; offset < slabFill; offset += 32) {
                long entry = (((slab) << 17) + (offset));
                entries[fill] = entry;
                hashes[fill++] = hashRow(entry);
                if (fill == batch) {
                    insertHashes(hashes, entries, fill);
                    fill = 0;
                }
            }
        }
        insertHashes(hashes, entries, fill);
    }

    private void insertHashes(long[] hashes, long[] entries, int fill)
    {
        AriaLookupSource table = this.table;
        int statusMask = table.statusMask;
        Slice[] slices = table.slices;
        for (int i = 0; i < fill; ++i) {
            int h = (int) hashes[i] & statusMask;
            long field = (hashes[i] >> 56) & 0x7f;
            byte statusByte = (byte) field;
            field |= field << 8;
            field |= field << 16;
            field |= field << 32;
            nextKey:
            do {
                long st = table.status[h];
                long hits = st ^ field;
                hits = st - 0x0101010101010101L;
                hits &= 0x8080808080808080L;
                Slice slice;
                int aoffset;
                Slice otherSlice;
                int boffset;
                while (hits != 0) {
                    {
                        otherSlice = slices[(int) ((entries[i]) >> 17)];
                        boffset = (int) (entries[i]) & 0x1ffff;
                    }
                    int pos = Long.numberOfTrailingZeros(hits) >> 3;
                    {
                        slice = slices[(int) ((table.table[h * 8 + pos]) >> 17)];
                        aoffset = (int) (table.table[h * 8 + pos]) & 0x1ffff;
                    }
                    if (getLongUnchecked(slice, aoffset) == getLongUnchecked(otherSlice, boffset)
                            && getLongUnchecked(slice, aoffset + 8) == getLongUnchecked(otherSlice, boffset + 8)) {
                        slice.setLong(aoffset + 24, entries[i]);
                        break nextKey;
                    }
                    hits &= (hits - 1);
                }

                st &= 0x8080808080808080L;
                if (st != 0) {
                    int pos = Long.numberOfTrailingZeros(st) >> 3;
                    table.status[h] = table.status[h] ^ (long) (statusByte | 0x80) << (pos * 8);
                    table.table[h * 8 + pos] = entries[i];
                    break;
                }
                h = (h + 1) & statusMask;
            }
            while (true);
        }
    }

    public static class AriaLookupSource
            implements LookupSource
    {
        int statusMask;
        long[] status;
        long[] table;
        Slice[] slices = new Slice[16];

        private int[] fill = new int[16];
        private int currentSlab = -1;

        private long allocBytes(int bytes)
        {
            if (currentSlab == -1 || fill[currentSlab] + bytes > (128 * 1024)) {
                long w = newSlab();
                fill[currentSlab] = bytes;
                return w;
            }
            int off = fill[currentSlab];
            fill[currentSlab] += bytes;
            return (((currentSlab) << 17) + (off));
        }

        private long newSlab()
        {
            ++currentSlab;
            if (slices.length <= currentSlab) {
                int newSize = slices.length * 2;
                slices = Arrays.copyOf(slices, newSize);
                fill = Arrays.copyOf(fill, newSize);
            }
            Slice s = AriaHash.getSlice();
            slices[currentSlab] = s;
            return (currentSlab) << 17;
        }

        private void release()
        {
            for (Slice slice : slices) {
                if (slice != null) {
                    AriaHash.releaseSlice(slice);
                }
            }
        }

        private void setSize(int count)
        {
            int size = 8;
            count *= 1.3;
            while (size < count) {
                size *= 2;
            }
            table = new long[size];
            Arrays.fill(table, -1);
            status = new long[size >> 3];
            Arrays.fill(status, 0x8080808080808080L);
            statusMask = (size >> 3) - 1;
        }

        @Override
        public long getJoinPositionCount()
        {
            return statusMask + 1;
        }

        @Override
        public long getInMemorySizeInBytes()
        {
            return 8 * (statusMask + 1) + (128 * 1024) * (currentSlab + 1);
        }

        @Override
        public int getChannelCount()
        {
            return 1;
        }

        @Override
        public long joinPositionWithinPartition(long joinPosition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEmpty()
        {
            return statusMask == 0;
        }

        @Override
        public void close()
        {
            release();
            boolean closed = true;
        }
    }
}
