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

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static io.airlift.slice.UnsafeSlice.getLongUnchecked;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockDecoder;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.DictionaryId;
import com.facebook.presto.spi.block.ExprContext;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import sun.misc.Unsafe;

public class TestAriaHash {

    static Unsafe unsafe;

    static {
        try {

            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static boolean silent = false;
    static boolean useBloomFilter = false;

    static boolean recycleTable = false;

    static List<Slice> sliceReserve = new ArrayList();

    static void clearAllocCache() {
        sliceReserve.clear();
    }

    static Slice getSlice() {
        if (recycleTable) {
            synchronized (sliceReserve) {
                if (!sliceReserve.isEmpty()) {
                    return sliceReserve.remove(sliceReserve.size() - 1);
                }
            }
        }
        return Slices.allocate((128 * 1024));
    }

    static void releaseSlice(Slice slice) {
        if (recycleTable) {
            synchronized (sliceReserve) {
                sliceReserve.add(slice);
            }
        }
    }

    static boolean supportsLayout(
            List<Type> types, List<Integer> hashChannels, List<Integer> outputChannels) {
        return hashChannels.size() == 2
                && outputChannels.size() == 1
                && types.get(hashChannels.get(0).intValue()) == BIGINT
                && types.get(hashChannels.get(1).intValue()) == BIGINT
                && types.get(outputChannels.get(0).intValue()) == DOUBLE;
    }

    public static class AriaLookupSource implements LookupSource {
        int statusMask;
        long[] status;
        long[] table;
        Slice[] slices = new Slice[16];
        long[] slabs = new long[16];
        int[] fill = new int[16];
        int currentSlab = -1;
        long[] bloomFilter;
        int bloomFilterSize = 0;
        boolean closed = false;

        long nextResult(long entry, int offset) {
            Slice aslice;
            int aoffset;
            {
                aslice = slices[(int) ((entry) >> 17)];
                aoffset = (int) (entry) & 0x1ffff;
            }
            ;
            return getLongUnchecked(aslice, aoffset + offset);
        }

        public long allocBytes(int bytes) {
            if (currentSlab == -1 || fill[currentSlab] + bytes > (128 * 1024)) {
                long w = newSlab();
                fill[currentSlab] = bytes;
                return w;
            }
            int off = fill[currentSlab];
            fill[currentSlab] += bytes;
            return (((currentSlab) << 17) + (off));
        }

        long newSlab() {

            ++currentSlab;
            if (slices.length <= currentSlab) {
                int newSize = slices.length * 2;
                slices = Arrays.copyOf(slices, newSize);
                fill = Arrays.copyOf(fill, newSize);
            }
            Slice s = getSlice();
            slices[currentSlab] = s;
            return (((currentSlab) << 17) + (0));
        }

        void release() {

            for (Slice slice : slices) {
                if (slice != null) {
                    releaseSlice(slice);
                }
            }

            ;;
            if (bloomFilterSize > 0) {;
            }
        }

        void setSize(int count) {
            int size = 8;
            count *= 1.3;
            while (size < count) {
                size *= 2;
            }
            table = new long[size];
            status = new long[size / 8];
            statusMask = (size >> 3) - 1;
            for (int i = 0; i <= statusMask; ++i) {;
                status[i] = 0x8080808080808080L;
            }
            for (int i = 0; i < size; ++i) {;

                table[i] = -1;
            }
        }

        @Override
        public long getJoinPositionCount() {
            return statusMask + 1;
        }

        @Override
        public long getInMemorySizeInBytes() {
            return 8 * (statusMask + 1) + (128 * 1024) * (currentSlab + 1);
        }

        @Override
        public int getChannelCount() {
            return 1;
        }

        @Override
        public long joinPositionWithinPartition(long joinPosition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPosition(
                int position, Page hashChannelsPage, Page allChannelsPage, long rawHash) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getNextJoinPosition(
                long currentJoinPosition, int probePosition, Page allProbeChannelsPage) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isJoinPositionEligible(
                long currentJoinPosition, int probePosition, Page allProbeChannelsPage) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEmpty() {
            return statusMask == 0;
        }

        @Override
        public void close() {
            release();
            closed = true;
        }
    }

    public static class HashBuild extends ExprContext {
        AriaLookupSource table = new AriaLookupSource();
        BlockDecoder k1 = new BlockDecoder();
        BlockDecoder k2 = new BlockDecoder();
        BlockDecoder d1 = new BlockDecoder();
        int[] hashChannels;
        int[] outputChannels;
        int entryCount = 0;
        boolean makeBloomFilter = false;

        public HashBuild(List<Integer> hashChannels, List<Integer> outputChannels) {
            this.hashChannels = hashChannels.stream().mapToInt(Integer::intValue).toArray();
            this.outputChannels = outputChannels.stream().mapToInt(Integer::intValue).toArray();
        }

        long hashRow(long row) {
            AriaLookupSource table = this.table;
            int statusMask = table.statusMask;
            Slice[] slices = table.slices;
            ;
            Slice kslice;
            int koffset;
            {
                kslice = slices[(int) ((row) >> 17)];
                koffset = (int) (row) & 0x1ffff;
            }
            ;
            long h;
            {
                long __k = getLongUnchecked(kslice, koffset + 0);
                __k *= 0xc6a4a7935bd1e995L;
                __k ^= __k >> 47;
                h = __k * 0xc6a4a7935bd1e995L;
            }
            ;
            {
                long __k = getLongUnchecked(kslice, koffset + 8);
                __k *= 0xc6a4a7935bd1e995L;
                __k ^= __k >> 47;
                __k *= 0xc6a4a7935bd1e995L;
                h ^= __k;
                h *= 0xc6a4a7935bd1e995L;
            }
            ;
            return h;
        }

        public void addInput(Page page) {
            k1.decodeBlock(page.getBlock(hashChannels[0]), intArrayAllocator);
            k2.decodeBlock(page.getBlock(hashChannels[1]), intArrayAllocator);
            d1.decodeBlock(page.getBlock(outputChannels[0]), intArrayAllocator);
            int positionCount = page.getPositionCount();
            nullsInBatch = null;
            int[] k1Map = k1.rowNumberMap;
            int[] k2Map = k2.rowNumberMap;
            int[] d1Map = d1.rowNumberMap;
            addNullFlags(k1.valueIsNull, k1.isIdentityMap ? null : k1Map, positionCount);
            addNullFlags(k2.valueIsNull, k2.isIdentityMap ? null : k2Map, positionCount);

            AriaLookupSource table = this.table;
            int statusMask = table.statusMask;
            Slice[] slices = table.slices;
            ;

            for (int i = 0; i < positionCount; ++i) {
                if (nullsInBatch == null || !nullsInBatch[i]) {
                    ++entryCount;
                    long row = table.allocBytes(32);

                    slices = table.slices;

                    Slice kslice;
                    int koffset;
                    {
                        kslice = slices[(int) ((row) >> 17)];
                        koffset = (int) (row) & 0x1ffff;
                    }
                    ;
                    kslice.setLong(koffset + 0, k1.longs[k1Map[i]]);
                    kslice.setLong(koffset + 8, k2.longs[k1Map[i]]);
                    kslice.setLong(koffset + 16, d1.longs[d1Map[i]]);
                    kslice.setLong(koffset + 24, -1);
                }
            }
            k1.release(intArrayAllocator);
            k2.release(intArrayAllocator);
            d1.release(intArrayAllocator);
        }

        public static class AriaLookupSourceSupplier implements LookupSourceSupplier {
            AriaLookupSource lookupSource;

            public AriaLookupSourceSupplier(AriaLookupSource lookupSource) {
                this.lookupSource = lookupSource;
            }

            @Override
            public LookupSource get() {
                return lookupSource;
            }

            public long getHashCollisions() {
                return 0;
            }

            public double getExpectedHashCollisions() {
                return 0;
            }

            public long checksum() {
                return 1234;
            }
        }

        public LookupSourceSupplier createLookupSourceSupplier(
                Session session,
                List<Integer> joinChannels,
                OptionalInt hashChannel,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                Optional<Integer> sortChannel,
                List<JoinFilterFunctionFactory> searchFunctionFactories,
                Optional<List<Integer>> outputChannels) {
            build();
            boolean reuse = SystemSessionProperties.enableAriaReusePages(session);
            return new AriaLookupSourceSupplier(table);
        }

        public void build() {
            AriaLookupSource table = this.table;
            int statusMask = table.statusMask;
            Slice[] slices = table.slices;
            ;
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

        void insertHashes(long[] hashes, long[] entries, int fill) {
            AriaLookupSource table = this.table;
            int statusMask = table.statusMask;
            Slice[] slices = table.slices;
            ;;;
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
                    Slice aslice;
                    int aoffset;
                    Slice bslice;
                    int boffset;
                    while (hits != 0) {
                        {
                            bslice = slices[(int) ((entries[i]) >> 17)];
                            boffset = (int) (entries[i]) & 0x1ffff;
                        }
                        ;
                        int pos = Long.numberOfTrailingZeros(hits) >> 3;
                        ;
                        {
                            aslice = slices[(int) ((table.table[h * 8 + pos]) >> 17)];
                            aoffset = (int) (table.table[h * 8 + pos]) & 0x1ffff;
                        }
                        ;
                        if (getLongUnchecked(aslice, aoffset + 0) == getLongUnchecked(bslice, boffset + 0)
                                && getLongUnchecked(aslice, aoffset + 8) == getLongUnchecked(bslice, boffset + 8)) {
                            aslice.setLong(aoffset + 24, entries[i]);
                            break nextKey;
                        }
                        hits &= (hits - 1);
                    }

                    st &= 0x8080808080808080L;
                    if (st != 0) {
                        int pos = Long.numberOfTrailingZeros(st) >> 3;
                        table.status[h] = table.status[h] ^ (long) (statusByte | 0x80) << (pos * 8);
                        ;
                        table.table[h * 8 + pos] = entries[i];
                        break;
                    }
                    h = (h + 1) & statusMask;
                } while (true);
            }
            if (makeBloomFilter) {
                int size;
                long[] bloomArray;
                ;
                if (table.bloomFilterSize == 0) {
                    size = (entryCount / 8) + 1;
                    table.bloomFilter = new long[size];
                    bloomArray = table.bloomFilter;
                    table.bloomFilterSize = size;
                    for (int i = 0; i < size; ++i) {;
                        bloomArray[i] = 0;
                    }
                } else {
                    size = table.bloomFilterSize;
                    bloomArray = table.bloomFilter;
                }
                for (int i = 0; i < fill; i++) {
                    long h = hashes[i];
                    int w = (int) ((h & 0x7fffffff) % size);
                    long mask =
                            ((1L << (63 & ((h) >> 32)))
                                    | (1L << (63 & ((h) >> 38)))
                                    | (1L << (63 & ((h) >> 44)))
                                    | (1L << (63 & ((h) >> 50))));
                    ;
                    bloomArray[w] = bloomArray[w] | mask;
                }
            }
        }
    }

    public static class AriaProbe extends ExprContext {
        BlockDecoder k1 = new BlockDecoder();
        BlockDecoder k2 = new BlockDecoder();
        BlockDecoder hashDecoder;
        long hashes[];
        AriaLookupSource[] tables;
        int currentInput;
        long nextRow;
        long[] k1d;
        long[] k2d;
        int[] k1Map;
        int[] k2Map;
        int maxResults = 1024;
        int[] candidates;
        int[] partitions;
        int candidateFill;
        int positionCount;
        int[] resultMap;
        int resultFill;
        long[] result1;
        long currentResult;
        int currentProbe;
        JoinProbe probe;
        Page resultPage;
        Page returnPage;
        boolean reuseResult;
        boolean unroll = true;
        boolean finishing = false;
        DictionaryId dictionaryId;
        LocalPartitionGenerator partitionGenerator;

        AriaProbe(
                AriaLookupSource[] tables,
                LocalPartitionGenerator partitionGenerator,
                boolean reuseResult) {
            this.tables = tables;
            this.partitionGenerator = partitionGenerator;
            this.reuseResult = reuseResult;
            dictionaryId = DictionaryId.randomDictionaryId();
        }

        public void addInput(JoinProbe probe) {
            this.probe = probe;
            Block[] probes = probe.getProbeBlocks();
            k1.decodeBlock(probes[0], intArrayAllocator);
            k2.decodeBlock(probes[1], intArrayAllocator);
            positionCount = probe.getPage().getPositionCount();
            if (partitions == null || partitions.length < positionCount) {
                partitions = new int[(int) (positionCount * 1.2)];
            }

            Block hashBlock = probe.getProbeHashBlock();
            if (hashBlock != null && tables.length > 1) {
                if (hashDecoder == null) {
                    hashDecoder = new BlockDecoder(intArrayAllocator);
                }
                hashDecoder.decodeBlock(hashBlock);
                long[] longs = hashDecoder.longs;
                int[] map = hashDecoder.rowNumberMap;
                int numPartitions = tables.length;
                for (int i = 0; i < positionCount; i++) {
                    partitions[i] = partitionGenerator.getPartition(longs[map[i]]);
                }
                hashDecoder.release();
            } else {
                Arrays.fill(partitions, 0);
            }

            if (hashes == null || hashes.length < positionCount) {
                hashes = new long[positionCount + 10];
            }
            nullsInBatch = null;
            k1d = k1.longs;
            k2d = k2.longs;
            k1Map = k1.rowNumberMap;
            k2Map = k2.rowNumberMap;
            addNullFlags(k1.valueIsNull, k1.isIdentityMap ? null : k1Map, positionCount);
            addNullFlags(k2.valueIsNull, k2.isIdentityMap ? null : k2Map, positionCount);
            candidateFill = 0;
            if (candidates == null || candidates.length < positionCount) {
                candidates = intArrayAllocator.getIntArray(positionCount);
            }
            if (nullsInBatch != null) {
                for (int i = 0; i < positionCount; ++i) {
                    if (nullsInBatch[i]) {
                        candidates[candidateFill++] = i;
                    }
                }
            } else {
                for (int i = 0; i < positionCount; ++i) {
                    candidates[i] = i;
                }
                candidateFill = positionCount;
            }
            boolean hashPrecomputed = false;
            if (!hashPrecomputed) {
                for (int i = 0; i < candidateFill; ++i) {
                    int row = candidates[i];
                    long h;
                    {
                        long __k = k1d[k1Map[row]];
                        __k *= 0xc6a4a7935bd1e995L;
                        __k ^= __k >> 47;
                        h = __k * 0xc6a4a7935bd1e995L;
                    }
                    ;
                    {
                        long __k = k2d[k2Map[row]];
                        __k *= 0xc6a4a7935bd1e995L;
                        __k ^= __k >> 47;
                        __k *= 0xc6a4a7935bd1e995L;
                        h ^= __k;
                        h *= 0xc6a4a7935bd1e995L;
                    }
                    ;
                    hashes[row] = h;
                }
            }
            if (result1 == null) {
                result1 = new long[maxResults];
                resultMap = new int[maxResults];
            }
            boolean useBloomFilter = false;
            if (useBloomFilter) {
                int newFill = 0;
                for (int i = 0; i < candidateFill; ++i) {
                    int candidate = candidates[i];
                    AriaLookupSource table = tables[partitions[candidate]];
                    long[] bloomArray = table.bloomFilter;
                    int size = table.bloomFilterSize;
                    long h = hashes[candidate];
                    int w = (int) ((h & 0x7fffffff) % size);
                    long mask =
                            ((1L << (63 & ((h) >> 32)))
                                    | (1L << (63 & ((h) >> 38)))
                                    | (1L << (63 & ((h) >> 44)))
                                    | (1L << (63 & ((h) >> 50))));
                    ;
                    if (mask == (bloomArray[w] & mask)) {
                        candidates[newFill++] = candidate;
                    }
                }
                candidateFill = newFill;
            }
            currentProbe = 0;
            currentResult = -1;
        }

        public boolean addResult(long entry, int candidate) {
            int probeRow = candidates[candidate];
            AriaLookupSource table = tables[partitions[candidate]];
            int statusMask = table.statusMask;
            Slice[] slices = table.slices;
            ;
            do {
                resultMap[resultFill] = probeRow;
                Slice aslice;
                int aoffset;
                {
                    aslice = slices[(int) ((entry) >> 17)];
                    aoffset = (int) (entry) & 0x1ffff;
                }
                ;
                result1[resultFill] = getLongUnchecked(aslice, aoffset + 16);
                entry = getLongUnchecked(aslice, aoffset + 24);
                ++resultFill;
                if (resultFill >= maxResults) {
                    currentResult = entry;
                    currentProbe = candidate;
                    finishResult();
                    return true;
                }
            } while (entry != -1);
            currentResult = -1;
            return false;
        }

        void finishResult() {
            if (currentResult == -1 && currentProbe < candidateFill) {
                ++currentProbe;
            }
            if (currentProbe == candidateFill) {
                k1.release(intArrayAllocator);
                k2.release(intArrayAllocator);
            }
            if (resultFill == 0) {
                returnPage = null;
                return;
            }
            Page page = probe.getPage();
            int[] probeOutputChannels = probe.getOutputChannels();
            Block[] blocks = new Block[probeOutputChannels.length + 1];
            for (int i = 0; i < probeOutputChannels.length; i++) {
                blocks[i] =
                        new DictionaryBlock(
                                0,
                                resultFill,
                                page.getBlock(probeOutputChannels[i]),
                                resultMap,
                                false,
                                dictionaryId);
            }
            blocks[probeOutputChannels.length] =
                    new LongArrayBlock(resultFill, Optional.empty(), result1);
            resultPage = new Page(resultFill, blocks);
            if (!reuseResult) {
                result1 = new long[maxResults];
                resultMap = new int[maxResults];
            }
            resultFill = 0;
            returnPage = resultPage;
        }

        public boolean needsInput() {
            return currentResult == -1 && currentProbe == candidateFill;
        }

        public void finish() {
            finishing = true;
        }

        public boolean isFinished() {
            return finishing && currentResult == -1 && currentProbe == candidateFill;
        }

        public Page getOutput() {
            if (currentResult != -1) {
                if (addResult(currentResult, currentProbe)) {
                    return returnPage;
                }
            }
            long tempHash;
            int unrollFill = unroll ? candidateFill : 0;
            for (; currentProbe + 3 < unrollFill; currentProbe += 4) {
                long entry0 = -1;
                long field0;
                long empty0;
                long hits0;
                int hash0;
                int row0;
                boolean match0 = false;
                Slice g0slice;
                int g0offset;
                ;;;
                long entry1 = -1;
                long field1;
                long empty1;
                long hits1;
                int hash1;
                int row1;
                boolean match1 = false;
                Slice g1slice;
                int g1offset;
                ;;;
                long entry2 = -1;
                long field2;
                long empty2;
                long hits2;
                int hash2;
                int row2;
                boolean match2 = false;
                Slice g2slice;
                int g2offset;
                ;;;
                long entry3 = -1;
                long field3;
                long empty3;
                long hits3;
                int hash3;
                int row3;
                boolean match3 = false;
                Slice g3slice;
                int g3offset;
                ;;;
                AriaLookupSource table0 = tables[partitions[candidates[currentProbe + 0]]];
                int statusMask0 = table0.statusMask;
                Slice[] slices0 = table0.slices;
                ;
                row0 = candidates[currentProbe + 0];
                tempHash = hashes[row0];
                hash0 = (int) tempHash & statusMask0;
                field0 = (tempHash >> 56) & 0x7f;
                ;
                hits0 = table0.status[hash0];
                field0 |= field0 << 8;
                field0 |= field0 << 16;
                field0 |= field0 << 32;
                ;
                AriaLookupSource table1 = tables[partitions[candidates[currentProbe + 1]]];
                int statusMask1 = table1.statusMask;
                Slice[] slices1 = table1.slices;
                ;
                row1 = candidates[currentProbe + 1];
                tempHash = hashes[row1];
                hash1 = (int) tempHash & statusMask1;
                field1 = (tempHash >> 56) & 0x7f;
                ;
                hits1 = table1.status[hash1];
                field1 |= field1 << 8;
                field1 |= field1 << 16;
                field1 |= field1 << 32;
                ;
                AriaLookupSource table2 = tables[partitions[candidates[currentProbe + 2]]];
                int statusMask2 = table2.statusMask;
                Slice[] slices2 = table2.slices;
                ;
                row2 = candidates[currentProbe + 2];
                tempHash = hashes[row2];
                hash2 = (int) tempHash & statusMask2;
                field2 = (tempHash >> 56) & 0x7f;
                ;
                hits2 = table2.status[hash2];
                field2 |= field2 << 8;
                field2 |= field2 << 16;
                field2 |= field2 << 32;
                ;
                AriaLookupSource table3 = tables[partitions[candidates[currentProbe + 3]]];
                int statusMask3 = table3.statusMask;
                Slice[] slices3 = table3.slices;
                ;
                row3 = candidates[currentProbe + 3];
                tempHash = hashes[row3];
                hash3 = (int) tempHash & statusMask3;
                field3 = (tempHash >> 56) & 0x7f;
                ;
                hits3 = table3.status[hash3];
                field3 |= field3 << 8;
                field3 |= field3 << 16;
                field3 |= field3 << 32;
                ;
                empty0 = hits0 & 0x8080808080808080L;
                hits0 ^= field0;
                hits0 -= 0x0101010101010101L;
                hits0 &= 0x8080808080808080L ^ empty0;
                if (hits0 != 0) {
                    int pos = Long.numberOfTrailingZeros(hits0) >> 3;
                    hits0 &= hits0 - 1;
                    ;
                    entry0 = table0.table[hash0 * 8 + pos];
                    {
                        g0slice = slices0[(int) ((entry0) >> 17)];
                        g0offset = (int) (entry0) & 0x1ffff;
                    }
                    ;
                    match0 =
                            getLongUnchecked(g0slice, g0offset + 0) == k1d[k1Map[row0]]
                                    & getLongUnchecked(g0slice, g0offset + 8) == k2d[k2Map[row0]];
                }
                ;
                empty1 = hits1 & 0x8080808080808080L;
                hits1 ^= field1;
                hits1 -= 0x0101010101010101L;
                hits1 &= 0x8080808080808080L ^ empty1;
                if (hits1 != 0) {
                    int pos = Long.numberOfTrailingZeros(hits1) >> 3;
                    hits1 &= hits1 - 1;
                    ;
                    entry1 = table1.table[hash1 * 8 + pos];
                    {
                        g1slice = slices1[(int) ((entry1) >> 17)];
                        g1offset = (int) (entry1) & 0x1ffff;
                    }
                    ;
                    match1 =
                            getLongUnchecked(g1slice, g1offset + 0) == k1d[k1Map[row1]]
                                    & getLongUnchecked(g1slice, g1offset + 8) == k2d[k2Map[row1]];
                }
                ;
                empty2 = hits2 & 0x8080808080808080L;
                hits2 ^= field2;
                hits2 -= 0x0101010101010101L;
                hits2 &= 0x8080808080808080L ^ empty2;
                if (hits2 != 0) {
                    int pos = Long.numberOfTrailingZeros(hits2) >> 3;
                    hits2 &= hits2 - 1;
                    ;
                    entry2 = table2.table[hash2 * 8 + pos];
                    {
                        g2slice = slices2[(int) ((entry2) >> 17)];
                        g2offset = (int) (entry2) & 0x1ffff;
                    }
                    ;
                    match2 =
                            getLongUnchecked(g2slice, g2offset + 0) == k1d[k1Map[row2]]
                                    & getLongUnchecked(g2slice, g2offset + 8) == k2d[k2Map[row2]];
                }
                ;
                empty3 = hits3 & 0x8080808080808080L;
                hits3 ^= field3;
                hits3 -= 0x0101010101010101L;
                hits3 &= 0x8080808080808080L ^ empty3;
                if (hits3 != 0) {
                    int pos = Long.numberOfTrailingZeros(hits3) >> 3;
                    hits3 &= hits3 - 1;
                    ;
                    entry3 = table3.table[hash3 * 8 + pos];
                    {
                        g3slice = slices3[(int) ((entry3) >> 17)];
                        g3offset = (int) (entry3) & 0x1ffff;
                    }
                    ;
                    match3 =
                            getLongUnchecked(g3slice, g3offset + 0) == k1d[k1Map[row3]]
                                    & getLongUnchecked(g3slice, g3offset + 8) == k2d[k2Map[row3]];
                }
                ;
                if (match0) {
                    if (addResult(entry0, currentProbe + 0)) return returnPage;
                } else {
                    bucketLoop0:
                    for (; ; ) {
                        while (hits0 != 0) {
                            int pos = Long.numberOfTrailingZeros(hits0) >> 3;
                            ;
                            entry0 = table0.table[hash0 * 8 + pos];
                            {
                                g0slice = slices0[(int) ((entry0) >> 17)];
                                g0offset = (int) (entry0) & 0x1ffff;
                            }
                            ;
                            if (getLongUnchecked(g0slice, g0offset + 0) == k1d[k1Map[row0]]
                                    && getLongUnchecked(g0slice, g0offset + 8) == k2d[k2Map[row0]]) {
                                if (addResult(entry0, currentProbe + 0)) {
                                    return returnPage;
                                }
                                break bucketLoop0;
                            }
                            hits0 &= hits0 - 1;
                        }
                        if (empty0 != 0) break;
                        hash0 = (hash0 + 1) & statusMask0;
                        ;
                        hits0 = table0.status[hash0];
                        empty0 = hits0 & 0x8080808080808080L;
                        hits0 ^= field0;
                        hits0 -= 0x0101010101010101L;
                        hits0 &= 0x8080808080808080L ^ empty0;
                    }
                }
                ;
                if (match1) {
                    if (addResult(entry1, currentProbe + 1)) return returnPage;
                } else {
                    bucketLoop1:
                    for (; ; ) {
                        while (hits1 != 0) {
                            int pos = Long.numberOfTrailingZeros(hits1) >> 3;
                            ;
                            entry1 = table1.table[hash1 * 8 + pos];
                            {
                                g1slice = slices1[(int) ((entry1) >> 17)];
                                g1offset = (int) (entry1) & 0x1ffff;
                            }
                            ;
                            if (getLongUnchecked(g1slice, g1offset + 0) == k1d[k1Map[row1]]
                                    && getLongUnchecked(g1slice, g1offset + 8) == k2d[k2Map[row1]]) {
                                if (addResult(entry1, currentProbe + 1)) {
                                    return returnPage;
                                }
                                break bucketLoop1;
                            }
                            hits1 &= hits1 - 1;
                        }
                        if (empty1 != 0) break;
                        hash1 = (hash1 + 1) & statusMask1;
                        ;
                        hits1 = table1.status[hash1];
                        empty1 = hits1 & 0x8080808080808080L;
                        hits1 ^= field1;
                        hits1 -= 0x0101010101010101L;
                        hits1 &= 0x8080808080808080L ^ empty1;
                    }
                }
                ;
                if (match2) {
                    if (addResult(entry2, currentProbe + 2)) return returnPage;
                } else {
                    bucketLoop2:
                    for (; ; ) {
                        while (hits2 != 0) {
                            int pos = Long.numberOfTrailingZeros(hits2) >> 3;
                            ;
                            entry2 = table2.table[hash2 * 8 + pos];
                            {
                                g2slice = slices2[(int) ((entry2) >> 17)];
                                g2offset = (int) (entry2) & 0x1ffff;
                            }
                            ;
                            if (getLongUnchecked(g2slice, g2offset + 0) == k1d[k1Map[row2]]
                                    && getLongUnchecked(g2slice, g2offset + 8) == k2d[k2Map[row2]]) {
                                if (addResult(entry2, currentProbe + 2)) {
                                    return returnPage;
                                }
                                break bucketLoop2;
                            }
                            hits2 &= hits2 - 1;
                        }
                        if (empty2 != 0) break;
                        hash2 = (hash2 + 1) & statusMask2;
                        ;
                        hits2 = table2.status[hash2];
                        empty2 = hits2 & 0x8080808080808080L;
                        hits2 ^= field2;
                        hits2 -= 0x0101010101010101L;
                        hits2 &= 0x8080808080808080L ^ empty2;
                    }
                }
                ;
                if (match3) {
                    if (addResult(entry3, currentProbe + 3)) return returnPage;
                } else {
                    bucketLoop3:
                    for (; ; ) {
                        while (hits3 != 0) {
                            int pos = Long.numberOfTrailingZeros(hits3) >> 3;
                            ;
                            entry3 = table3.table[hash3 * 8 + pos];
                            {
                                g3slice = slices3[(int) ((entry3) >> 17)];
                                g3offset = (int) (entry3) & 0x1ffff;
                            }
                            ;
                            if (getLongUnchecked(g3slice, g3offset + 0) == k1d[k1Map[row3]]
                                    && getLongUnchecked(g3slice, g3offset + 8) == k2d[k2Map[row3]]) {
                                if (addResult(entry3, currentProbe + 3)) {
                                    return returnPage;
                                }
                                break bucketLoop3;
                            }
                            hits3 &= hits3 - 1;
                        }
                        if (empty3 != 0) break;
                        hash3 = (hash3 + 1) & statusMask3;
                        ;
                        hits3 = table3.status[hash3];
                        empty3 = hits3 & 0x8080808080808080L;
                        hits3 ^= field3;
                        hits3 -= 0x0101010101010101L;
                        hits3 &= 0x8080808080808080L ^ empty3;
                    }
                }
                ;
            }
            for (; currentProbe < candidateFill; ++currentProbe) {
                long entry0 = -1;
                long field0;
                long empty0;
                long hits0;
                int hash0;
                int row0;
                boolean match0 = false;
                Slice g0slice;
                int g0offset;
                ;;;
                AriaLookupSource table0 = tables[partitions[candidates[currentProbe + 0]]];
                int statusMask0 = table0.statusMask;
                Slice[] slices0 = table0.slices;
                ;
                row0 = candidates[currentProbe + 0];
                tempHash = hashes[row0];
                hash0 = (int) tempHash & statusMask0;
                field0 = (tempHash >> 56) & 0x7f;
                ;
                hits0 = table0.status[hash0];
                field0 |= field0 << 8;
                field0 |= field0 << 16;
                field0 |= field0 << 32;
                ;
                empty0 = hits0 & 0x8080808080808080L;
                hits0 ^= field0;
                hits0 -= 0x0101010101010101L;
                hits0 &= 0x8080808080808080L ^ empty0;
                if (hits0 != 0) {
                    int pos = Long.numberOfTrailingZeros(hits0) >> 3;
                    hits0 &= hits0 - 1;
                    ;
                    entry0 = table0.table[hash0 * 8 + pos];
                    {
                        g0slice = slices0[(int) ((entry0) >> 17)];
                        g0offset = (int) (entry0) & 0x1ffff;
                    }
                    ;
                    match0 =
                            getLongUnchecked(g0slice, g0offset + 0) == k1d[k1Map[row0]]
                                    & getLongUnchecked(g0slice, g0offset + 8) == k2d[k2Map[row0]];
                }
                ;
                if (match0) {
                    if (addResult(entry0, currentProbe + 0)) return returnPage;
                } else {
                    bucketLoop0:
                    for (; ; ) {
                        while (hits0 != 0) {
                            int pos = Long.numberOfTrailingZeros(hits0) >> 3;
                            ;
                            entry0 = table0.table[hash0 * 8 + pos];
                            {
                                g0slice = slices0[(int) ((entry0) >> 17)];
                                g0offset = (int) (entry0) & 0x1ffff;
                            }
                            ;
                            if (getLongUnchecked(g0slice, g0offset + 0) == k1d[k1Map[row0]]
                                    && getLongUnchecked(g0slice, g0offset + 8) == k2d[k2Map[row0]]) {
                                if (addResult(entry0, currentProbe + 0)) {
                                    return returnPage;
                                }
                                break bucketLoop0;
                            }
                            hits0 &= hits0 - 1;
                        }
                        if (empty0 != 0) break;
                        hash0 = (hash0 + 1) & statusMask0;
                        ;
                        hits0 = table0.status[hash0];
                        empty0 = hits0 & 0x8080808080808080L;
                        hits0 ^= field0;
                        hits0 -= 0x0101010101010101L;
                        hits0 &= 0x8080808080808080L ^ empty0;
                    }
                }
                ;
            }
            finishResult();
            return returnPage;
        }
    }
}
