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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockDecoder;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.DictionaryId;
import com.facebook.presto.spi.block.LongArrayBlock;
import io.airlift.slice.Slice;

import java.util.Arrays;
import java.util.Optional;

import static io.airlift.slice.UnsafeSlice.getLongUnchecked;
import static it.unimi.dsi.fastutil.ints.IntArrays.ensureCapacity;

public class AriaProbe
{
    private static final long M = 0xc6a4a7935bd1e995L;

    private final BlockDecoder k1 = new BlockDecoder();
    private final BlockDecoder k2 = new BlockDecoder();
    private final LookupSource.PartitionToLookupSourceSupplier partitionToLookupSourceSupplier;

    private BlockDecoder hashDecoder;
    private long[] hashes;

    private long[] k1d;
    private long[] k2d;
    private int[] k1Map;
    private int[] k2Map;
    private final int maxResults = 1024;
    private int[] candidates = new int[] {};
    private int[] partitions;
    private int candidateFill;
    private int[] resultMap;
    private int resultFill;
    private long[] result1;
    private long currentResult;
    private int currentProbe;
    private JoinProbe probe;
    private Page returnPage;
    private final boolean reuseResult;
    private boolean finishing;
    private final DictionaryId dictionaryId;

    private boolean[] nullsInReserve;
    private boolean[] nullsInBatch;

    AriaProbe(LookupSource.PartitionToLookupSourceSupplier partitionToLookupSourceSupplier,
            boolean reuseResult)
    {
        this.partitionToLookupSourceSupplier = partitionToLookupSourceSupplier;
        this.reuseResult = reuseResult;
        dictionaryId = DictionaryId.randomDictionaryId();
    }

    public void addInput(JoinProbe probe)
    {
        this.probe = probe;
        Block[] probes = probe.getProbeBlocks();
        k1.decodeBlock(probes[0]);
        k2.decodeBlock(probes[1]);
        int positionCount = probe.getPage().getPositionCount();
        if (partitions == null || partitions.length < positionCount) {
            partitions = new int[(int) (positionCount * 1.2)];
        }

        Block hashBlock = probe.getProbeHashBlock();
        if (hashBlock != null && partitionToLookupSourceSupplier.getPartitionCount() > 1) {
            if (hashDecoder == null) {
                hashDecoder = new BlockDecoder();
            }
            hashDecoder.decodeBlock(hashBlock);
            long[] longs = hashDecoder.getValues(long[].class);
            int[] map = hashDecoder.getRowNumberMap();
            for (int i = 0; i < positionCount; i++) {
                partitions[i] = partitionToLookupSourceSupplier.getPartition(longs[map[i]]);
            }
            hashDecoder.release();
        }
        else {
            Arrays.fill(partitions, 0);
        }

        if (hashes == null || hashes.length < positionCount) {
            hashes = new long[positionCount + 10];
        }
        nullsInBatch = null;
        k1d = k1.getValues(long[].class);
        k2d = k2.getValues(long[].class);
        k1Map = k1.getRowNumberMap();
        k2Map = k2.getRowNumberMap();
        addNullFlags(k1.getValueIsNull(), k1.isIdentityMap() ? null : k1Map, positionCount);
        addNullFlags(k2.getValueIsNull(), k2.isIdentityMap() ? null : k2Map, positionCount);
        candidateFill = 0;
        candidates = ensureCapacity(candidates, positionCount); // Should only be resized once

        if (nullsInBatch != null) {
            for (int i = 0; i < positionCount; ++i) {
                if (nullsInBatch[i]) {
                    candidates[candidateFill++] = i;
                }
            }
        }
        else {
            for (int i = 0; i < positionCount; ++i) {
                candidates[i] = i;
            }
            candidateFill = positionCount;
        }

        // TODO: layout specific
        for (int i = 0; i < candidateFill; ++i) {
            int row = candidates[i];
            long h;
            long k1 = k1d[k1Map[row]];
            k1 *= M;
            k1 ^= k1 >> 47;
            h = k1 * M;
            long k0 = k2d[k2Map[row]];
            k0 *= M;
            k0 ^= k0 >> 47;
            k0 *= M;
            h ^= k0;
            h *= M;

            hashes[row] = h;
        }

        if (result1 == null) {
            result1 = new long[maxResults];
            resultMap = new int[maxResults];
        }
        currentProbe = 0;
        currentResult = -1;
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

    private boolean addResult(long entry, int candidate)
    {
        int probeRow = candidates[candidate];
        AriaLookupSource table = (AriaLookupSource) partitionToLookupSourceSupplier.getLookupSource(partitions[candidate]);
        Slice[] slices = table.slices;

        do {
            resultMap[resultFill] = probeRow;
            Slice slice;
            int aoffset;
            {
                slice = slices[(int) ((entry) >> 17)];
                aoffset = (int) (entry) & 0x1ffff;
            }
            result1[resultFill] = getLongUnchecked(slice, aoffset + 16);
            entry = getLongUnchecked(slice, aoffset + 24);
            ++resultFill;
            if (resultFill >= maxResults) {
                currentResult = entry;
                currentProbe = candidate;
                finishResult();
                return true;
            }
        }
        while (entry != -1);
        currentResult = -1;
        return false;
    }

    private void finishResult()
    {
        if (currentResult == -1 && currentProbe < candidateFill) {
            ++currentProbe;
        }
        if (currentProbe == candidateFill) {
            k1.release();
            k2.release();
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
        Page resultPage = new Page(resultFill, blocks);
        if (!reuseResult) {
            result1 = new long[maxResults];
            resultMap = new int[maxResults];
        }
        resultFill = 0;
        returnPage = resultPage;
    }

    public boolean needsInput()
    {
        return currentResult == -1 && currentProbe == candidateFill;
    }

    public void finish()
    {
        finishing = true;
    }

    public boolean isFinished()
    {
        System.out.println("probe is finished: " + (finishing && currentResult == -1 && currentProbe == candidateFill));
        System.out.println("probe is finished (finishing): " + (finishing));
        System.out.println("probe is finished (currentResult == -1): " + (currentResult == -1));
        System.out.println("probe is finished (currentProbe == candidateFill): " + (currentProbe == candidateFill));
        return finishing && currentResult == -1 && currentProbe == candidateFill;
    }

    public Page getOutput()
    {
        if (currentResult != -1) {
            if (addResult(currentResult, currentProbe)) {
                return returnPage;
            }
        }
        long tempHash;
        boolean unroll = true;
        int unrollFill = candidateFill;
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
            long entry1 = -1;
            long field1;
            long empty1;
            long hits1;
            int hash1;
            int row1;
            boolean match1 = false;
            Slice g1slice;
            int g1offset;
            long entry2 = -1;
            long field2;
            long empty2;
            long hits2;
            int hash2;
            int row2;
            boolean match2 = false;
            Slice g2slice;
            int g2offset;
            long entry3 = -1;
            long field3;
            long empty3;
            long hits3;
            int hash3;
            int row3;
            boolean match3 = false;
            Slice g3slice;
            int g3offset;
            AriaLookupSource table0 = (AriaLookupSource) partitionToLookupSourceSupplier.getLookupSource(partitions[candidates[currentProbe]]);
            int statusMask0 = table0.statusMask;
            Slice[] slices0 = table0.slices;
            row0 = candidates[currentProbe];
            tempHash = hashes[row0];
            hash0 = (int) tempHash & statusMask0;
            field0 = (tempHash >> 56) & 0x7f;
            hits0 = table0.status[hash0];
            field0 |= field0 << 8;
            field0 |= field0 << 16;
            field0 |= field0 << 32;
            AriaLookupSource table1 = (AriaLookupSource) partitionToLookupSourceSupplier.getLookupSource(partitions[candidates[currentProbe + 1]]);
            int statusMask1 = table1.statusMask;
            Slice[] slices1 = table1.slices;
            row1 = candidates[currentProbe + 1];
            tempHash = hashes[row1];
            hash1 = (int) tempHash & statusMask1;
            field1 = (tempHash >> 56) & 0x7f;
            hits1 = table1.status[hash1];
            field1 |= field1 << 8;
            field1 |= field1 << 16;
            field1 |= field1 << 32;
            AriaLookupSource table2 = (AriaLookupSource) partitionToLookupSourceSupplier.getLookupSource(partitions[candidates[currentProbe + 2]]);
            int statusMask2 = table2.statusMask;
            Slice[] slices2 = table2.slices;
            row2 = candidates[currentProbe + 2];
            tempHash = hashes[row2];
            hash2 = (int) tempHash & statusMask2;
            field2 = (tempHash >> 56) & 0x7f;
            hits2 = table2.status[hash2];
            field2 |= field2 << 8;
            field2 |= field2 << 16;
            field2 |= field2 << 32;
            AriaLookupSource table3 = (AriaLookupSource) partitionToLookupSourceSupplier.getLookupSource(partitions[candidates[currentProbe + 3]]);
            int statusMask3 = table3.statusMask;
            Slice[] slices3 = table3.slices;
            row3 = candidates[currentProbe + 3];
            tempHash = hashes[row3];
            hash3 = (int) tempHash & statusMask3;
            field3 = (tempHash >> 56) & 0x7f;
            hits3 = table3.status[hash3];
            field3 |= field3 << 8;
            field3 |= field3 << 16;
            field3 |= field3 << 32;
            empty0 = hits0 & 0x8080808080808080L;
            hits0 ^= field0;
            hits0 -= 0x0101010101010101L;
            hits0 &= 0x8080808080808080L ^ empty0;
            if (hits0 != 0) {
                int pos = Long.numberOfTrailingZeros(hits0) >> 3;
                hits0 &= hits0 - 1;
                entry0 = table0.table[hash0 * 8 + pos];
                {
                    g0slice = slices0[(int) ((entry0) >> 17)];
                    g0offset = (int) (entry0) & 0x1ffff;
                }
                match0 = getLongUnchecked(g0slice, g0offset) == k1d[k1Map[row0]]
                        & getLongUnchecked(g0slice, g0offset + 8) == k2d[k2Map[row0]];
            }
            empty1 = hits1 & 0x8080808080808080L;
            hits1 ^= field1;
            hits1 -= 0x0101010101010101L;
            hits1 &= 0x8080808080808080L ^ empty1;
            if (hits1 != 0) {
                int pos = Long.numberOfTrailingZeros(hits1) >> 3;
                hits1 &= hits1 - 1;
                entry1 = table1.table[hash1 * 8 + pos];
                {
                    g1slice = slices1[(int) ((entry1) >> 17)];
                    g1offset = (int) (entry1) & 0x1ffff;
                }
                match1 = getLongUnchecked(g1slice, g1offset) == k1d[k1Map[row1]]
                        & getLongUnchecked(g1slice, g1offset + 8) == k2d[k2Map[row1]];
            }
            empty2 = hits2 & 0x8080808080808080L;
            hits2 ^= field2;
            hits2 -= 0x0101010101010101L;
            hits2 &= 0x8080808080808080L ^ empty2;
            if (hits2 != 0) {
                int pos = Long.numberOfTrailingZeros(hits2) >> 3;
                hits2 &= hits2 - 1;
                entry2 = table2.table[hash2 * 8 + pos];
                g2slice = slices2[(int) ((entry2) >> 17)];
                g2offset = (int) (entry2) & 0x1ffff;
                match2 = getLongUnchecked(g2slice, g2offset) == k1d[k1Map[row2]]
                        & getLongUnchecked(g2slice, g2offset + 8) == k2d[k2Map[row2]];
            }
            empty3 = hits3 & 0x8080808080808080L;
            hits3 ^= field3;
            hits3 -= 0x0101010101010101L;
            hits3 &= 0x8080808080808080L ^ empty3;
            if (hits3 != 0) {
                int pos = Long.numberOfTrailingZeros(hits3) >> 3;
                hits3 &= hits3 - 1;

                entry3 = table3.table[hash3 * 8 + pos];
                {
                    g3slice = slices3[(int) ((entry3) >> 17)];
                    g3offset = (int) (entry3) & 0x1ffff;
                }
                match3 = getLongUnchecked(g3slice, g3offset) == k1d[k1Map[row3]]
                        & getLongUnchecked(g3slice, g3offset + 8) == k2d[k2Map[row3]];
            }
            if (match0) {
                if (addResult(entry0, currentProbe)) {
                    return returnPage;
                }
            }
            else {
                bucketLoop0:
                for (; ; ) {
                    while (hits0 != 0) {
                        int pos = Long.numberOfTrailingZeros(hits0) >> 3;
                        entry0 = table0.table[hash0 * 8 + pos];
                        {
                            g0slice = slices0[(int) ((entry0) >> 17)];
                            g0offset = (int) (entry0) & 0x1ffff;
                        }
                        if (getLongUnchecked(g0slice, g0offset) == k1d[k1Map[row0]]
                                && getLongUnchecked(g0slice, g0offset + 8) == k2d[k2Map[row0]]) {
                            if (addResult(entry0, currentProbe)) {
                                return returnPage;
                            }
                            break bucketLoop0;
                        }
                        hits0 &= hits0 - 1;
                    }
                    if (empty0 != 0) {
                        break;
                    }
                    hash0 = (hash0 + 1) & statusMask0;
                    hits0 = table0.status[hash0];
                    empty0 = hits0 & 0x8080808080808080L;
                    hits0 ^= field0;
                    hits0 -= 0x0101010101010101L;
                    hits0 &= 0x8080808080808080L ^ empty0;
                }
            }
            if (match1) {
                if (addResult(entry1, currentProbe + 1)) {
                    return returnPage;
                }
            }
            else {
                bucketLoop1:
                for (; ; ) {
                    while (hits1 != 0) {
                        int pos = Long.numberOfTrailingZeros(hits1) >> 3;
                        entry1 = table1.table[hash1 * 8 + pos];
                        {
                            g1slice = slices1[(int) ((entry1) >> 17)];
                            g1offset = (int) (entry1) & 0x1ffff;
                        }
                        if (getLongUnchecked(g1slice, g1offset) == k1d[k1Map[row1]]
                                && getLongUnchecked(g1slice, g1offset + 8) == k2d[k2Map[row1]]) {
                            if (addResult(entry1, currentProbe + 1)) {
                                return returnPage;
                            }
                            break bucketLoop1;
                        }
                        hits1 &= hits1 - 1;
                    }
                    if (empty1 != 0) {
                        break;
                    }
                    hash1 = (hash1 + 1) & statusMask1;
                    hits1 = table1.status[hash1];
                    empty1 = hits1 & 0x8080808080808080L;
                    hits1 ^= field1;
                    hits1 -= 0x0101010101010101L;
                    hits1 &= 0x8080808080808080L ^ empty1;
                }
            }
            if (match2) {
                if (addResult(entry2, currentProbe + 2)) {
                    return returnPage;
                }
            }
            else {
                bucketLoop2:
                for (; ; ) {
                    while (hits2 != 0) {
                        int pos = Long.numberOfTrailingZeros(hits2) >> 3;
                        entry2 = table2.table[hash2 * 8 + pos];
                        {
                            g2slice = slices2[(int) ((entry2) >> 17)];
                            g2offset = (int) (entry2) & 0x1ffff;
                        }
                        if (getLongUnchecked(g2slice, g2offset) == k1d[k1Map[row2]]
                                && getLongUnchecked(g2slice, g2offset + 8) == k2d[k2Map[row2]]) {
                            if (addResult(entry2, currentProbe + 2)) {
                                return returnPage;
                            }
                            break bucketLoop2;
                        }
                        hits2 &= hits2 - 1;
                    }
                    if (empty2 != 0) {
                        break;
                    }
                    hash2 = (hash2 + 1) & statusMask2;
                    hits2 = table2.status[hash2];
                    empty2 = hits2 & 0x8080808080808080L;
                    hits2 ^= field2;
                    hits2 -= 0x0101010101010101L;
                    hits2 &= 0x8080808080808080L ^ empty2;
                }
            }
            if (match3) {
                if (addResult(entry3, currentProbe + 3)) {
                    return returnPage;
                }
            }
            else {
                bucketLoop3:
                for (; ; ) {
                    while (hits3 != 0) {
                        int pos = Long.numberOfTrailingZeros(hits3) >> 3;
                        entry3 = table3.table[hash3 * 8 + pos];
                        {
                            g3slice = slices3[(int) ((entry3) >> 17)];
                            g3offset = (int) (entry3) & 0x1ffff;
                        }
                        if (getLongUnchecked(g3slice, g3offset) == k1d[k1Map[row3]]
                                && getLongUnchecked(g3slice, g3offset + 8) == k2d[k2Map[row3]]) {
                            if (addResult(entry3, currentProbe + 3)) {
                                return returnPage;
                            }
                            break bucketLoop3;
                        }
                        hits3 &= hits3 - 1;
                    }
                    if (empty3 != 0) {
                        break;
                    }
                    hash3 = (hash3 + 1) & statusMask3;
                    hits3 = table3.status[hash3];
                    empty3 = hits3 & 0x8080808080808080L;
                    hits3 ^= field3;
                    hits3 -= 0x0101010101010101L;
                    hits3 &= 0x8080808080808080L ^ empty3;
                }
            }
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
            AriaLookupSource table0 = (AriaLookupSource) partitionToLookupSourceSupplier.getLookupSource(partitions[candidates[currentProbe]]);
            int statusMask0 = table0.statusMask;
            Slice[] slices0 = table0.slices;
            row0 = candidates[currentProbe];
            tempHash = hashes[row0];
            hash0 = (int) tempHash & statusMask0;
            field0 = (tempHash >> 56) & 0x7f;
            hits0 = table0.status[hash0];
            field0 |= field0 << 8;
            field0 |= field0 << 16;
            field0 |= field0 << 32;
            empty0 = hits0 & 0x8080808080808080L;
            hits0 ^= field0;
            hits0 -= 0x0101010101010101L;
            hits0 &= 0x8080808080808080L ^ empty0;
            if (hits0 != 0) {
                int pos = Long.numberOfTrailingZeros(hits0) >> 3;
                hits0 &= hits0 - 1;
                entry0 = table0.table[hash0 * 8 + pos];
                {
                    g0slice = slices0[(int) ((entry0) >> 17)];
                    g0offset = (int) (entry0) & 0x1ffff;
                }
                match0 = getLongUnchecked(g0slice, g0offset) == k1d[k1Map[row0]]
                        & getLongUnchecked(g0slice, g0offset + 8) == k2d[k2Map[row0]];
            }
            if (match0) {
                if (addResult(entry0, currentProbe)) {
                    return returnPage;
                }
            }
            else {
                bucketLoop0:
                for (; ; ) {
                    while (hits0 != 0) {
                        int pos = Long.numberOfTrailingZeros(hits0) >> 3;
                        entry0 = table0.table[hash0 * 8 + pos];
                        {
                            g0slice = slices0[(int) ((entry0) >> 17)];
                            g0offset = (int) (entry0) & 0x1ffff;
                        }
                        if (getLongUnchecked(g0slice, g0offset) == k1d[k1Map[row0]]
                                && getLongUnchecked(g0slice, g0offset + 8) == k2d[k2Map[row0]]) {
                            if (addResult(entry0, currentProbe)) {
                                return returnPage;
                            }
                            break bucketLoop0;
                        }
                        hits0 &= hits0 - 1;
                    }
                    if (empty0 != 0) {
                        break;
                    }
                    hash0 = (hash0 + 1) & statusMask0;
                    hits0 = table0.status[hash0];
                    empty0 = hits0 & 0x8080808080808080L;
                    hits0 ^= field0;
                    hits0 -= 0x0101010101010101L;
                    hits0 &= 0x8080808080808080L ^ empty0;
                }
            }
        }
        finishResult();
        return returnPage;
    }
}
