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
import static it.unimi.dsi.fastutil.longs.LongArrays.ensureCapacity;

// TODO layout specific
public class LayoutSpecificProbeHelper
        implements AutoCloseable
{
    private static final int MAX_RESULTS = 1024;
    private static final long M = 0xc6a4a7935bd1e995L;

    private final LookupSource.PartitionToLookupSourceSupplier partitionToLookupSourceSupplier;
    private final DictionaryId dictionaryId = DictionaryId.randomDictionaryId();
    private JoinProbe probe;

    private long[] hashes = new long[] {};
    private int[] partitions;
    private boolean[] nullsInReserve;
    private boolean[] nullsInBatch;
    private int[] candidates;
    private int candidateFill;

    private long currentResult;
    private int currentProbe;
    private int resultFill;
    private Page returnPage;

    // Layout specific members
    private final BlockDecoder k1 = new BlockDecoder();
    private final BlockDecoder k2 = new BlockDecoder();
    private long[] k1d;
    private long[] k2d;
    private int[] k1Map;
    private int[] k2Map;
    private int[] resultMap = new int[MAX_RESULTS];
    private long[] result = new long[MAX_RESULTS];
    private BlockDecoder hashDecoder = new BlockDecoder();

    public LayoutSpecificProbeHelper(LookupSource.PartitionToLookupSourceSupplier partitionToLookupSourceSupplier)
    {
        this.partitionToLookupSourceSupplier = partitionToLookupSourceSupplier;
    }

    public final void init(JoinProbe probe)
    {
        this.probe = probe;
        int positionCount = probe.getPage().getPositionCount();
        Block[] probes = probe.getProbeBlocks();

        k1.decodeBlock(probes[0]);
        k2.decodeBlock(probes[1]);
        k1d = k1.getValues(long[].class);
        k2d = k2.getValues(long[].class);
        k1Map = k1.getRowNumberMap();
        k2Map = k2.getRowNumberMap();

        nullsInBatch = null;
        addNullFlags(k1.getValueIsNull(), k1.isIdentityMap() ? null : k1Map, positionCount);
        addNullFlags(k2.getValueIsNull(), k2.isIdentityMap() ? null : k2Map, positionCount);

        candidateFill = 0;
        candidates = new int[positionCount];

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

        currentProbe = 0;
        currentResult = -1;

        initializePartitions(probe, positionCount);
        initializeHashes(positionCount);
    }

    private void initializePartitions(JoinProbe probe, int positionCount)
    {
        if (partitions == null || partitions.length < positionCount) {
            partitions = new int[(int) (positionCount * 1.2)];
        }

        Block hashBlock = probe.getProbeHashBlock();
        if (hashBlock != null && partitionToLookupSourceSupplier.getPartitionCount() > 1) {
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
    }

    private void initializeHashes(int positionCount)
    {
        hashes = ensureCapacity(hashes, positionCount);
        for (int i = 0; i < getCandidateFill(); ++i) {
            int candidateIndex = getCandidate(i);
            hashes[candidateIndex] = hashRow(candidateIndex);
        }
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
        int probeRow = getCandidate(candidate);
        LayoutSpecificAriaHash.AriaLookupSource table = (LayoutSpecificAriaHash.AriaLookupSource) partitionToLookupSourceSupplier.getLookupSource(partitions[candidate]);
        Slice[] slices = table.slices;

        do {
            resultMap[resultFill] = probeRow;
            Slice slice = slices[(int) ((entry) >> 17)];
            int offset = (int) (entry) & 0x1ffff;

            result[resultFill] = getLongUnchecked(slice, offset + 16);
            entry = getLongUnchecked(slice, offset + 24);
            ++resultFill;
            if (resultFill >= MAX_RESULTS) {
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

    private int getCandidate(int position)
    {
        return candidates[position];
    }

    private int getCandidateFill()
    {
        return candidateFill;
    }

    @Override
    public void close()
    {
        k1.release();
        k2.release();
    }

    private long hashRow(int row)
    {
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
        return h;
    }

    private boolean isMatch(Slice slice, int offset, int row)
    {
        return getLongUnchecked(slice, offset) == k1d[k1Map[row]]
                && getLongUnchecked(slice, offset + 8) == k2d[k2Map[row]];
    }

    public void finishResult()
    {
        if (currentResult == -1 && currentProbe < getCandidateFill()) {
            ++currentProbe;
        }
        if (currentProbe == getCandidateFill()) {
            close();
        }

        if (resultFill == 0) {
            returnPage = null;
            return;
        }
        Page page = probe.getPage();
        int[] probeOutputChannels = probe.getOutputChannels();
        Block[] blocks = new Block[probeOutputChannels.length + 1];
        for (int i = 0; i < probeOutputChannels.length; i++) {
            blocks[i] = new DictionaryBlock(
                    0,
                    resultFill,
                    page.getBlock(probeOutputChannels[i]),
                    resultMap,
                    false,
                    dictionaryId);
        }
        blocks[probeOutputChannels.length] =
                new LongArrayBlock(resultFill, Optional.empty(), result);
        Page resultPage = new Page(resultFill, blocks);
        resultFill = 0;
        returnPage = resultPage;
    }

    public boolean needsInput()
    {
        return currentResult == -1 && currentProbe == getCandidateFill();
    }

    public boolean isFinished()
    {
        return currentResult == -1 && currentProbe == getCandidateFill();
    }

    public Page getOutput()
    {
        if (currentResult != -1) {
            if (addResult(currentResult, currentProbe)) {
                return returnPage;
            }
        }
        int unrollFill = getCandidateFill();

        ProbeHelper t0 = new ProbeHelper(this);
        ProbeHelper t1 = new ProbeHelper(this);
        ProbeHelper t2 = new ProbeHelper(this);
        ProbeHelper t3 = new ProbeHelper(this);

        for (; currentProbe + 3 < unrollFill; currentProbe += 4) {
            t0.preprobe(getCandidate(currentProbe), hashes[currentProbe]);
            t1.preprobe(getCandidate(currentProbe + 1), hashes[currentProbe + 1]);
            t2.preprobe(getCandidate(currentProbe + 2), hashes[currentProbe + 2]);
            t3.preprobe(getCandidate(currentProbe + 3), hashes[currentProbe + 3]);

            t0.firstProbe();
            t1.firstProbe();
            t2.firstProbe();
            t3.firstProbe();

            if (t0.fullProbe(currentProbe)) {
                return returnPage;
            }
            if (t1.fullProbe(currentProbe + 1)) {
                return returnPage;
            }
            if (t2.fullProbe(currentProbe + 2)) {
                return returnPage;
            }
            if (t3.fullProbe(currentProbe + 3)) {
                return returnPage;
            }
        }

        ProbeHelper helper = new ProbeHelper(this);
        for (; currentProbe < getCandidateFill(); ++currentProbe) {
            helper.preprobe(getCandidate(currentProbe), hashes[currentProbe]);
            helper.firstProbe();
            if (helper.fullProbe(currentProbe)) {
                return returnPage;
            }
        }

        finishResult();
        return returnPage;
    }

    private final class ProbeHelper
    {
        private final LayoutSpecificProbeHelper concurrentFetchProbe;
        private LayoutSpecificAriaHash.AriaLookupSource table;
        private int sliceMask;
        private Slice[] slices;

        private long entry = -1;
        private long field;
        private long empty;
        private long hits;
        private int hash;
        private int row;
        private boolean match;
        private Slice slice;
        private int offset;

        private ProbeHelper(LayoutSpecificProbeHelper concurrentFetchProbe)
        {
            this.concurrentFetchProbe = concurrentFetchProbe;
        }

        private void preprobe(int row, long hashInput)
        {
            table = (LayoutSpecificAriaHash.AriaLookupSource) partitionToLookupSourceSupplier.getLookupSource(partitions[row]);
            sliceMask = table.statusMask;
            slices = table.slices;

            this.row = row;
            hash = (int) hashInput & sliceMask;
            hits = table.status[hash];
            // Extract 7 bit hash number extract
            field = (hashInput >> 56) & 0x7f;
            // Fill word with extract
            field |= field << 8;
            field |= field << 16;
            field |= field << 32;
        }

        private void firstProbe()
        {
            processHits();
            if (hits != 0) {
                int pos = getPositionFromStatusMask(hits);
                hits &= hits - 1;
                loadRow(pos);
                match = isMatch(slice, offset, row);
            }
        }

        private boolean fullProbe(int probePosition)
        {
            if (match) {
                match = false;
                return concurrentFetchProbe.addResult(entry, probePosition);
            }
            else {
                bucketLoop0:
                while (true) {
                    while (hits != 0) {
                        loadRow(getPositionFromStatusMask(hits));
                        if (concurrentFetchProbe.isMatch(slice, offset, row)) {
                            if (concurrentFetchProbe.addResult(entry, probePosition)) {
                                return true;
                            }
                            break bucketLoop0;
                        }
                        // Clear lowest bit
                        hits &= hits - 1;
                    }
                    if (empty != 0) {
                        break;
                    }

                    hash = (hash + 1) & sliceMask;
                    hits = table.status[hash];
                    processHits();
                }
            }
            return false;
        }

        private void processHits()
        {
            empty = hits & 0x8080808080808080L; // Represents empty positions
            hits ^= field; // Hit is represented as 0 byte
            hits -= 0x0101010101010101L; // Flip the high bits for each hit
            hits &= 0x8080808080808080L ^ empty; // Xor out the empty positions
        }

        private void loadRow(int pos)
        {
            entry = table.table[hash * 8 + pos];
            slice = slices[(int) ((entry) >> 17)];
            offset = (int) (entry) & 0x1ffff;
        }

        private int getPositionFromStatusMask(long status)
        {
            return Long.numberOfTrailingZeros(status) >> 3;
        }
    }
}
