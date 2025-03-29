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

package com.facebook.presto.type.khyperloglog;

import com.facebook.airlift.stats.cardinality.HyperLogLog;
import com.google.common.collect.Sets;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Murmur3Hash128;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongRBTreeSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.stream.LongStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.wrappedIntArray;
import static io.airlift.slice.Slices.wrappedLongArray;
import static java.util.Objects.requireNonNull;

/**
 * For reference on KHyperLogLog, see "KHyperLogLog: Estimating Reidentifiability and
 * Joinability of Large Data at Scale" by Chia et al., 2019.
 */
public class KHyperLogLog
{
    public static final int DEFAULT_HLL_BUCKETS = 256;
    public static final int DEFAULT_MAX_SIZE = 4096;
    public static final long DEFAULT_HISTOGRAM_SIZE = 256;
    private static final byte VERSION_BYTE = 1;
    private static final long HASH_OUTPUT_HALF_RANGE = Long.MAX_VALUE;
    private static final int SIZE_OF_KHYPERLOGLOG = ClassLayout.parseClass(KHyperLogLog.class).instanceSize();
    private static final int SIZE_OF_RBTREEMAP = ClassLayout.parseClass(Long2ObjectRBTreeMap.class).instanceSize();

    private final Long2ObjectSortedMap<HyperLogLog> minhash;
    private final int maxSize;
    private final int hllBuckets;

    private int hllsTotalEstimatedInMemorySize;
    private int hllsTotalEstimatedSerializedSize;

    public KHyperLogLog()
    {
        this(DEFAULT_MAX_SIZE, DEFAULT_HLL_BUCKETS, new Long2ObjectRBTreeMap<>());
    }

    public KHyperLogLog(int maxSize, int hllBuckets)
    {
        this(maxSize, hllBuckets, new Long2ObjectRBTreeMap<>());
    }

    public KHyperLogLog(int maxSize, int hllBuckets, Long2ObjectSortedMap<HyperLogLog> minhash)
    {
        this.maxSize = maxSize;
        this.hllBuckets = hllBuckets;
        this.minhash = requireNonNull(minhash, "minhash is null");

        minhash.values().forEach(this::increaseTotalHllSize);
    }

    public static KHyperLogLog newInstance(Slice serialized)
    {
        requireNonNull(serialized, "serialized is null");
        SliceInput input = serialized.getInput();
        checkArgument(input.readByte() == VERSION_BYTE, "Unexpected version");
        Long2ObjectRBTreeMap<HyperLogLog> minhash = new Long2ObjectRBTreeMap<>();

        int maxSize = input.readInt();
        int hllBuckets = input.readInt();
        int minhashSize = input.readInt();
        int totalHllSize = input.readInt();

        int[] hllSizes = new int[minhashSize];
        long[] keys = new long[minhashSize];
        input.readBytes(wrappedIntArray(hllSizes));
        input.readBytes(wrappedLongArray(keys));

        Slice allSerializedHlls = input.readSlice(totalHllSize);

        int hllLength;
        int index = 0;
        for (int i = 0; i < minhashSize; i++) {
            Slice serializedHll;
            hllLength = hllSizes[i];
            serializedHll = allSerializedHlls.slice(index, hllLength);
            index += hllLength;
            minhash.put(keys[i], HyperLogLog.newInstance(serializedHll));
        }

        return new KHyperLogLog(maxSize, hllBuckets, minhash);
    }

    public Slice serialize()
    {
        try (SliceOutput output = new DynamicSliceOutput(estimatedSerializedSize())) {
            List<Slice> hllSlices = new ArrayList<>();
            IntList hllSizes = new IntArrayList();
            int totalHllSize = 0;

            for (HyperLogLog hll : minhash.values()) {
                Slice serializedHll = hll.serialize();
                hllSlices.add(serializedHll);
                totalHllSize += serializedHll.length();
                hllSizes.add(serializedHll.length());
            }

            Slice hashesSlice = wrappedLongArray(minhash.keySet().toLongArray());
            Slice hllSizesSlice = wrappedIntArray(hllSizes.toIntArray());

            output.appendByte(VERSION_BYTE);
            output.appendInt(maxSize);
            output.appendInt(hllBuckets);
            output.appendInt(minhash.size());
            output.appendInt(totalHllSize);
            output.appendBytes(hllSizesSlice);
            output.appendBytes(hashesSlice);
            for (Slice hllSlice : hllSlices) {
                output.appendBytes(hllSlice);
            }

            return output.slice();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static long exactIntersectionCardinality(KHyperLogLog a, KHyperLogLog b)
    {
        checkState(a.isExact(), "exact intersection cannot operate on approximate sets");
        checkArgument(b.isExact(), "exact intersection cannot operate on approximate sets");

        return Sets.intersection(a.minhash.keySet(), b.minhash.keySet()).size();
    }

    public static double jaccardIndex(KHyperLogLog a, KHyperLogLog b)
    {
        int sizeOfSmallerSet = Math.min(a.minhash.size(), b.minhash.size());
        LongSortedSet minUnion = new LongRBTreeSet(a.minhash.keySet());
        minUnion.addAll(b.minhash.keySet());

        int intersection = 0;
        int i = 0;

        LongIterator iterator = minUnion.iterator();
        while (iterator.hasNext()) {
            long key = iterator.nextLong();
            if (a.minhash.containsKey(key) && b.minhash.containsKey(key)) {
                intersection++;
            }
            i++;
            if (i >= sizeOfSmallerSet) {
                break;
            }
        }
        return intersection / (double) sizeOfSmallerSet;
    }

    public static KHyperLogLog merge(KHyperLogLog khll1, KHyperLogLog khll2)
    {
         // Return the one with smallest K so resolution is not lost. This loss would happen in the case
         // one merged a smaller KHLL into a bigger one because the former's minhash struct won't
         // cover all of the latter's minhash space.
        if (khll1.maxSize <= khll2.maxSize) {
            return khll1.mergeWith(khll2);
        }
        return khll2.mergeWith(khll1);
    }

    public boolean isExact()
    {
        return minhash.size() < maxSize;
    }

    public long getMinhashSize()
    {
        return minhash.size();
    }

    public int estimatedInMemorySize()
    {
        return SIZE_OF_KHYPERLOGLOG +
                SIZE_OF_RBTREEMAP +
                minhash.size() * SIZE_OF_LONG +
                hllsTotalEstimatedInMemorySize * SIZE_OF_BYTE;
    }

    public int estimatedSerializedSize()
    {
        // version byte +
        // maxSize + hllBuckets + minhashSize + totalHllSize +
        // minhash keys + individual HLL sizes +
        // HLLs size sum
        return SIZE_OF_BYTE +
                4 * SIZE_OF_INT +
                minhash.size() * (SIZE_OF_LONG + SIZE_OF_INT) +
                hllsTotalEstimatedSerializedSize * SIZE_OF_BYTE;
    }

    public void add(long value, long uii)
    {
        update(Murmur3Hash128.hash64(value), uii);
    }

    public void add(Slice value, long uii)
    {
        update(Murmur3Hash128.hash64(value), uii);
    }

    private void update(long hash, long uii)
    {
        if (!(minhash.containsKey(hash) || isExact() || hash < minhash.lastLongKey())) {
            return;
        }

        HyperLogLog hll = minhash.computeIfAbsent(hash, k -> {
            HyperLogLog newHll = HyperLogLog.newInstance(hllBuckets);
            increaseTotalHllSize(newHll);
            return newHll;
        });

        decreaseTotalHllSize(hll);
        hll.add(uii);
        increaseTotalHllSize(hll);

        removeOverflowEntries();
    }

    public long cardinality()
    {
        if (isExact()) {
            return minhash.size();
        }

        // Intuition is: get the stored hashes' density, and extrapolate to the whole Hash output range.
        // Since Hash output range (2^64) cannot be stored in long type, I use half of the range
        // via Long.MAX_VALUE and also divide the hash values' density by 2. The "-1" is bias correction
        // detailed in "On Synopses for Distinct-Value Estimation Under Multiset Operations" by Beyer et. al.
        long hashesRange = minhash.lastLongKey() - Long.MIN_VALUE;
        double halfDensity = Long.divideUnsigned(hashesRange, minhash.size() - 1) / 2D;
        return (long) (HASH_OUTPUT_HALF_RANGE / halfDensity);
    }

    public KHyperLogLog mergeWith(KHyperLogLog other)
    {
        LongIterator iterator = other.minhash.keySet().iterator();
        while (iterator.hasNext()) {
            long key = iterator.nextLong();
            HyperLogLog thisHll = minhash.get(key);
            HyperLogLog otherHll = other.minhash.get(key);
            if (minhash.containsKey(key)) {
                decreaseTotalHllSize(thisHll);
                thisHll.mergeWith(otherHll);
                increaseTotalHllSize(thisHll);
            }
            else {
                minhash.put(key, otherHll);
                increaseTotalHllSize(otherHll);
            }
        }

        removeOverflowEntries();

        return this;
    }

    public double reidentificationPotential(long threshold)
    {
        long highlyUniqueValues = minhash.values().stream()
                .map(HyperLogLog::cardinality)
                .filter(cardinality -> cardinality <= threshold)
                .count();

        return (double) highlyUniqueValues / minhash.size();
    }

    public Long2DoubleMap uniquenessDistribution()
    {
        return uniquenessDistribution(DEFAULT_HISTOGRAM_SIZE);
    }

    public Long2DoubleMap uniquenessDistribution(long histogramSize)
    {
        Long2DoubleMap out = new Long2DoubleOpenHashMap();
        PrimitiveIterator.OfLong iterator = LongStream.rangeClosed(1, histogramSize).iterator();
        while (iterator.hasNext()) {
            // Initialize all entries to zero
            out.put(iterator.nextLong(), 0D);
        }

        int size = minhash.size();
        for (HyperLogLog hll : minhash.values()) {
            long bucket = Math.min(hll.cardinality(), histogramSize);
            out.merge(bucket, (double) 1 / size, Double::sum);
        }
        return out;
    }

    private void removeOverflowEntries()
    {
        while (minhash.size() > maxSize) {
            HyperLogLog hll = minhash.remove(minhash.lastLongKey());
            decreaseTotalHllSize(hll);
        }
    }

    private void decreaseTotalHllSize(HyperLogLog hll)
    {
        hllsTotalEstimatedInMemorySize -= hll.estimatedInMemorySize();
        hllsTotalEstimatedSerializedSize -= hll.estimatedSerializedSize();
    }

    private void increaseTotalHllSize(HyperLogLog hll)
    {
        hllsTotalEstimatedInMemorySize += hll.estimatedInMemorySize();
        hllsTotalEstimatedSerializedSize += hll.estimatedSerializedSize();
    }
}
