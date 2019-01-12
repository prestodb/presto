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

package io.prestosql.type.setdigest;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.primitives.Shorts;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Murmur3;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.stats.cardinality.HyperLogLog;
import it.unimi.dsi.fastutil.longs.Long2ShortRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ShortSortedMap;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import it.unimi.dsi.fastutil.longs.LongRBTreeSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static java.util.Objects.requireNonNull;

/**
 * For the MinHash algorithm, see "On the resemblance and containment of documents" by Andrei Z. Broder,
 * and the Wikipedia page: http://en.wikipedia.org/wiki/MinHash#Variant_with_a_single_hash_function
 */
public class SetDigest
{
    private static final byte UNCOMPRESSED_FORMAT = 1;
    public static final int NUMBER_OF_BUCKETS = 2048;
    public static final int DEFAULT_MAX_HASHES = 8192;
    private static final int SIZE_OF_ENTRY = SIZE_OF_LONG + SIZE_OF_SHORT;
    private static final int SIZE_OF_SETDIGEST = ClassLayout.parseClass(SetDigest.class).instanceSize();
    private static final int SIZE_OF_RBTREEMAP = ClassLayout.parseClass(Long2ShortRBTreeMap.class).instanceSize();

    private final HyperLogLog hll;
    private final Long2ShortSortedMap minhash;
    private final int maxHashes;

    public SetDigest()
    {
        this(DEFAULT_MAX_HASHES, HyperLogLog.newInstance(NUMBER_OF_BUCKETS), new Long2ShortRBTreeMap());
    }

    public SetDigest(int maxHashes, int numHllBuckets)
    {
        this(maxHashes, HyperLogLog.newInstance(numHllBuckets), new Long2ShortRBTreeMap());
    }

    public SetDigest(int maxHashes, HyperLogLog hll, Long2ShortSortedMap minhash)
    {
        this.maxHashes = maxHashes;
        this.hll = requireNonNull(hll, "hll is null");
        this.minhash = requireNonNull(minhash, "minhash is null");
    }

    public static SetDigest newInstance(Slice serialized)
    {
        requireNonNull(serialized, "serialized is null");
        SliceInput input = serialized.getInput();
        checkArgument(input.readByte() == UNCOMPRESSED_FORMAT, "Unexpected version");

        int hllLength = input.readInt();
        Slice serializedHll = Slices.allocate(hllLength);
        input.readBytes(serializedHll, hllLength);
        HyperLogLog hll = HyperLogLog.newInstance(serializedHll);

        Long2ShortRBTreeMap minhash = new Long2ShortRBTreeMap();
        int maxHashes = input.readInt();
        int minhashLength = input.readInt();
        // The values are stored after the keys
        SliceInput valuesInput = serialized.getInput();
        valuesInput.setPosition(input.position() + minhashLength * SIZE_OF_LONG);

        for (int i = 0; i < minhashLength; i++) {
            minhash.put(input.readLong(), valuesInput.readShort());
        }

        return new SetDigest(maxHashes, hll, minhash);
    }

    public Slice serialize()
    {
        try (SliceOutput output = new DynamicSliceOutput(estimatedSerializedSize())) {
            output.appendByte(UNCOMPRESSED_FORMAT);
            Slice serializedHll = hll.serialize();
            output.appendInt(serializedHll.length());
            output.appendBytes(serializedHll);
            output.appendInt(maxHashes);
            output.appendInt(minhash.size());
            for (long key : minhash.keySet()) {
                output.appendLong(key);
            }
            for (short value : minhash.values()) {
                output.appendShort(value);
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public HyperLogLog getHll()
    {
        return hll;
    }

    public int estimatedInMemorySize()
    {
        return hll.estimatedInMemorySize() + minhash.size() * SIZE_OF_ENTRY + SIZE_OF_SETDIGEST + SIZE_OF_RBTREEMAP;
    }

    public int estimatedSerializedSize()
    {
        return SIZE_OF_BYTE + SIZE_OF_INT + hll.estimatedSerializedSize() + 2 * SIZE_OF_INT + minhash.size() * SIZE_OF_ENTRY;
    }

    public boolean isExact()
    {
        // There's an ambiguity when minhash.size() == maxHashes, since this could either
        // be an exact set with maxHashes elements, or an inexact one. Which is why strict
        // inequality is used here.
        return minhash.size() < maxHashes;
    }

    public long cardinality()
    {
        if (isExact()) {
            return minhash.size();
        }
        return hll.cardinality();
    }

    public static long exactIntersectionCardinality(SetDigest a, SetDigest b)
    {
        checkState(a.isExact(), "exact intersection cannot operate on approximate sets");
        checkArgument(b.isExact(), "exact intersection cannot operate on approximate sets");

        return Sets.intersection(a.minhash.keySet(), b.minhash.keySet()).size();
    }

    public static double jaccardIndex(SetDigest a, SetDigest b)
    {
        int sizeOfSmallerSet = Math.min(a.minhash.size(), b.minhash.size());
        LongSortedSet minUnion = new LongRBTreeSet(a.minhash.keySet());
        minUnion.addAll(b.minhash.keySet());

        int intersection = 0;
        int i = 0;
        for (long key : minUnion) {
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

    public void add(long value)
    {
        addHash(Murmur3.hash64(value));
        hll.add(value);
    }

    public void add(Slice value)
    {
        addHash(Murmur3.hash64(value));
        hll.add(value);
    }

    private void addHash(long hash)
    {
        short value = minhash.get(hash);
        if (value < Short.MAX_VALUE) {
            minhash.put(hash, (short) (value + 1));
        }
        while (minhash.size() > maxHashes) {
            minhash.remove(minhash.lastLongKey());
        }
    }

    public void mergeWith(SetDigest other)
    {
        hll.mergeWith(other.hll);
        LongBidirectionalIterator iterator = other.minhash.keySet().iterator();
        while (iterator.hasNext()) {
            long key = iterator.nextLong();
            int count = minhash.get(key) + other.minhash.get(key);
            minhash.put(key, Shorts.saturatedCast(count));
        }
        while (minhash.size() > maxHashes) {
            minhash.remove(minhash.lastLongKey());
        }
    }

    public Map<Long, Short> getHashCounts()
    {
        return ImmutableMap.copyOf(minhash);
    }
}
