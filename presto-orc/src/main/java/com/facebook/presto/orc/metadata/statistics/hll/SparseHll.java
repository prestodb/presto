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
package com.facebook.presto.orc.metadata.statistics.hll;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static com.facebook.presto.orc.metadata.statistics.hll.Utils.computeIndex;
import static com.facebook.presto.orc.metadata.statistics.hll.Utils.linearCounting;
import static com.facebook.presto.orc.metadata.statistics.hll.Utils.numberOfBuckets;
import static com.facebook.presto.orc.metadata.statistics.hll.Utils.numberOfLeadingZeros;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;
import static java.util.Comparator.comparingInt;

final class SparseHll
        implements HllInstance
{
    private static final int SPARSE_INSTANCE_SIZE = ClassLayout.parseClass(SparseHll.class).instanceSize();

    // 6 bits to encode the number of zeros after the truncated hash
    // and be able to fit the encoded value in an integer
    private static final int VALUE_BITS = 6;
    private static final int VALUE_MASK = (1 << VALUE_BITS) - 1;
    private static final int EXTENDED_PREFIX_BITS = Integer.SIZE - VALUE_BITS;

    private final byte indexBitLength;
    private short numberOfEntries;
    private int[] entries;

    public SparseHll(int indexBitLength)
    {
        validatePrefixLength(indexBitLength);

        this.indexBitLength = (byte) indexBitLength;
        entries = new int[1];
    }

    public SparseHll(Slice serialized)
    {
        BasicSliceInput input = serialized.getInput();

        checkArgument(input.readByte() == Format.SPARSE_V2.getTag(), "invalid format tag");

        indexBitLength = input.readByte();
        validatePrefixLength(indexBitLength);

        numberOfEntries = input.readShort();

        entries = new int[numberOfEntries];
        for (int i = 0; i < numberOfEntries; i++) {
            entries[i] = input.readInt();
        }

        checkArgument(!input.isReadable(), "input is too big");
    }

    public static boolean canDeserialize(Slice serialized)
    {
        return serialized.getByte(0) == Format.SPARSE_V2.getTag();
    }

    public void insertHash(long hash)
    {
        // TODO: investigate whether accumulate, sort and merge results in better performance due to avoiding the shift+insert in every call

        int bucket = Utils.computeIndex(hash, EXTENDED_PREFIX_BITS);
        int position = searchBucket(bucket);

        // add entry if missing
        if (position < 0) {
            // ensure capacity
            if (numberOfEntries + 1 > entries.length) {
                entries = Arrays.copyOf(entries, entries.length + 10);
            }

            // shift right
            int insertionPoint = -(position + 1);
            if (insertionPoint < numberOfEntries) {
                System.arraycopy(entries, insertionPoint, entries, insertionPoint + 1, numberOfEntries - insertionPoint);
            }

            entries[insertionPoint] = encode(hash);
            numberOfEntries++;
        }
        else {
            int currentEntry = entries[position];
            int newValue = Utils.numberOfLeadingZeros(hash, EXTENDED_PREFIX_BITS);

            if (decodeBucketValue(currentEntry) < newValue) {
                entries[position] = encode(bucket, newValue);
            }
        }
    }

    private int encode(long hash)
    {
        return encode(computeIndex(hash, EXTENDED_PREFIX_BITS), numberOfLeadingZeros(hash, EXTENDED_PREFIX_BITS));
    }

    private static int encode(int bucketIndex, int value)
    {
        return (bucketIndex << VALUE_BITS) | value;
    }

    private static int decodeBucketIndex(int entry)
    {
        return decodeBucketIndex(EXTENDED_PREFIX_BITS, entry);
    }

    private static int decodeBucketValue(int entry)
    {
        return entry & VALUE_MASK;
    }

    private static int decodeBucketIndex(int indexBitLength, int entry)
    {
        return entry >>> (Integer.SIZE - indexBitLength);
    }

    public void mergeWith(SparseHll other)
    {
        entries = mergeEntries(other);
        numberOfEntries = (short) entries.length;
    }

    public DenseHll toDense()
    {
        DenseHll result = new DenseHll(indexBitLength);

        for (int i = 0; i < numberOfEntries; i++) {
            int entry = entries[i];

            // The leading EXTENDED_BITS_LENGTH are a proper subset of the original hash.
            // Since we're guaranteed that indexBitLength is <= EXTENDED_BITS_LENGTH,
            // the value stored in those bits corresponds to the bucket index in the dense HLL
            int bucket = decodeBucketIndex(indexBitLength, entry);

            // compute the number of zeros between indexBitLength and EXTENDED_BITS_LENGTH
            int zeros = Integer.numberOfLeadingZeros(entry << indexBitLength);

            // if zeros > EXTENDED_BITS_LENGTH - indexBits, it means all those bits were zeros,
            // so look at the entry value, which contains the number of leading 0 *after* EXTENDED_BITS_LENGTH
            int bits = EXTENDED_PREFIX_BITS - indexBitLength;
            if (zeros > bits) {
                zeros = bits + decodeBucketValue(entry);
            }

            result.insert(bucket, zeros + 1); // + 1 because HLL stores leading number of zeros + 1
        }

        return result;
    }

    @Override
    public long cardinality()
    {
        // Estimate the cardinality using linear counting over the theoretical 2^EXTENDED_BITS_LENGTH buckets available due
        // to the fact that we're recording the raw leading EXTENDED_BITS_LENGTH of the hash. This produces much better precision
        // while in the sparse regime.
        int totalBuckets = numberOfBuckets(EXTENDED_PREFIX_BITS);
        int zeroBuckets = totalBuckets - numberOfEntries;

        return Math.round(linearCounting(zeroBuckets, totalBuckets));
    }

    @Override
    public int estimatedInMemorySize()
    {
        return SPARSE_INSTANCE_SIZE + toIntExact(sizeOf(entries));
    }

    @Override
    public int getIndexBitLength()
    {
        return indexBitLength;
    }

    /**
     * Returns a index of the entry if found. Otherwise, it returns -(insertionPoint + 1)
     */
    private int searchBucket(int bucketIndex)
    {
        int low = 0;
        int high = numberOfEntries - 1;

        while (low <= high) {
            int middle = (low + high) >>> 1;

            int middleBucketIndex = decodeBucketIndex(entries[middle]);

            if (bucketIndex > middleBucketIndex) {
                low = middle + 1;
            }
            else if (bucketIndex < middleBucketIndex) {
                high = middle - 1;
            }
            else {
                return middle;
            }
        }

        return -(low + 1); // not found... return insertion point
    }

    private int[] mergeEntries(SparseHll other)
    {
        int[] result = new int[numberOfEntries + other.numberOfEntries];
        int leftIndex = 0;
        int rightIndex = 0;

        int index = 0;
        while (leftIndex < numberOfEntries && rightIndex < other.numberOfEntries) {
            int left = decodeBucketIndex(entries[leftIndex]);
            int right = decodeBucketIndex(other.entries[rightIndex]);

            if (left < right) {
                result[index++] = entries[leftIndex++];
            }
            else if (left > right) {
                result[index++] = other.entries[rightIndex++];
            }
            else {
                int value = Math.max(decodeBucketValue(entries[leftIndex]), decodeBucketValue(other.entries[rightIndex]));
                result[index++] = encode(left, value);
                leftIndex++;
                rightIndex++;
            }
        }

        while (leftIndex < numberOfEntries) {
            result[index++] = entries[leftIndex++];
        }

        while (rightIndex < other.numberOfEntries) {
            result[index++] = other.entries[rightIndex++];
        }

        return Arrays.copyOf(result, index);
    }

    public Slice serialize()
    {
        int size = SizeOf.SIZE_OF_BYTE + // format tag
                SizeOf.SIZE_OF_BYTE + // p
                SizeOf.SIZE_OF_SHORT + // number of entries
                SizeOf.SIZE_OF_INT * numberOfEntries;

        DynamicSliceOutput out = new DynamicSliceOutput(size)
                .appendByte(Format.SPARSE_V2.getTag())
                .appendByte(indexBitLength)
                .appendShort(numberOfEntries);

        for (int i = 0; i < numberOfEntries; i++) {
            out.appendInt(entries[i]);
        }

        return out.slice();
    }

    @Override
    public int estimatedSerializedSize()
    {
        return SizeOf.SIZE_OF_SHORT // type + version
                + SizeOf.SIZE_OF_BYTE  // p
                + SizeOf.SIZE_OF_SHORT // numberOfEntries
                + SizeOf.SIZE_OF_INT * numberOfEntries; // entries
    }

    private static void validatePrefixLength(int indexBitLength)
    {
        checkArgument(indexBitLength >= 1 && indexBitLength <= EXTENDED_PREFIX_BITS, "indexBitLength is out of range");
    }

    @VisibleForTesting
    public void verify()
    {
        checkState(numberOfEntries <= entries.length,
                "Expected number of hashes (%s) larger than array length (%s)",
                numberOfEntries, entries.length);

        checkState(Ordering.from(comparingInt(e -> decodeBucketIndex((Integer) e)))
                        .isOrdered(Ints.asList(Arrays.copyOf(entries, numberOfEntries))),
                "entries are not sorted");
    }
}
