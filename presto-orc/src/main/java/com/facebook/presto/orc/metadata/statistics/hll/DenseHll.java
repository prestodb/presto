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
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.facebook.presto.orc.metadata.statistics.hll.Utils.alpha;
import static com.facebook.presto.orc.metadata.statistics.hll.Utils.computeIndex;
import static com.facebook.presto.orc.metadata.statistics.hll.Utils.computeValue;
import static com.facebook.presto.orc.metadata.statistics.hll.Utils.linearCounting;
import static com.facebook.presto.orc.metadata.statistics.hll.Utils.numberOfBuckets;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

final class DenseHll
        implements HllInstance
{
    private static final double LINEAR_COUNTING_MIN_EMPTY_BUCKETS = 0.4;

    private static final int BITS_PER_BUCKET = 4;
    private static final int MAX_DELTA = (1 << BITS_PER_BUCKET) - 1;
    private static final int BUCKET_MASK = (1 << BITS_PER_BUCKET) - 1;

    private static final int DENSE_INSTANCE_SIZE = ClassLayout.parseClass(DenseHll.class).instanceSize();
    private static final int OVERFLOW_GROW_INCREMENT = 5;

    private final byte indexBitLength;
    private byte baseline;
    private int baselineCount;
    private final byte[] deltas;

    private int overflows;
    private int[] overflowBuckets;
    private byte[] overflowValues;

    public DenseHll(int indexBitLength)
    {
        validatePrefixLength(indexBitLength);

        int numberOfBuckets = numberOfBuckets(indexBitLength);

        this.indexBitLength = (byte) indexBitLength;
        baselineCount = numberOfBuckets;
        deltas = new byte[numberOfBuckets * BITS_PER_BUCKET / Byte.SIZE];
        overflowBuckets = new int[0];
        overflowValues = new byte[0];
    }

    public DenseHll(Slice serialized)
    {
        BasicSliceInput input = serialized.getInput();

        byte formatTag = input.readByte();
        checkArgument(formatTag == Format.DENSE_V1.getTag() || formatTag == Format.DENSE_V2.getTag(), "Invalid format tag");

        indexBitLength = input.readByte();
        validatePrefixLength(indexBitLength);
        int numberOfBuckets = numberOfBuckets(indexBitLength);

        baseline = input.readByte();
        deltas = new byte[numberOfBuckets / 2];
        input.readBytes(deltas);

        if (formatTag == Format.DENSE_V1.getTag()) {
            // for backward compatibility
            int bucket = input.readShort();
            byte value = input.readByte();
            if (bucket >= 0 && value > 0) {
                checkArgument(bucket <= numberOfBuckets, "Overflow bucket index is out of range");
                overflows = 1;
                overflowBuckets = new int[] {bucket};
                overflowValues = new byte[] {value};
            }
            else {
                overflows = 0;
                overflowBuckets = new int[0];
                overflowValues = new byte[0];
            }
        }
        else if (formatTag == Format.DENSE_V2.getTag()) {
            overflows = input.readUnsignedShort();
            checkArgument(overflows <= numberOfBuckets, "Overflow entries is greater than actual number of buckets (possibly corrupt input)");

            overflowBuckets = new int[overflows];
            overflowValues = new byte[overflows];

            for (int i = 0; i < overflows; i++) {
                overflowBuckets[i] = input.readUnsignedShort();
                checkArgument(overflowBuckets[i] <= numberOfBuckets, "Overflow bucket index is out of range");
            }

            for (int i = 0; i < overflows; i++) {
                overflowValues[i] = input.readByte();
                checkArgument(overflowValues[i] > 0, "Overflow bucket value must be > 0");
            }
        }
        else {
            throw new IllegalArgumentException(String.format("Invalid format tag: %d", formatTag));
        }

        baselineCount = 0;
        for (int i = 0; i < numberOfBuckets; i++) {
            if (getDelta(i) == 0) {
                baselineCount++;
            }
        }

        checkArgument(!input.isReadable(), "input is too big");
    }

    public static boolean canDeserialize(Slice serialized)
    {
        byte formatTag = serialized.getByte(0);
        return formatTag == Format.DENSE_V1.getTag() || formatTag == Format.DENSE_V2.getTag();
    }

    public void insertHash(long hash)
    {
        int index = computeIndex(hash, indexBitLength);
        int value = computeValue(hash, indexBitLength);

        insert(index, value);
    }

    @Override
    public int estimatedInMemorySize()
    {
        return (int) (DENSE_INSTANCE_SIZE +
                SizeOf.sizeOf(deltas) +
                SizeOf.sizeOf(overflowBuckets) +
                SizeOf.sizeOf(overflowValues));
    }

    @Override
    public int getIndexBitLength()
    {
        return indexBitLength;
    }

    @Override
    public long cardinality()
    {
        int numberOfBuckets = numberOfBuckets(indexBitLength);

        // if baseline is zero, then baselineCount is the number of buckets with value 0
        if ((baseline == 0) && (baselineCount > (LINEAR_COUNTING_MIN_EMPTY_BUCKETS * numberOfBuckets))) {
            return Math.round(linearCounting(baselineCount, numberOfBuckets));
        }

        double sum = 0;
        for (int i = 0; i < numberOfBuckets; i++) {
            int value = getValue(i);
            sum += 1.0 / (1L << value);
        }

        double estimate = (alpha(indexBitLength) * numberOfBuckets * numberOfBuckets) / sum;
        estimate = correctBias(estimate);

        return Math.round(estimate);
    }

    private double correctBias(double rawEstimate)
    {
        double[] estimates = BiasCorrection.RAW_ESTIMATES[indexBitLength - 4];
        if (rawEstimate < estimates[0] || rawEstimate > estimates[estimates.length - 1]) {
            return rawEstimate;
        }

        double[] biases = BiasCorrection.BIAS[indexBitLength - 4];

        int position = search(rawEstimate, estimates);

        double bias;
        if (position >= 0) {
            bias = biases[position];
        }
        else {
            // interpolate
            int insertionPoint = -(position + 1);

            double x0 = estimates[insertionPoint - 1];
            double y0 = biases[insertionPoint - 1];
            double x1 = estimates[insertionPoint];
            double y1 = biases[insertionPoint];

            bias = ((((rawEstimate - x0) * (y1 - y0)) / (x1 - x0)) + y0);
        }

        return rawEstimate - bias;
    }

    private int search(double rawEstimate, double[] estimateCurve)
    {
        int low = 0;
        int high = estimateCurve.length - 1;

        while (low <= high) {
            int middle = (low + high) >>> 1;

            double middleValue = estimateCurve[middle];

            if (rawEstimate > middleValue) {
                low = middle + 1;
            }
            else if (rawEstimate < middleValue) {
                high = middle - 1;
            }
            else {
                return middle;
            }
        }

        return -(low + 1);
    }

    public void insert(int bucket, int value)
    {
        int delta = value - baseline;
        final int oldDelta = getDelta(bucket);

        if (delta <= oldDelta || (oldDelta == MAX_DELTA && (delta <= oldDelta + getOverflow(bucket)))) {
            // the old bucket value is (baseline + oldDelta) + possibly an overflow, so it's guaranteed to be >= the new value
            return;
        }

        if (delta > MAX_DELTA) {
            int overflow = delta - MAX_DELTA;

            if (!setOverflow(bucket, overflow)) {
                // grow overflows arrays if necessary
                overflowBuckets = Ints.ensureCapacity(overflowBuckets, overflows + 1, OVERFLOW_GROW_INCREMENT);
                overflowValues = Bytes.ensureCapacity(overflowValues, overflows + 1, OVERFLOW_GROW_INCREMENT);

                overflowBuckets[overflows] = bucket;
                overflowValues[overflows] = (byte) overflow;
                overflows++;
            }

            delta = MAX_DELTA;
        }

        setDelta(bucket, delta);

        if (oldDelta == 0) {
            --baselineCount;
            adjustBaselineIfNeeded();
        }
    }

    private int getOverflow(int bucket)
    {
        for (int i = 0; i < overflows; i++) {
            if (overflowBuckets[i] == bucket) {
                return overflowValues[i];
            }
        }
        return 0;
    }

    /**
     * Returns false if not overflow bucket matching the given bucket id was found
     */
    private boolean setOverflow(int bucket, int overflow)
    {
        for (int i = 0; i < overflows; i++) {
            if (overflowBuckets[i] == bucket) {
                overflowValues[i] = (byte) overflow;
                return true;
            }
        }
        return false;
    }

    public Slice serialize()
    {
        int size = estimatedSerializedSize();

        DynamicSliceOutput output = new DynamicSliceOutput(size)
                .appendByte(Format.DENSE_V2.getTag())
                .appendByte(indexBitLength)
                .appendByte(baseline)
                .appendBytes(deltas)
                .appendShort(overflows);

        // sort overflow arrays to get consistent serialization for equivalent HLLs
        sortOverflows();

        for (int i = 0; i < overflows; i++) {
            output.appendShort(overflowBuckets[i]);
        }
        for (int i = 0; i < overflows; i++) {
            output.appendByte(overflowValues[i]);
        }

        return output.slice();
    }

    private void sortOverflows()
    {
        // traditional insertion sort (ok for small arrays)
        for (int i = 1; i < overflows; i++) {
            for (int j = i; j > 0 && overflowBuckets[j - 1] > overflowBuckets[j]; j--) {
                int bucket = overflowBuckets[j];
                int value = overflowValues[j];

                overflowBuckets[j] = overflowBuckets[j - 1];
                overflowValues[j] = overflowValues[j - 1];

                overflowBuckets[j - 1] = bucket;
                overflowValues[j - 1] = (byte) value;
            }
        }
    }

    @Override
    public DenseHll toDense()
    {
        return this;
    }

    public int estimatedSerializedSize()
    {
        return SizeOf.SIZE_OF_BYTE + // type + version
                SizeOf.SIZE_OF_BYTE + // p
                SizeOf.SIZE_OF_BYTE + // baseline
                (numberOfBuckets(indexBitLength) * SizeOf.SIZE_OF_BYTE) / 2 + // buckets
                SizeOf.SIZE_OF_SHORT + // overflow bucket count
                SizeOf.SIZE_OF_SHORT * overflows + // overflow bucket indexes
                SizeOf.SIZE_OF_BYTE * overflows; // overflow bucket values
    }

    @SuppressWarnings("NarrowingCompoundAssignment")
    private void setDelta(int bucket, int value)
    {
        int slot = bucketToSlot(bucket);

        // clear the old value
        byte clearMask = (byte) (BUCKET_MASK << shiftForBucket(bucket));
        deltas[slot] &= ~clearMask;

        // set the new value
        byte setMask = (byte) (value << shiftForBucket(bucket));
        deltas[slot] |= setMask;
    }

    private int getDelta(int bucket)
    {
        int slot = bucketToSlot(bucket);

        return (deltas[slot] >> shiftForBucket(bucket)) & BUCKET_MASK;
    }

    @VisibleForTesting
    int getValue(int bucket)
    {
        int delta = getDelta(bucket);

        if (delta == MAX_DELTA) {
            delta += getOverflow(bucket);
        }

        return baseline + delta;
    }

    private void adjustBaselineIfNeeded()
    {
        while (baselineCount == 0) {
            baseline++;

            for (int bucket = 0; bucket < numberOfBuckets(indexBitLength); ++bucket) {
                int delta = getDelta(bucket);

                boolean hasOverflow = false;
                if (delta == MAX_DELTA) {
                    // scan overflows
                    for (int i = 0; i < overflows; i++) {
                        if (overflowBuckets[i] == bucket) {
                            hasOverflow = true;
                            overflowValues[i]--;

                            if (overflowValues[i] == 0) {
                                int lastEntry = overflows - 1;
                                if (i < lastEntry) {
                                    // remove the entry by moving the last entry to this position
                                    overflowBuckets[i] = overflowBuckets[lastEntry];
                                    overflowValues[i] = overflowValues[lastEntry];

                                    // clean up to make it easier to catch bugs
                                    overflowBuckets[lastEntry] = -1;
                                    overflowValues[lastEntry] = 0;
                                }
                                overflows--;
                            }
                            break;
                        }
                    }
                }

                if (!hasOverflow) {
                    // getDelta is guaranteed to return a value greater than zero
                    // because baselineCount is zero (i.e., number of deltas with zero value)
                    // So it's safe to decrement here
                    delta--;
                    setDelta(bucket, delta);
                }

                if (delta == 0) {
                    ++baselineCount;
                }
            }
        }
    }

    /**
     * Returns "this" for chaining
     */
    public DenseHll mergeWith(DenseHll other)
    {
        if (indexBitLength != other.indexBitLength) {
            throw new IllegalArgumentException(String.format(
                    "Cannot merge HLLs with different number of buckets: %s vs %s",
                    numberOfBuckets(indexBitLength),
                    numberOfBuckets(other.indexBitLength)));
        }

        int baseline = Math.max(this.baseline, other.baseline);
        int baselineCount = 0;

        int overflows = 0;
        int[] overflowBuckets = new int[OVERFLOW_GROW_INCREMENT];
        byte[] overflowValues = new byte[OVERFLOW_GROW_INCREMENT];

        int numberOfBuckets = numberOfBuckets(indexBitLength);
        for (int i = 0; i < numberOfBuckets; i++) {
            int value = Math.max(getValue(i), other.getValue(i));

            int delta = value - baseline;
            if (delta == 0) {
                baselineCount++;
            }
            else if (delta > MAX_DELTA) {
                // grow overflows arrays if necessary
                overflowBuckets = Ints.ensureCapacity(overflowBuckets, overflows + 1, OVERFLOW_GROW_INCREMENT);
                overflowValues = Bytes.ensureCapacity(overflowValues, overflows + 1, OVERFLOW_GROW_INCREMENT);

                overflowBuckets[overflows] = i;
                overflowValues[overflows] = (byte) (delta - MAX_DELTA);

                overflows++;

                delta = MAX_DELTA;
            }

            setDelta(i, delta);
        }

        this.baseline = (byte) baseline;
        this.baselineCount = baselineCount;
        this.overflows = overflows;
        this.overflowBuckets = overflowBuckets;
        this.overflowValues = overflowValues;

        // all baseline values in one of the HLLs lost to the values
        // in the other HLL, so we need to adjust the final baseline
        adjustBaselineIfNeeded();

        return this;
    }

    public static int estimatedInMemorySize(int indexBitLength)
    {
        // note: we don't take into account overflow entries since their number can vary
        return (int) (DENSE_INSTANCE_SIZE + SizeOf.sizeOfByteArray(numberOfBuckets(indexBitLength) / 2));
    }

    private static int bucketToSlot(int bucket)
    {
        return bucket >> 1;
    }

    private static int shiftForBucket(int bucket)
    {
        // ((1 - bucket) % 2) * BITS_PER_BUCKET
        return ((~bucket) & 1) << 2;
    }

    private static void validatePrefixLength(int indexBitLength)
    {
        checkArgument(indexBitLength >= 1 && indexBitLength <= 16, "indexBitLength is out of range");
    }

    @Override
    public void verify()
    {
        int zeroDeltas = 0;
        for (int i = 0; i < numberOfBuckets(indexBitLength); i++) {
            if (getDelta(i) == 0) {
                zeroDeltas++;
            }
        }

        checkState(zeroDeltas == baselineCount, "baselineCount (%s) doesn't match number of zero deltas (%s)",
                baselineCount, zeroDeltas);

        Set<Integer> overflows = new HashSet<>();
        for (int i = 0; i < this.overflows; i++) {
            int bucket = overflowBuckets[i];
            overflows.add(bucket);

            checkState(overflowValues[i] > 0, "Overflow at %s for bucket %s is 0", i, bucket);
            checkState(getDelta(bucket) == MAX_DELTA,
                    "delta in bucket %s is less than MAX_DELTA (%s < %s) even though there's an associated overflow entry",
                    bucket, getDelta(bucket), MAX_DELTA);
        }

        checkState(overflows.size() == this.overflows, "Duplicate overflow buckets: %s",
                Ints.asList(Arrays.copyOf(overflowBuckets, this.overflows)));
    }
}
