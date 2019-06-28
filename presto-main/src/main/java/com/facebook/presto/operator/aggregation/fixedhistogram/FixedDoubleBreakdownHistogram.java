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
package com.facebook.presto.operator.aggregation.fixedhistogram;

import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import static com.facebook.presto.operator.aggregation.fixedhistogram.FixedHistogramUtils.getIndexForValue;
import static com.facebook.presto.operator.aggregation.fixedhistogram.FixedHistogramUtils.getLeftValueForIndex;
import static com.facebook.presto.operator.aggregation.fixedhistogram.FixedHistogramUtils.getRightValueForIndex;
import static com.facebook.presto.operator.aggregation.fixedhistogram.FixedHistogramUtils.verifyParameters;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Fixed-bucket histogram of weights breakdowns. For each bucket, it stores the number of times
 * each weight was seen.
 *
 * @apiNote This class is built for cases where the cardinality of the different weights is small, and
 * the number of instances is very large. If the number of different weights added is not small, the time
 * and space complexities can be very large.
 */
public class FixedDoubleBreakdownHistogram
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FixedDoubleBreakdownHistogram.class).instanceSize();

    private int bucketCount;
    private double min;
    private double max;

    private int[] indices;
    private double[] weights;
    private long[] counts;

    public static class BucketWeight
    {
        private final double left;
        private final double right;
        private final double weight;
        private final long count;

        public double getLeft()
        {
            return left;
        }

        public double getRight()
        {
            return right;
        }

        public double getWeight()
        {
            return weight;
        }

        public long getCount()
        {
            return count;
        }

        public BucketWeight(double left, double right, double weight, long count)
        {
            this.left = left;
            this.right = right;
            this.weight = weight;
            this.count = count;
        }
    }

    public FixedDoubleBreakdownHistogram(int bucketCount, double min, double max)
    {
        this.bucketCount = bucketCount;
        this.min = min;
        this.max = max;
        verifyParameters(bucketCount, min, max);
        this.indices = new int[0];
        this.weights = new double[0];
        this.counts = new long[0];
    }

    public FixedDoubleBreakdownHistogram(SliceInput input)
    {
        this.bucketCount = input.readInt();
        this.min = input.readDouble();
        this.max = input.readDouble();
        verifyParameters(bucketCount, min, max);
        int size = input.readInt();
        this.indices = new int[size];
        this.weights = new double[size];
        this.counts = new long[size];
        input.readBytes(
                Slices.wrappedIntArray(indices),
                size * SizeOf.SIZE_OF_INT);
        input.readBytes(
                Slices.wrappedDoubleArray(weights),
                size * SizeOf.SIZE_OF_DOUBLE);
        input.readBytes(
                Slices.wrappedLongArray(counts),
                size * SizeOf.SIZE_OF_LONG);
    }

    protected FixedDoubleBreakdownHistogram(FixedDoubleBreakdownHistogram other)
    {
        this.bucketCount = other.bucketCount;
        this.min = other.min;
        this.max = other.max;
        verifyParameters(bucketCount, min, max);
        this.indices = new int[other.indices.length];
        this.weights = new double[other.weights.length];
        this.counts = new long[other.counts.length];
        for (int i = 0; i < other.indices.length; ++i) {
            this.indices[i] = other.indices[i];
            this.weights[i] = other.weights[i];
            this.counts[i] = other.counts[i];
        }
    }

    public int getBucketCount()
    {
        return bucketCount;
    }

    public double getMin()
    {
        return min;
    }

    public double getMax()
    {
        return max;
    }

    public double getWidth()
    {
        return (max - min) / bucketCount;
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + indices.length * (SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_DOUBLE + SizeOf.SIZE_OF_LONG);
    }

    public int getRequiredBytesForSerialization()
    {
        return SizeOf.SIZE_OF_INT + // bucketCount
                // 2 * SizeOf.SIZE_OF_DOUBLE + // min, max
                SizeOf.SIZE_OF_INT + // size
                indices.length * (SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_DOUBLE + SizeOf.SIZE_OF_LONG); // arrays
    }

    public void serialize(SliceOutput out)
    {
        out.appendInt(bucketCount);
        out.appendDouble(min);
        out.appendDouble(max);
        out.appendInt(indices.length);
        IntStream.range(0, indices.length).forEach(i -> out.appendInt(indices[i]));
        IntStream.range(0, indices.length).forEach(i -> out.appendDouble(weights[i]));
        IntStream.range(0, indices.length).forEach(i -> out.appendLong(counts[i]));
    }

    public void add(double value)
    {
        add(value, 1.0);
    }

    public void add(double value, double weight)
    {
        add(value, weight, 1);
    }

    public void add(double value, double weight, long count)
    {
        int logicalBucketIndex = getIndexForValue(bucketCount, min, max, value);

        add(logicalBucketIndex, weight, count);
    }

    private void add(int logicalBucketIndex, double weight, long count)
    {
        int foundIndex = lowerBoundBinarySearch(logicalBucketIndex, weight);
        if (foundIndex < indices.length && indices[foundIndex] == logicalBucketIndex && weights[foundIndex] == weight) {
            counts[foundIndex] += count;
            return;
        }

        int[] newIndices = new int[indices.length + 1];
        System.arraycopy(indices, 0, newIndices, 0, foundIndex);
        newIndices[foundIndex] = logicalBucketIndex;
        System.arraycopy(indices, foundIndex, newIndices, foundIndex + 1, indices.length - foundIndex);
        indices = newIndices;

        double[] newWeights = new double[weights.length + 1];
        System.arraycopy(weights, 0, newWeights, 0, foundIndex);
        newWeights[foundIndex] = weight;
        System.arraycopy(weights, foundIndex, newWeights, foundIndex + 1, weights.length - foundIndex);
        weights = newWeights;

        long[] newCounts = new long[counts.length + 1];
        System.arraycopy(counts, 0, newCounts, 0, foundIndex);
        newCounts[foundIndex] = count;
        System.arraycopy(counts, foundIndex, newCounts, foundIndex + 1, counts.length - foundIndex);
        counts = newCounts;
    }

    public void mergeWith(FixedDoubleBreakdownHistogram other)
    {
        checkArgument(bucketCount == other.bucketCount, "bucket counts must be equal.");
        checkArgument(min == other.min, "minimums must be equal.");
        checkArgument(max == other.max, "maximums must be equal.");

        for (int i = 0; i < other.indices.length; ++i) {
            add(other.indices[i],
                    other.weights[i],
                    other.counts[i]);
        }
    }

    public Iterator<BucketWeight> iterator()
    {
        return new Iterator<BucketWeight>() {
            private int currentIndex;

            @Override
            public boolean hasNext()
            {
                return currentIndex < indices.length;
            }

            @Override
            public BucketWeight next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                final BucketWeight bucket = new BucketWeight(
                        getLeftValueForIndex(bucketCount, min, max, indices[currentIndex]),
                        getRightValueForIndex(bucketCount, min, max, indices[currentIndex]),
                        weights[currentIndex],
                        counts[currentIndex]);
                ++currentIndex;
                return bucket;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public FixedDoubleBreakdownHistogram clone()
    {
        return new FixedDoubleBreakdownHistogram(this);
    }

    private int lowerBoundBinarySearch(int logicalBucketIndex, double weight)
    {
        int count = indices.length;
        int first = 0;
        while (count > 0) {
            int step = count / 2;
            int index = first + step;
            if (indices[index] < logicalBucketIndex ||
                    (indices[index] == logicalBucketIndex && weights[index] < weight)) {
                first = index + 1;
                count -= step + 1;
            }
            else {
                count = step;
            }
        }
        return first;
    }
}
