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

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import static com.facebook.presto.operator.aggregation.fixedhistogram.FixedHistogramUtils.getIndexForValue;
import static com.facebook.presto.operator.aggregation.fixedhistogram.FixedHistogramUtils.getLeftValueForIndex;
import static com.facebook.presto.operator.aggregation.fixedhistogram.FixedHistogramUtils.getRightValueForIndex;
import static com.facebook.presto.operator.aggregation.fixedhistogram.FixedHistogramUtils.validateParameters;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Fixed-bucket histogram of weights breakdowns. For each bucket, it stores the number of times
 * each weight was seen.
 *
 * @apiNote This class is built for cases where the cardinality of the different weights is small, and
 * the number of instances is very large. If the number of different weights added is not small, the time
 * and space complexities can be very large.
 */
public class FixedDoubleBreakdownHistogram
        implements Iterable<FixedDoubleBreakdownHistogram.Bucket>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FixedDoubleBreakdownHistogram.class).instanceSize();

    private final int bucketCount;
    private final double min;
    private final double max;

    private int[] indices;
    private double[] weights;
    private long[] counts;

    public static class Bucket
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

        public Bucket(double left, double right, double weight, long count)
        {
            this.left = left;
            this.right = right;
            this.weight = weight;
            this.count = count;
        }
    }

    public FixedDoubleBreakdownHistogram(int bucketCount, double min, double max)
    {
        validateParameters(bucketCount, min, max);
        this.bucketCount = bucketCount;
        this.min = min;
        this.max = max;
        this.indices = new int[0];
        this.weights = new double[0];
        this.counts = new long[0];
    }

    private FixedDoubleBreakdownHistogram(FixedDoubleBreakdownHistogram other)
    {
        this.bucketCount = other.bucketCount;
        this.min = other.min;
        this.max = other.max;
        this.indices = Arrays.copyOf(other.indices, other.indices.length);
        this.weights = Arrays.copyOf(other.weights, other.weights.length);
        this.counts = Arrays.copyOf(other.counts, other.counts.length);
    }

    private FixedDoubleBreakdownHistogram(
            int bucketCount,
            double min,
            double max,
            int[] indices,
            double[] weights,
            long[] counts)
    {
        validateParameters(bucketCount, min, max);
        this.bucketCount = bucketCount;
        this.min = min;
        this.max = max;
        this.indices = requireNonNull(indices, "indices is null");
        this.weights = requireNonNull(weights, "weights is null");
        this.counts = requireNonNull(counts, "counts is null");
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
        return INSTANCE_SIZE +
                SizeOf.sizeOf(indices) +
                SizeOf.sizeOf(weights) +
                SizeOf.sizeOf(counts);
    }

    public int getRequiredBytesForSerialization()
    {
        return SizeOf.SIZE_OF_INT + // bucketCount
                2 * SizeOf.SIZE_OF_DOUBLE + // min, max
                SizeOf.SIZE_OF_INT + // size
                indices.length *
                        (SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_DOUBLE + SizeOf.SIZE_OF_LONG); // indices, weights, counts
    }

    public static FixedDoubleBreakdownHistogram deserialize(SliceInput input)
    {
        int bucketCount = input.readInt();
        double min = input.readDouble();
        double max = input.readDouble();
        validateParameters(bucketCount, min, max);

        int size = input.readInt();
        int[] indices = new int[size];
        double[] weights = new double[size];
        long[] counts = new long[size];
        input.readBytes(Slices.wrappedIntArray(indices), size * SizeOf.SIZE_OF_INT);
        input.readBytes(Slices.wrappedDoubleArray(weights), size * SizeOf.SIZE_OF_DOUBLE);
        input.readBytes(Slices.wrappedLongArray(counts), size * SizeOf.SIZE_OF_LONG);
        return new FixedDoubleBreakdownHistogram(bucketCount, min, max, indices, weights, counts);
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
        checkArgument(
                bucketCount == other.bucketCount,
                format("bucket count must be equal to other bucket count: %s %s", bucketCount, other.bucketCount));
        checkArgument(
                min == other.min,
                format("minimum must be equal to other minimum: %s %s", min, other.min));
        checkArgument(
                max == other.max,
                format("Maximum must be equal to other maximum: %s %s", max, other.max));

        for (int i = 0; i < other.indices.length; i++) {
            add(other.indices[i], other.weights[i], other.counts[i]);
        }
    }

    public Iterator<Bucket> iterator()
    {
        return new Iterator<Bucket>() {
            private int currentIndex;

            @Override
            public boolean hasNext()
            {
                return currentIndex < indices.length;
            }

            @Override
            public Bucket next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                final Bucket bucket = new Bucket(
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
