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

import static com.facebook.presto.operator.aggregation.fixedhistogram.FixedHistogramUtils.getIndexForValue;
import static com.facebook.presto.operator.aggregation.fixedhistogram.FixedHistogramUtils.getLeftValuForIndex;
import static com.facebook.presto.operator.aggregation.fixedhistogram.FixedHistogramUtils.getRightValuForIndex;
import static com.facebook.presto.operator.aggregation.fixedhistogram.FixedHistogramUtils.verifyParams;
import static com.google.common.base.Preconditions.checkArgument;

public class FixedDoubleHistogram
        implements Cloneable, Iterable<FixedDoubleHistogram.Bucket>
{
    public static class Bucket
    {
        private final double left;
        private final double right;
        private final double weight;

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

        public Bucket(double left, double right, double weight)
        {
            this.left = left;
            this.right = right;
            this.weight = weight;
        }
    }

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FixedDoubleHistogram.class).instanceSize();

    protected final double[] weights;

    private int bucketCount;
    private double min;
    private double max;

    public FixedDoubleHistogram(int bucketCount, double min, double max)
    {
        this.bucketCount = bucketCount;
        this.min = min;
        this.max = max;
        verifyParams(bucketCount, min, max);
        this.weights = new double[bucketCount];
    }

    public FixedDoubleHistogram(SliceInput input)
    {
        this.bucketCount = input.readInt();
        this.min = input.readDouble();
        this.max = input.readDouble();
        verifyParams(bucketCount, min, max);
        this.weights = new double[bucketCount];
        input.readBytes(
                Slices.wrappedDoubleArray(weights),
                bucketCount * SizeOf.SIZE_OF_DOUBLE);
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

    protected FixedDoubleHistogram(FixedDoubleHistogram other)
    {
        bucketCount = other.bucketCount;
        min = other.min;
        max = other.max;
        weights = new double[bucketCount];
        for (int i = 0; i < bucketCount; ++i) {
            weights[i] = other.weights[i];
        }
    }

    public int getRequiredBytesForSerialization()
    {
        return SizeOf.SIZE_OF_BYTE + // format
                SizeOf.SIZE_OF_INT + // num buckets
                SizeOf.SIZE_OF_DOUBLE + // min
                SizeOf.SIZE_OF_DOUBLE + // max
                SizeOf.SIZE_OF_DOUBLE * bucketCount; // weights
    }

    public void serialize(SliceOutput out)
    {
        out
                .appendInt(bucketCount)
                .appendDouble(min)
                .appendDouble(max)
                .appendBytes(Slices.wrappedDoubleArray(weights, 0, bucketCount));
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + getRequiredBytesForSerialization();
    }

    public void set(double value, double weight)
    {
        weights[getIndexForValue(bucketCount, min, max, value)] = weight;
    }

    public void add(double value)
    {
        add(value, 1.0);
    }

    public void add(double value, double weight)
    {
        weights[getIndexForValue(bucketCount, min, max, value)] += weight;
    }

    public void mergeWith(FixedDoubleHistogram other)
    {
        checkArgument(min == other.min, "min %s must be equal to other min %s", min, other.min);
        checkArgument(max == other.max, "max %s must be equal to other max %s", max, other.max);
        checkArgument(
                bucketCount == other.bucketCount,
                "bucketCount %s must be equal to other bucketCount %s", bucketCount, other.bucketCount);
        for (int i = 0; i < bucketCount; ++i) {
            weights[i] += other.weights[i];
        }
    }

    @Override
    public Iterator<Bucket> iterator()
    {
        final int bucketCount = getBucketCount();
        final double min = getMin();
        final double max = getMax();

        return new Iterator<Bucket>()
        {
            private int currentIndex;

            @Override
            public boolean hasNext()
            {
                return currentIndex < bucketCount;
            }

            @Override
            public Bucket next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                final Bucket value = new Bucket(
                        getLeftValuForIndex(bucketCount, min, max, currentIndex),
                        getRightValuForIndex(bucketCount, min, max, currentIndex),
                        weights[currentIndex]);

                ++currentIndex;
                return value;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public FixedDoubleHistogram clone()
    {
        return new FixedDoubleHistogram(this);
    }
}
