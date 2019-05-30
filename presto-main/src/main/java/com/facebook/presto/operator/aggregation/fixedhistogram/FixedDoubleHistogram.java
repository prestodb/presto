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

import static com.google.common.base.Preconditions.checkArgument;

public class FixedDoubleHistogram
        implements Cloneable, Iterable<FixedDoubleHistogram.Bucket>
{
    private static final byte FORMAT_TAG = 0;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FixedHistogram.class).instanceSize();

    protected final double[] weights;

    private int bucketCount;
    private double min;
    private double max;

    public FixedDoubleHistogram(int bucketCount, double min, double max)
    {
        this.bucketCount = bucketCount;
        checkArgument(bucketCount >= 2, "bucketCount %s must be >= 2", bucketCount);
        this.min = min;
        this.max = max;
        checkArgument(min < max, "min %s must be greater than max %s ", min, max);
        this.weights = new double[bucketCount];
    }

    public FixedDoubleHistogram(SliceInput input)
    {
        checkArgument(input.readByte() == FORMAT_TAG, "Unsupported format tag");
        this.bucketCount = input.readInt();
        checkArgument(bucketCount >= 2, "bucketCount %s must be >= 2", bucketCount);
        this.min = input.readDouble();
        this.max = input.readDouble();
        checkArgument(min < max, "min %s must be greater than max %s ", min, max);
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

    protected int getIndexForValue(Double value)
    {
        checkArgument(
                value >= min && value <= max,
                "value must be within range [%s, %s]", min, max);
        return Math.min(
                (int) (bucketCount * (value - min) / (max - min)),
                bucketCount - 1);
    }

    protected Iterator<Bucket> getIterator()
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
                final Bucket value = new Bucket(
                        currentIndex * (max - min) / bucketCount,
                        (currentIndex + 1) * (max - min) / bucketCount,
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

    public static class Bucket
    {
        public final Double left;
        public final Double right;
        public final Double weight;

        public Bucket(Double left, Double right, Double weight)
        {
            this.left = left;
            this.right = right;
            this.weight = weight;
        }
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
        out.appendByte(FORMAT_TAG)
                .appendInt(bucketCount)
                .appendDouble(min)
                .appendDouble(max)
                .appendBytes(Slices.wrappedDoubleArray(weights, 0, bucketCount));
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + SizeOf.sizeOf(weights);
    }

    public void set(double value, double weight)
    {
        weights[getIndexForValue(value)] = weight;
    }

    public void add(double value)
    {
        add(value, 1.0);
    }

    public void add(double value, double weight)
    {
        weights[getIndexForValue(value)] += weight;
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

                final Bucket bucket = new Bucket(
                        currentIndex * (max - min) / bucketCount,
                        (currentIndex + 1) * (max - min) / bucketCount,
                        (currentIndex + 1) * (max - min) / bucketCount);
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

    public FixedDoubleHistogram clone()
    {
        return new FixedDoubleHistogram(this);
    }
}
