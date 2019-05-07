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
package com.facebook.presto.operator.aggregation;

import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkArgument;

public class FixedDoubleHistogram
        implements Cloneable, Iterable<FixedDoubleHistogram.Bin>
{
    private static final byte FORMAT_TAG = 0;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FixedDoubleHistogram.class).instanceSize();

    private final int bucketCount;
    private final double min;
    private final double max;
    private final double[] weights;

    public class Bin
    {
        public final double left;
        public final double right;
        public final double weight;

        public Bin(double left, double right, double weight)
        {
            this.left = left;
            this.right = right;
            this.weight = weight;
        }
    }

    public FixedDoubleHistogram(int bucketCount, double min, double max)
    {
        checkArgument(bucketCount >= 2, "bucketCount %s must be >= 2", bucketCount);
        checkArgument(min < max, "min %s must be greater than max %s ", min, max);

        this.bucketCount = bucketCount;
        this.min = min;
        this.max = max;
        this.weights = new double[bucketCount];
    }

    public FixedDoubleHistogram(SliceInput input)
    {
        checkArgument(input.readByte() == FORMAT_TAG, "Unsupported format tag");
        bucketCount = input.readInt();
        checkArgument(bucketCount >= 2, "bucketCount %s must be >= 2", bucketCount);
        min = input.readDouble();
        max = input.readDouble();
        checkArgument(min < max, "min %s must be greater than max %s ", min, max);
        weights = new double[bucketCount];
        input.readBytes(Slices.wrappedDoubleArray(weights), bucketCount * SizeOf.SIZE_OF_DOUBLE);
    }

    protected FixedDoubleHistogram(FixedDoubleHistogram other)
    {
        this(other.bucketCount, other.min, other.max);
        for (int i = 0; i < bucketCount; ++i) {
            weights[i] = other.weights[i];
        }
    }

    public FixedDoubleHistogram clone()
    {
        return new FixedDoubleHistogram(this);
    }

    int getBucketCount()
    {
        return bucketCount;
    }

    double getMin()
    {
        return min;
    }

    double getMax()
    {
        return max;
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
        checkArgument(
                value >= min && value <= max,
                "value must be within range [%s, %s]", min, max);
        weights[getIndexForValue(value)] = weight;
    }

    public void add(double value)
    {
        add(value, 1);
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
    public Iterator<Bin> iterator()
    {
        return new Iterator<Bin>()
        {
            private int currentIndex;

            @Override
            public boolean hasNext()
            {
                return currentIndex < bucketCount;
            }

            @Override
            public Bin next()
            {
                if (currentIndex == bucketCount) {
                    throw new NoSuchElementException();
                }

                return new Bin(
                    currentIndex * (max - min) / bucketCount,
                    (currentIndex + 1) * (max - min) / bucketCount,
                    weights[currentIndex++]);
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private int getIndexForValue(double value)
    {
        checkArgument(
                value >= min && value <= max,
                "value must be within range [%s, %s]", min, max);
        return Math.min(
                (int) (bucketCount * (value - min) / (max - min)),
                bucketCount - 1);
    }
}
