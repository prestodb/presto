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
import org.openjdk.jol.info.ClassLayout;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;

public class FixedHistogram
{
    private static final byte FORMAT_TAG = 0;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FixedHistogram.class).instanceSize();

    private int bucketCount;
    private double min;
    private double max;

    public class Bucket
    {
        public final int index;
        public final Double left;
        public final Double right;

        public Bucket(int index, Double left, Double right)
        {
            this.index = index;
            this.left = left;
            this.right = right;
        }
    }

    protected FixedHistogram(int bucketCount, double min, double max)
    {
        checkArgument(bucketCount >= 2, "bucketCount %s must be >= 2", bucketCount);
        checkArgument(min < max, "min %s must be greater than max %s ", min, max);

        this.bucketCount = bucketCount;
        this.min = min;
        this.max = max;
    }

    protected FixedHistogram(SliceInput input)
    {
        checkArgument(input.readByte() == FORMAT_TAG, "Unsupported format tag");
        bucketCount = input.readInt();
        checkArgument(bucketCount >= 2, "bucketCount %s must be >= 2", bucketCount);
        min = input.readDouble();
        max = input.readDouble();
        checkArgument(min < max, "min %s must be greater than max %s ", min, max);
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

    public void mergeWith(FixedHistogram other)
    {
        checkArgument(min == other.min, "min %s must be equal to other min %s", min, other.min);
        checkArgument(max == other.max, "max %s must be equal to other max %s", max, other.max);
        checkArgument(
                bucketCount == other.bucketCount,
                "bucketCount %s must be equal to other bucketCount %s", bucketCount, other.bucketCount);
    }

    protected int getRequiredBytesForSerialization()
    {
        return SizeOf.SIZE_OF_BYTE + // format
                SizeOf.SIZE_OF_INT + // num buckets
                SizeOf.SIZE_OF_DOUBLE + // min
                SizeOf.SIZE_OF_DOUBLE; // max
    }

    protected void serialize(SliceOutput out)
    {
        out.appendByte(FORMAT_TAG)
                .appendInt(bucketCount)
                .appendDouble(min)
                .appendDouble(max);
    }

    protected long estimatedInMemorySize()
    {
        return INSTANCE_SIZE;
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
                        currentIndex,
                        currentIndex * (max - min) / bucketCount,
                        (currentIndex + 1) * (max - min) / bucketCount);
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
}
