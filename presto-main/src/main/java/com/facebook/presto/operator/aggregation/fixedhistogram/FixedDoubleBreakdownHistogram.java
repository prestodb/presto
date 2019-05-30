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

import com.google.common.collect.Streams;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class FixedDoubleBreakdownHistogram
        extends FixedHistogram
        implements Iterable<FixedDoubleBreakdownHistogram.Bucket>
{
    protected DoubleBreakdownMap breakdowns[];

    public static class Bucket
    {
        public final double left;
        public final double right;
        public final DoubleBreakdownMap breakdown;

        public Bucket(double left, double right, DoubleBreakdownMap breakdown)
        {
            this.left = left;
            this.right = right;
            this.breakdown = breakdown;
        }
    }

    public FixedDoubleBreakdownHistogram(int bucketCount, double min, double max)
    {
        super(bucketCount, min, max);
        breakdowns = new DoubleBreakdownMap[bucketCount];
        for (int i = 0; i < getBucketCount(); ++i) {
            breakdowns[i] = new DoubleBreakdownMap();
        }
    }

    public FixedDoubleBreakdownHistogram(SliceInput input)
    {
        super(input);
        breakdowns = new DoubleBreakdownMap[getBucketCount()];
        System.out.println("Read in " + super.getBucketCount() + " buckets");
        for (int i = 0; i < super.getBucketCount(); ++i) {
            System.out.println("Reading bucket " + i);
            breakdowns[i] = new DoubleBreakdownMap();
            final long numInBucket = input.readLong();
            for (int j = 0; j < numInBucket; ++j) {
                final double weight = input.readDouble();
                final long multiplicity = input.readLong();
                System.out.println("Read in " + weight + "," + multiplicity);
                breakdowns[i].add(weight, multiplicity);
            }
        }
    }

    public FixedDoubleBreakdownHistogram(FixedDoubleBreakdownHistogram other)
    {
        super(other.getBucketCount(), other.getMin(), other.getMax());
        breakdowns = new DoubleBreakdownMap[other.getBucketCount()];
        for (int i = 0; i < super.getBucketCount(); ++i) {
            breakdowns[i] = new DoubleBreakdownMap(other.breakdowns[i]) ;
        }        int size = super.getRequiredBytesForSerialization();

    }

    public int getRequiredBytesForSerialization()
    {
        int size = super.getRequiredBytesForSerialization();
        System.out.println("Bytes for " + size);
        for (int i = 0; i < super.getBucketCount(); ++i) {
            size += SizeOf.SIZE_OF_LONG;
            size += Streams.stream(breakdowns[i].iterator())
                    .mapToInt(e -> SizeOf.SIZE_OF_DOUBLE + SizeOf.SIZE_OF_LONG)
                    .sum();
            System.out.println("Bytes for bucket " + i + " " + size);
        }
        return size;
    }

    public void serialize(SliceOutput out)
    {
        super.serialize(out);
        for (int i = 0; i < super.getBucketCount(); ++i) {
            out.appendLong(breakdowns[i].getSize());
            System.out.println("ser " + i + ": " + breakdowns[i].getSize());
            Streams.stream(breakdowns[i].iterator())
                    .forEach(e -> {
                        System.out.println("ser " + e.weight + "; " + e.count);
                        out.appendDouble(e.weight);
                        out.appendLong(e.count);
                    });
        }
    }

    public long estimatedInMemorySize()
    {
        long size = super.estimatedInMemorySize();
        for (int i = 0; i < super.getBucketCount(); ++i) {
            size += SizeOf.SIZE_OF_LONG + breakdowns[i].size * (Long.SIZE + Double.SIZE);
        }
        return size;
    }

    public void add(double value)
    {
        add(value, 1.0);
    }

    public void add(double value, double weight)
    {
        System.out.println(this + " adding " + value + " -> " + weight + "; " + getIndexForValue(value));
        breakdowns[getIndexForValue(value)].add(weight, 1);
    }

    public void mergeWith(FixedDoubleBreakdownHistogram other)
    {
        System.out.println("Merging " + this + " -< " + other);
        super.mergeWith(other);
        for (int i = 0; i < super.getBucketCount(); ++i) {
            final int index = i;
            Streams.stream(other.breakdowns[i].iterator()).forEach(
                    e -> breakdowns[index].add(e.weight, e.count));
            }
    }

    @Override
    public Iterator<Bucket> iterator()
    {
        final int bucketCount = super.getBucketCount();
        final double min = super.getMin();
        final double max = super.getMax();

        Iterator<FixedHistogram.Bucket> baseIterator = super.getIterator();

        return new Iterator<Bucket>()
        {
            final Iterator<FixedHistogram.Bucket> iterator = baseIterator;

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public Bucket next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                final FixedHistogram.Bucket baseValue = iterator.next();

                return new Bucket(baseValue.left, baseValue.right, breakdowns[baseValue.index]);
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
        FixedDoubleBreakdownHistogram cloned = new FixedDoubleBreakdownHistogram(
                getBucketCount(),
                getMin(),
                getMax());
        for (int i = 0; i < getBucketCount(); ++i) {
            cloned.breakdowns[i] = new DoubleBreakdownMap(breakdowns[i]);
        }
        return cloned;
    }
}
