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

import io.airlift.slice.SliceInput;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class DoubleBreakdownMap
        implements Iterable<DoubleBreakdownMap.Entry>
{
    protected double[] weights;
    protected long[] counts;
    int nextIndex;
    int size;

    public static class Entry
    {
        public final double weight;
        public final long count;

        public Entry(double weight, long count)
        {
            this.weight = weight;
            this.count = count;
        }
    }

    protected DoubleBreakdownMap()
    {
        nextIndex = 0;
        size = 1;
        weights = new double[size];
        counts = new long[size];
    }

    protected DoubleBreakdownMap(SliceInput input)
    {
        nextIndex = input.readInt();
        size = 2 * nextIndex;
        weights = new double[size];
        counts = new long[size];
        for (int i = 0; i < nextIndex; ++i) {
            weights[i] = input.readDouble();
            counts[i] = input.readLong();
        }
    }

    protected DoubleBreakdownMap(DoubleBreakdownMap other)
    {
        nextIndex = other.nextIndex;
        size = other.size;
        weights = new double[size];
        counts = new long[size];
        for (int i = 0; i < nextIndex; ++i) {
            weights[i] = other.weights[i];
            counts[i] = other.counts[i];
        }
    }

    int getSize()
    {
        return this.nextIndex;
    }

    public void add(double weight, long count)
    {
        for (int i = 0; i < nextIndex; ++i) {
            if (weights[i] == weight) {
                counts[i] += count;
                System.out.println(this + " Found " + weight);
                return;
            }
        }
        System.out.println(this + " inserting " + weight);
        ++nextIndex;
        if (nextIndex == size) {
            double weights[] = new double[2 * size];
            long counts[] = new long[2 * size];
            for (int i = 0; i < nextIndex; ++i) {
                weights[i] = this.weights[i];
                counts[i] = this.counts[i];
            }
            this.size *= 2;
            this.weights = weights;
            this.counts = counts;
        }
        weights[nextIndex] = weight;
        counts[nextIndex] = count;
    }

    public Iterator<Entry> iterator()
    {
        return new Iterator<Entry>()
        {
            private int index;

            @Override
            public boolean hasNext()
            {
                return index < nextIndex;
            }

            @Override
            public Entry next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                final Entry entry = new Entry(weights[index], counts[index]);
                ++index;
                return entry;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
