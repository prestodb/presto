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
package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.spi.block.BlockBuilder;
import com.google.common.collect.Streams;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.concurrent.ThreadLocalRandom;

/*
Weighted reservoir samples.
 */
public class WeightedReservoirSample
        extends AbstractReservoirSample
{
    private static class Entry
            implements Comparable<Entry>
    {
        final Object element;
        final double weight;

        public Entry(Object element, double weight)
        {
            this.element = element;
            this.weight = weight;
        }

        public int compareTo(Entry other)
        {
            return (int) Math.signum(weight - other.weight);
        }
    }

    PriorityQueue<Entry> entries = new PriorityQueue<Entry>();

    public WeightedReservoirSample(long maxSamples)
    {
        super(maxSamples);
    }

    public WeightedReservoirSample(SliceInput sliceInput)
    {
        super(sliceInput.readLong());
        final Long numEntries = sliceInput.readLong();
        for (long i = 0; i < numEntries; ++i) {
            final Object sample = typeTraits.deserialize(sliceInput);
            final double weight = sliceInput.readDouble();
            entries.add(new Entry(sample, weight));
        }
    }

    protected WeightedReservoirSample(WeightedReservoirSample other)
    {
        super(other.maxSamples);
        entries = new PriorityQueue<Entry>(other.entries);
    }

    @Override
    protected void addObject(Object element, Double weight)
    {
        // Tmp Ami - check weight
        final double adjustedWeight = Math.pow(
                ThreadLocalRandom.current().nextDouble(),
                1.0 / weight);
        if (entries.size() < maxSamples) {
            entries.add(new Entry(element, adjustedWeight));
            return;
        }

        if (((Entry) entries.peek()).weight < adjustedWeight) {
            return;
        }

        entries.remove();
        entries.add(new Entry(element, adjustedWeight));
    }

    public void mergeWith(WeightedReservoirSample other)
    {
        entries.addAll(other.entries);
        while (entries.size() > maxSamples) {
            entries.remove();
        }
    }

    public AbstractReservoirSample clone()
    {
        return new WeightedReservoirSample(this);
    }

    @Override
    protected int getRequiredBytesForSerialization()
    {
        int bytes = typeTraits.getRequiredBytesForSerialization();
        bytes += SizeOf.SIZE_OF_LONG;
        bytes += Streams.stream(entries.iterator())
                .mapToInt(e ->
                        typeTraits.getRequiredBytesForSerialization(e.element) + SizeOf.SIZE_OF_DOUBLE)
                .sum();
        return bytes;
    }

    @Override
    public void serialize(SliceOutput sliceOutput)
    {
        sliceOutput.appendLong(maxSamples);
        typeTraits.serialize(sliceOutput);
        sliceOutput.appendLong(entries.size());
        Streams.stream(entries.iterator())
                .forEach(e -> {
                    typeTraits.serialize(sliceOutput, e.element);
                    sliceOutput.appendDouble(e.weight);
                });
    }

    @Override
    protected Iterator<Object> iterator()
    {
        return new Iterator<Object>()
        {
            Iterator<Entry> priorityQueueIterator = entries.iterator();

            @Override
            public boolean hasNext()
            {
                return priorityQueueIterator.hasNext();
            }

            @Override
            public Object next()
            {
                if (!priorityQueueIterator.hasNext()) {
                    throw new NoSuchElementException();
                }
                return ((Entry) priorityQueueIterator.next()).element;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    void write(BlockBuilder builder)
    {
        /*
        elements.stream()
                .forEach(o -> writeNativeValue(BigintType.BIGINT, builder, o));
         */
    }
}
