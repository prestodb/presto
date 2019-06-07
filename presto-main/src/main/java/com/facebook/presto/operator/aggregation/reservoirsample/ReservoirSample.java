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
import com.facebook.presto.spi.type.BigintType;
import com.google.common.collect.Streams;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.google.common.base.Preconditions.checkArgument;

/*
Unweighted reservoir samples.
 */
public class ReservoirSample
        extends AbstractReservoirSample
{
    protected long count;
    protected ArrayList<Object> elements;

    public ReservoirSample(long maxSamples)
    {
        super(maxSamples);
        this.elements = new ArrayList<>();
    }

    public ReservoirSample(SliceInput sliceInput)
    {
        super(sliceInput.readLong());
        final Long numEntries = sliceInput.readLong();
        System.out.println("numEntries " + numEntries);
        for (long i = 0; i < numEntries; ++i) {
            elements.add(typeTraits.deserialize(sliceInput));
        }
    }

    public ReservoirSample(ReservoirSample other)
    {
        super(other.maxSamples);
        this.elements = new ArrayList<>(other.elements);
    }

    @Override
    protected void addObject(Object element, Double weight)
    {
        checkArgument(weight == null, "weight unsupported");

        ++count;

        System.out.println("Count " + count + " size " + elements.size() + " Add " + element);
        if (elements.size() < maxSamples) {
            System.out.println("Size " + elements.size() + " Adding new " + element);
            elements.add(element);
            return;
        }

        int index = (int) ThreadLocalRandom.current().nextInt(0, (int) count);
        if (index < elements.size()) {
            System.out.println("Size " + elements.size() + " Replacing " + element);
            elements.set(index, element);
        }
    }

    public void mergeWith(ReservoirSample other)
    {
        if (other.count < other.maxSamples) {
            for (int i = 0; i < other.count; ++i) {
                addObject(other.elements.get(i), null);
            }
            return;
        }
        if (count < maxSamples) {
            other = ((ReservoirSample) other.clone());
            for (int i = 0; i < count; ++i) {
                other.addObject(elements.get(i), null);
            }
            count = other.count;
            maxSamples = other.maxSamples;
            elements = other.elements;
            return;
        }

        Collections.shuffle(elements);
        Collections.shuffle(other.elements);
        int nextIndex = 0;
        int otherNextIndex = 0;
        final ArrayList<Object> merged = new ArrayList<Object>();
        while (merged.size() < maxSamples) {
            if (ThreadLocalRandom.current().nextLong(0, count + other.count) < count) {
                merged.add(elements.get(nextIndex++));
            }
            else {
                merged.add(other.elements.get(otherNextIndex++));
            }
        }
        count += other.count;
        elements = merged;
    }

    @Override
    protected int getRequiredBytesForSerialization()
    {
        int bytes = SizeOf.SIZE_OF_LONG; // maxSamples
        bytes += typeTraits.getRequiredBytesForSerialization();
        bytes += SizeOf.SIZE_OF_LONG; // size
        bytes += Streams.stream(elements.iterator())
                .mapToInt(o -> typeTraits.getRequiredBytesForSerialization(o))
                .sum();
        return bytes;
    }

    @Override
    public void serialize(SliceOutput sliceOutput)
    {
        sliceOutput.appendLong(maxSamples);
        System.out.println("serialize maxSamples " + maxSamples);
        typeTraits.serialize(sliceOutput);
        sliceOutput.appendLong(elements.size());
        Streams.stream(elements.iterator())
                .forEach(o -> typeTraits.serialize(sliceOutput, o));
    }

    @Override
    public AbstractReservoirSample clone()
    {
        return new ReservoirSample(this);
    }

    @Override
    protected Iterator<Object> iterator()
    {
        return elements.iterator();
    }

    @Override
    void write(BlockBuilder builder)
    {
        elements.stream()
                .forEach(o -> {
                    writeNativeValue(BigintType.BIGINT, builder, o)
                });
    }
}
