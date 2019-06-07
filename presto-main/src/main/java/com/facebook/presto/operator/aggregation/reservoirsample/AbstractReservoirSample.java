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
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.Iterator;

/*
Abstract base class for reservoir samples.
 */
public abstract class AbstractReservoirSample
        implements Cloneable
{
    protected long maxSamples;
    protected SingleTypeTraits typeTraits = new SingleTypeTraits();

    protected AbstractReservoirSample(long maxSamples)
    {
        this.maxSamples = maxSamples;
    }

    protected AbstractReservoirSample(SliceInput sliceInput)
    {
        maxSamples = sliceInput.readLong();
        System.out.println("des maxSamples " + maxSamples);
        typeTraits = new SingleTypeTraits(sliceInput);
    }

    public void add(Double element, Double weight)
    {
        typeTraits.add(element);
        addObject(element, weight);
    }

    public void add(Integer element, Double weight)
    {
        typeTraits.add(element);
        addObject(element, weight);
    }

    public void add(Long element, Double weight)
    {
        typeTraits.add(element);
        addObject(element, weight);
    }

    public void add(Boolean element, Double weight)
    {
        typeTraits.add(element);
        addObject(element, weight);
    }

    abstract void write(BlockBuilder out);

    abstract int getRequiredBytesForSerialization();

    public abstract void serialize(SliceOutput sliceOut);

    protected abstract void addObject(Object element, Double weight);

    protected abstract Iterator<Object> iterator();

    public abstract AbstractReservoirSample clone();

    public long getMaxSamples()
    {
        return maxSamples;
    }
}
