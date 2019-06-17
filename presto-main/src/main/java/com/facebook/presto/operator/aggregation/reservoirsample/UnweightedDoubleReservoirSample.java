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

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.DoubleStream;

import static com.google.common.base.Preconditions.checkArgument;

public class UnweightedDoubleReservoirSample
        implements Cloneable
{
    private static final int MAX_SAMPLES_LIMIT = 1_000_000;

    private final int maxSamples;
    private long count;
    private double[] elements;

    public UnweightedDoubleReservoirSample(int maxSamples)
    {
        this.maxSamples = maxSamples;
        checkArguments();
        this.elements = new double[0];
    }

    protected UnweightedDoubleReservoirSample(UnweightedDoubleReservoirSample other)
    {
        this.maxSamples = other.maxSamples;
        checkArguments();
        this.count = other.count;
        this.elements = new double[other.elements.length];
        System.arraycopy(other.elements, 0, elements, 0, elements.length);
    }

    public int getMaxSamples()
    {
        return maxSamples;
    }

    public void add(double element)
    {
        int index = getInsertIndex();
        if (index >= 0) {
            putAtIndex(index, Double.valueOf(element));
        }
    }

    protected int getInsertIndex()
    {
        ++count;
        if (elements.length < maxSamples) {
            return elements.length;
        }

        int index = (int) ThreadLocalRandom.current().nextInt(0, (int) count);
        System.out.println(index + " " + count);
        return index < elements.length ? index : -1;
    }

    protected void putAtIndex(int index, double element)
    {
        if (index == -1) {
            return;
        }

        if (elements.length < maxSamples) {
            double[] newElements = new double[elements.length + 1];
            System.arraycopy(elements, 0, newElements, 0, elements.length);
            newElements[elements.length] = element;
            elements = newElements;
            return;
        }

        elements[index] = element;
    }

    public void mergeWith(UnweightedDoubleReservoirSample other)
    {
        checkArgument(maxSamples == other.maxSamples, "Max samples must be equal");
        if (other.count < other.maxSamples) {
            Arrays.stream(other.elements).forEach(o -> add(o));
            return;
        }
        if (count < maxSamples) {
            final UnweightedDoubleReservoirSample target = ((UnweightedDoubleReservoirSample) other.clone());
            Arrays.stream(elements).forEach(o -> target.add(o));
            count = target.count;
            elements = target.elements;
            return;
        }

        shuffleArray(elements);
        shuffleArray(other.elements);
        int nextIndex = 0;
        int otherNextIndex = 0;
        double[] merged = new double[maxSamples];
        for (int i = 0; i < maxSamples; ++i) {
            if (ThreadLocalRandom.current().nextLong(0, count + other.count) < count) {
                merged[i] = elements[nextIndex++];
            }
            else {
                merged[i] = other.elements[otherNextIndex++];
            }
        }
        count += other.count;
        elements = merged;
    }

    @Override
    public UnweightedDoubleReservoirSample clone()
    {
        return new UnweightedDoubleReservoirSample(this);
    }

    public DoubleStream stream()
    {
        return Arrays.stream(elements);
    }

    private void checkArguments()
    {
        checkArgument(maxSamples > 0, "Maximum number of reservoir samples must be strictly positive");
        checkArgument(maxSamples <= MAX_SAMPLES_LIMIT, "Maximum number of reservoir samples is capped");
    }

    private static void shuffleArray(double[] ar)
    {
        for (int i = ar.length - 1; i > 0; i--) {
            int index = ThreadLocalRandom.current().nextInt(0, i + 1);
            double a = ar[index];
            ar[index] = ar[i];
            ar[i] = a;
        }
    }
}
