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

import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;

public class WeightedDoubleReservoirSample
        implements Cloneable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(WeightedDoubleReservoirSample.class).instanceSize();
    private static final int MAX_SAMPLES_LIMIT = 1_000_000;
    private static boolean DEBUG = true;

    private int size;
    private double[] samples;
    private double[] weights;

    public WeightedDoubleReservoirSample(int maxSamples)
    {
        checkArgument(maxSamples <= MAX_SAMPLES_LIMIT, "Maximum number of reservoir samples is capped");

        this.size = 0;
        this.samples = new double[maxSamples];
        this.weights = new double[maxSamples];
        assertValid();
    }

    protected WeightedDoubleReservoirSample(WeightedDoubleReservoirSample other)
    {
        this.size = 0;
        this.samples = new double[other.samples.length];
        this.weights = new double[other.weights.length];
        System.arraycopy(other.samples, 0, samples, 0, samples.length);
        System.arraycopy(other.weights, 0, weights, 0, weights.length);
        assertValid();
    }

    public WeightedDoubleReservoirSample(SliceInput input)
    {
        size = input.readInt();
        int maxSamples = input.readInt();
        this.samples = new double[maxSamples];
        input.readBytes(
                Slices.wrappedDoubleArray(samples),
                maxSamples * SizeOf.SIZE_OF_DOUBLE);
        this.weights = new double[maxSamples];
        input.readBytes(
                Slices.wrappedDoubleArray(weights),
                maxSamples * SizeOf.SIZE_OF_DOUBLE);
    }

    public long getMaxSamples()
    {
        return samples.length;
    }

    public void add(double element, double weight)
    {
        addWithAdjustedWeight(element, getAdjustedWeight(weight));
    }

    private void addWithAdjustedWeight(double element, double adjustedWeight)
    {
        if (size < samples.length) {
            samples[size] = element;
            weights[size] = adjustedWeight;
            ++size;
            bubbleUp();
            assertValid();
            return;
        }

        if (adjustedWeight <= weights[0]) {
            return;
        }

        samples[0] = element;
        weights[0] = adjustedWeight;
        bubbleDown();
        assertValid();
    }

    protected double getAdjustedWeight(double weight)
    {
        checkArgument(weight >= 0, "Weight cannot be negative");
        return Math.pow(
                ThreadLocalRandom.current().nextDouble(),
                1.0 / weight);
    }

    public void mergeWith(WeightedDoubleReservoirSample other)
    {
        for (int i = 0; i < other.samples.length; ++i) {
            addWithAdjustedWeight(other.samples[i], other.weights[i]);
        }
    }

    @Override
    public WeightedDoubleReservoirSample clone()
    {
        return new WeightedDoubleReservoirSample(this);
    }

    public DoubleStream stream()
    {
        return Arrays.stream(samples, 0, size);
    }

    private void assertValid()
    {
        checkArgument(samples.length > 0, "Number of reservoir samples must be strictly positive");
        checkArgument(size <= samples.length, "Size must be at most number of samples");

        if (DEBUG) {
            for (int i = 0; i < samples.length; ++i) {
                if (leftChild(i) < size) {
                    assert (weights[i] <= weights[leftChild(i)]);
                }
                if (rightChild(i) < size) {
                    assert (weights[i] <= weights[rightChild(i)]);
                }
            }
        }
    }

    private void swap(int fpos, int spos)
    {
        double tmpElement = samples[fpos];
        double tmpWeight = weights[fpos];
        samples[fpos] = samples[spos];
        weights[fpos] = weights[spos];
        samples[spos] = tmpElement;
        weights[spos] = tmpWeight;
    }

    private void bubbleDown()
    {
        int index = 0;
        while (leftChild(index) < size) {
            int smallestChildIndex = leftChild(index);

            if (rightChild(index) < size && weights[leftChild(index)] > weights[rightChild(index)]) {
                smallestChildIndex = rightChild(index);
            }

            if (weights[index] > weights[smallestChildIndex]) {
                swap(index, smallestChildIndex);
            }
            else {
                break;
            }

            index = smallestChildIndex;
        }
    }

    private void bubbleUp()
    {
        int index = size - 1;
        while (index > 0 && weights[index] < weights[parent(index)]) {
            swap(index, parent(index));
            index = parent(index);
        }
    }

    private static int parent(int pos)
    {
        return pos / 2;
    }

    private static int leftChild(int pos)
    {
        return 2 * pos;
    }

    private static int rightChild(int pos)
    {
        return 2 * pos + 1;
    }

    public void serialize(SliceOutput out)
    {
        out.appendInt(size);
        IntStream.range(0, size).forEach(i -> out.appendDouble(samples[i]));
        IntStream.range(0, size).forEach(i -> out.appendDouble(weights[i]));
    }

    public int getRequiredBytesForSerialization()
    {
        return SizeOf.SIZE_OF_INT + // size
                2 * SizeOf.SIZE_OF_DOUBLE * Math.min(size, samples.length); // samples, weights
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE +
                SizeOf.SIZE_OF_INT + // size
                2 * SizeOf.SIZE_OF_DOUBLE * Math.min(size, samples.length); // samples, weights
    }
}
