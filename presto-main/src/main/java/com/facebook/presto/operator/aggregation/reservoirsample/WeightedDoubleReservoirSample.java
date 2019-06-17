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
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;

public class WeightedDoubleReservoirSample
        implements Cloneable
{
    private int size;
    private double[] elements;
    private double[] weights;

    public WeightedDoubleReservoirSample(int maxSamples)
    {
        this.elements = new double[maxSamples];
        this.weights = new double[maxSamples];
        assertValid();
    }

    protected WeightedDoubleReservoirSample(WeightedDoubleReservoirSample other)
    {
        this.elements = new double[other.elements.length];
        this.weights = new double[other.weights.length];
        assertValid();
        System.arraycopy(other.elements, 0, elements, 0, elements.length);
        System.arraycopy(other.weights, 0, weights, 0, weights.length);
    }

    public WeightedDoubleReservoirSample(SliceInput input)
    {
        int length = input.readInt();
        this.elements = new double[length];
        input.readBytes(
                Slices.wrappedDoubleArray(elements),
                length * SizeOf.SIZE_OF_DOUBLE);
        this.weights = new double[length];
        input.readBytes(
                Slices.wrappedDoubleArray(weights),
                length * SizeOf.SIZE_OF_DOUBLE);
    }

    public long getMaxSamples()
    {
        return elements.length;
    }

    public void add(double element, double weight)
    {
        double adjustedWeight = getAdjustedWeight(weight);

        if (size < elements.length) {
            elements[size] = element;
            weights[size] = adjustedWeight;
            ++size;
            bubbleUp();
            assertValid();
            return;
        }

        if (adjustedWeight >= weights[0]) {
            return;
        }

        elements[0] = element;
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
        IntStream.range(0, other.elements.length).forEach(i -> add(other.elements[i], other.weights[i]));
    }

    @Override
    public WeightedDoubleReservoirSample clone()
    {
        return new WeightedDoubleReservoirSample(this);
    }

    public DoubleStream stream()
    {
        return Arrays.stream(elements);
    }

    private void assertValid()
    {
        checkArgument(elements.length > 0, "Number of reservoir samples must be strictly positive");
        checkArgument(size > 0, "Size must be strictly positive");
        checkArgument(size <= elements.length, "Size must be at most number of samples");

        for (int i = 0; i < elements.length; ++i) {
            if (leftChild(i) < size) {
                assert (weights[i] >= weights[leftChild(i)]);
            }
            if (rightChild(i) < size) {
                assert (weights[i] >= weights[rightChild(i)]);
            }
        }
    }

    private void swap(int fpos, int spos)
    {
        double tmpElement = elements[fpos];
        double tmpWeight = weights[fpos];
        elements[fpos] = elements[spos];
        weights[fpos] = weights[spos];
        elements[spos] = tmpElement;
        weights[spos] = tmpWeight;
    }

    private void bubbleDown()
    {
        int index = 0;
        while (leftChild(index) < size) {
            int largestChildIndex = leftChild(index);

            if (rightChild(index) < size && weights[leftChild(index)] > weights[leftChild(index)]) {
                largestChildIndex = rightChild(index);
            }

            if (weights[index] < weights[largestChildIndex]) {
                swap(index, largestChildIndex);
            }
            else {
                break;
            }

            index = largestChildIndex;
        }
    }

    private void bubbleUp()
    {
        int index = size - 1;
        while (index > 0 && weights[index] > weights[parent(index)]) {
            // parent/child are out of order; swap them
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
}
