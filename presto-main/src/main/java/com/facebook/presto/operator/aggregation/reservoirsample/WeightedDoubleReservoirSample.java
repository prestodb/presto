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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class WeightedDoubleReservoirSample
        implements Cloneable
{
    public static final int MAX_SAMPLES_LIMIT = 1_000_000;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(WeightedDoubleReservoirSample.class).instanceSize();

    private int count;
    private double[] samples;
    private double[] weights;
    private double totalPopulationWeight;

    public WeightedDoubleReservoirSample(int maxSamples)
    {
        checkArgument(maxSamples > 0, format("Maximum number of samples must be positive: %s", maxSamples));
        checkArgument(
                maxSamples <= MAX_SAMPLES_LIMIT,
                format("Maximum number of samples must not exceed limit: %s %s", maxSamples, MAX_SAMPLES_LIMIT));

        this.samples = new double[maxSamples];
        this.weights = new double[maxSamples];
    }

    private WeightedDoubleReservoirSample(WeightedDoubleReservoirSample other)
    {
        this.count = other.count;
        this.samples = Arrays.copyOf(other.samples, other.samples.length);
        this.weights = Arrays.copyOf(other.weights, other.weights.length);
        this.totalPopulationWeight = other.totalPopulationWeight;
    }

    private WeightedDoubleReservoirSample(int count, double[] samples, double[] weights, double totalPopulationWeight)
    {
        this.count = count;
        this.samples = requireNonNull(samples, "samples is null");
        this.weights = requireNonNull(weights, "weights is null");
        this.totalPopulationWeight = totalPopulationWeight;
    }

    public long getMaxSamples()
    {
        return samples.length;
    }

    public void add(double sample, double weight)
    {
        checkArgument(weight >= 0, format("Weight %s cannot be negative", weight));
        totalPopulationWeight += weight;
        double adjustedWeight = Math.pow(
                ThreadLocalRandom.current().nextDouble(),
                1.0 / weight);
        addWithAdjustedWeight(sample, adjustedWeight);
    }

    private void addWithAdjustedWeight(double sample, double adjustedWeight)
    {
        if (count < samples.length) {
            samples[count] = sample;
            weights[count] = adjustedWeight;
            count++;
            bubbleUp();
            return;
        }

        if (adjustedWeight <= weights[0]) {
            return;
        }

        samples[0] = sample;
        weights[0] = adjustedWeight;
        bubbleDown();
    }

    public void mergeWith(WeightedDoubleReservoirSample other)
    {
        totalPopulationWeight += other.totalPopulationWeight;
        for (int i = 0; i < other.count; i++) {
            addWithAdjustedWeight(other.samples[i], other.weights[i]);
        }
    }

    @Override
    public WeightedDoubleReservoirSample clone()
    {
        return new WeightedDoubleReservoirSample(this);
    }

    public double[] getSamples()
    {
        return Arrays.copyOf(samples, count);
    }

    private void swap(int i, int j)
    {
        double tmpElement = samples[i];
        double tmpWeight = weights[i];
        samples[i] = samples[j];
        weights[i] = weights[j];
        samples[j] = tmpElement;
        weights[j] = tmpWeight;
    }

    private void bubbleDown()
    {
        int index = 0;
        while (leftChild(index) < count) {
            int smallestChildIndex = leftChild(index);

            if (rightChild(index) < count && weights[leftChild(index)] > weights[rightChild(index)]) {
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
        int index = count - 1;
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

    public static WeightedDoubleReservoirSample deserialize(SliceInput input)
    {
        int count = input.readInt();
        int maxSamples = input.readInt();
        checkArgument(count <= maxSamples, "count must not be larger than number of samples");
        double[] samples = new double[maxSamples];
        input.readBytes(Slices.wrappedDoubleArray(samples), count * SizeOf.SIZE_OF_DOUBLE);
        double[] weights = new double[maxSamples];
        input.readBytes(Slices.wrappedDoubleArray(weights), count * SizeOf.SIZE_OF_DOUBLE);
        double totalPopulationWeight = input.readDouble();
        return new WeightedDoubleReservoirSample(count, samples, weights, totalPopulationWeight);
    }

    public void serialize(SliceOutput output)
    {
        output.appendInt(count);
        output.appendInt(samples.length);
        for (int i = 0; i < count; i++) {
            output.appendDouble(samples[i]);
        }
        for (int i = 0; i < count; i++) {
            output.appendDouble(weights[i]);
        }
        output.appendDouble(totalPopulationWeight);
    }

    public int getRequiredBytesForSerialization()
    {
        return SizeOf.SIZE_OF_INT + // count
                SizeOf.SIZE_OF_INT + 2 * SizeOf.SIZE_OF_DOUBLE * Math.min(count, samples.length) + // samples, weights
                SizeOf.SIZE_OF_DOUBLE; // totalPopulationWeight;
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE +
                SizeOf.sizeOf(samples) +
                SizeOf.sizeOf(weights);
    }

    public double getTotalPopulationWeight()
    {
        return totalPopulationWeight;
    }
}
