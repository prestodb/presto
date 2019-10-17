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
package com.facebook.presto.operator.aggregation.discreteentropy;

import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class WeightedMleStateStrategy
        implements DiscreteEntropyStateStrategy
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(WeightedMleStateStrategy.class).instanceSize();
    private int[] samples;
    private double[] weights;

    public WeightedMleStateStrategy()
    {
        samples = new int[0];
        weights = new double[0];
    }

    private WeightedMleStateStrategy(int[] samples, double[] weights)
    {
        this.samples = samples;
        this.weights = weights;
    }

    private WeightedMleStateStrategy(WeightedMleStateStrategy other)
    {
        this.samples = Arrays.copyOf(other.samples, other.samples.length);
        this.weights = Arrays.copyOf(other.weights, other.weights.length);
    }

    @Override
    public void add(int sample, double weight)
    {
        increment(sample, weight);
    }

    @Override
    public double calculateEntropy()
    {
        return EntropyCalculations.calculateEntropy(weights);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                SizeOf.sizeOf(samples) +
                SizeOf.sizeOf(weights);
    }

    @Override
    public int getRequiredBytesForSpecificSerialization()
    {
        return SizeOf.SIZE_OF_INT + // size
                samples.length * (SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_DOUBLE); // arrays
    }

    @Override
    public void mergeWith(DiscreteEntropyStateStrategy other)
    {
        WeightedMleStateStrategy otherStrategy = (WeightedMleStateStrategy) other;
        for (int i = 0; i < otherStrategy.samples.length; ++i) {
            increment(otherStrategy.samples[i], otherStrategy.weights[i]);
        }
    }

    public static WeightedMleStateStrategy deserialize(SliceInput input)
    {
        int size = input.readInt();
        int[] samples = new int[size];
        input.readBytes(
                Slices.wrappedIntArray(samples),
                size * SizeOf.SIZE_OF_INT);
        double[] weights = new double[size];
        input.readBytes(
                Slices.wrappedDoubleArray(weights),
                size * SizeOf.SIZE_OF_DOUBLE);
        checkState(IntStream.range(0, samples.length - 1).noneMatch(i -> samples[i] > samples[i + 1]), "weights must be sorted");
        checkState(!Arrays.stream(weights).filter(w -> w < 0).findFirst().isPresent(), "Weights must be non-negative");
        return new WeightedMleStateStrategy(samples, weights);
    }

    @Override
    public void serialize(SliceOutput out)
    {
        out.appendInt(samples.length);
        IntStream.range(0, samples.length).forEach(i -> out.appendInt(samples[i]));
        IntStream.range(0, weights.length).forEach(i -> out.appendDouble(weights[i]));
    }

    @Override
    public DiscreteEntropyStateStrategy clone()
    {
        return new WeightedMleStateStrategy(this);
    }

    private void increment(int sample, double weight)
    {
        checkArgument(weight >= 0, "Count must be positive");
        int foundIndex = lowerBoundBinarySearch(sample);
        if (foundIndex < samples.length && samples[foundIndex] == sample) {
            weights[foundIndex] += weight;
            return;
        }

        int[] newSamples = new int[samples.length + 1];
        System.arraycopy(samples, 0, newSamples, 0, foundIndex);
        newSamples[foundIndex] = sample;
        System.arraycopy(samples, foundIndex, newSamples, foundIndex + 1, samples.length - foundIndex);
        samples = newSamples;

        double[] newWeights = new double[weights.length + 1];
        System.arraycopy(weights, 0, newWeights, 0, foundIndex);
        newWeights[foundIndex] = weight;
        System.arraycopy(weights, foundIndex, newWeights, foundIndex + 1, weights.length - foundIndex);
        weights = newWeights;
    }

    private int lowerBoundBinarySearch(int sample)
    {
        int count = samples.length;
        int first = 0;
        while (count > 0) {
            int step = count / 2;
            int index = first + step;
            if (samples[index] < sample) {
                first = index + 1;
                count -= step + 1;
            }
            else {
                count = step;
            }
        }
        return first;
    }
}
