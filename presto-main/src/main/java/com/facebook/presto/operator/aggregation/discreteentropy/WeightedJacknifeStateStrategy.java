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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class WeightedJacknifeStateStrategy
        implements DiscreteEntropyStateStrategy
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(WeightedJacknifeStateStrategy.class).instanceSize();
    private int[] samples;
    private double[] weights;
    private int[] counts;

    public WeightedJacknifeStateStrategy()
    {
        samples = new int[0];
        weights = new double[0];
        counts = new int[0];
    }

    private WeightedJacknifeStateStrategy(int[] samples, double[] weights, int[] counts)
    {
        this.samples = samples;
        this.weights = weights;
        this.counts = counts;
    }

    private WeightedJacknifeStateStrategy(WeightedJacknifeStateStrategy other)
    {
        this.samples = Arrays.copyOf(other.samples, other.samples.length);
        this.weights = Arrays.copyOf(other.weights, other.weights.length);
        this.counts = Arrays.copyOf(other.counts, other.counts.length);
    }

    @Override
    public void add(int sample, double weight)
    {
        increment(sample, weight, 1);
    }

    @Override
    public double calculateEntropy()
    {
        Map<Integer, Double> bucketWeights = new HashMap<>();
        long n = 0;
        for (int i = 0; i < samples.length; i++) {
            bucketWeights.put(samples[i], counts[i] * weights[i] + bucketWeights.getOrDefault(samples[i], 0.0));
            n += counts[i];
        }
        double sumWeight = bucketWeights.values().stream().mapToDouble(Double::doubleValue).sum();
        if (sumWeight == 0.0) {
            return 0.0;
        }
        double sumWeightLogWeight =
                bucketWeights.values().stream().mapToDouble(w -> w == 0.0 ? 0.0 : w * Math.log(w)).sum();

        double entropy = n * EntropyCalculations.calculateEntropyFromAggregates(sumWeight, sumWeightLogWeight);
        for (int i = 0; i < samples.length; i++) {
            double weight = bucketWeights.get(samples[i]);
            if (weight > 0.0) {
                entropy -= EntropyCalculations.getHoldOutEntropy(
                        n,
                        sumWeight,
                        sumWeightLogWeight,
                        weight,
                        weights[i],
                        counts[i]);
            }
        }
        return entropy;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                SizeOf.sizeOf(samples) +
                SizeOf.sizeOf(weights) +
                SizeOf.sizeOf(counts);
    }

    @Override
    public int getRequiredBytesForSpecificSerialization()
    {
        return SizeOf.SIZE_OF_INT + // size
                samples.length * (SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_DOUBLE + SizeOf.SIZE_OF_INT); // arrays
    }

    @Override
    public void mergeWith(DiscreteEntropyStateStrategy other)
    {
        WeightedJacknifeStateStrategy otherStrategy = (WeightedJacknifeStateStrategy) other;
        for (int i = 0; i < otherStrategy.samples.length; ++i) {
            checkState(otherStrategy.weights[i] >= 0, "Weights must be nonnegative");
            increment(otherStrategy.samples[i], otherStrategy.weights[i], otherStrategy.counts[i]);
        }
    }

    public static WeightedJacknifeStateStrategy deserialize(SliceInput input)
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
        int[] counts = new int[size];
        input.readBytes(
                Slices.wrappedIntArray(counts),
                size * SizeOf.SIZE_OF_INT);

        return new WeightedJacknifeStateStrategy(samples, weights, counts);
    }

    @Override
    public void serialize(SliceOutput out)
    {
        out.appendInt(samples.length);
        IntStream.range(0, samples.length).forEach(i -> out.appendInt(samples[i]));
        IntStream.range(0, weights.length).forEach(i -> out.appendDouble(weights[i]));
        IntStream.range(0, counts.length).forEach(i -> out.appendInt(counts[i]));
    }

    @Override
    public DiscreteEntropyStateStrategy clone()
    {
        return new WeightedJacknifeStateStrategy(this);
    }

    private void increment(int sample, double weight, int count)
    {
        checkArgument(weight >= 0, "Weight must be non-negative");
        checkArgument(count > 0, "Count must be positive");
        if (weight == 0.0 || count == 0) {
            return;
        }

        int foundIndex = lowerBoundBinarySearch(sample, weight);
        if (foundIndex < samples.length && samples[foundIndex] == sample && weights[foundIndex] == weight) {
            counts[foundIndex] += count;
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

        int[] newCounts = new int[counts.length + 1];
        System.arraycopy(counts, 0, newCounts, 0, foundIndex);
        newCounts[foundIndex] = count;
        System.arraycopy(counts, foundIndex, newCounts, foundIndex + 1, counts.length - foundIndex);
        counts = newCounts;
    }

    private int lowerBoundBinarySearch(int sample, double weight)
    {
        int count = samples.length;
        int first = 0;
        while (count > 0) {
            int step = count / 2;
            int index = first + step;
            if (samples[index] < sample || (samples[index] == sample && weights[index] < weight)) {
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
