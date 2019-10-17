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

public class UnweightedJacknifeStateStrategy
        implements DiscreteEntropyStateStrategy
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(UnweightedJacknifeStateStrategy.class).instanceSize();
    private int[] samples;
    private int[] counts;

    public UnweightedJacknifeStateStrategy()
    {
        samples = new int[0];
        counts = new int[0];
    }

    private UnweightedJacknifeStateStrategy(int[] samples, int[] counts)
    {
        this.samples = samples;
        this.counts = counts;
    }

    private UnweightedJacknifeStateStrategy(UnweightedJacknifeStateStrategy other)
    {
        this.samples = Arrays.copyOf(other.samples, other.samples.length);
        this.counts = Arrays.copyOf(other.counts, other.counts.length);
    }

    @Override
    public void add(int sample)
    {
        increment(sample, 1);
    }

    @Override
    public double calculateEntropy()
    {
        double sumWeight = 0;
        double sumWeightLogWeight = 0;
        long n = 0;
        for (int i = 0; i < counts.length; i++) {
            sumWeight += counts[i];
            sumWeightLogWeight += counts[i] == 0 ? 0 : counts[i] * Math.log(counts[i]);
            n += counts[i];
        }
        if (sumWeight == 0.0) {
            return 0.0;
        }

        double entropy = n * EntropyCalculations.calculateEntropyFromAggregates(sumWeight, sumWeightLogWeight);
        for (int i = 0; i < counts.length; i++) {
            if (counts[i] > 0.0) {
                entropy -= EntropyCalculations.getHoldOutEntropy(
                        n,
                        sumWeight,
                        sumWeightLogWeight,
                        counts[i],
                        1,
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
                SizeOf.sizeOf(counts);
    }

    @Override
    public int getRequiredBytesForSpecificSerialization()
    {
        return SizeOf.SIZE_OF_INT + // size
                samples.length * (SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_INT); // arrays
    }

    @Override
    public void mergeWith(DiscreteEntropyStateStrategy other)
    {
        UnweightedJacknifeStateStrategy otherStrategy = (UnweightedJacknifeStateStrategy) other;
        for (int i = 0; i < otherStrategy.samples.length; ++i) {
            increment(otherStrategy.samples[i], otherStrategy.counts[i]);
        }
    }

    public static UnweightedJacknifeStateStrategy deserialize(SliceInput input)
    {
        int size = input.readInt();
        int[] samples = new int[size];
        input.readBytes(
                Slices.wrappedIntArray(samples),
                size * SizeOf.SIZE_OF_INT);
        int[] counts = new int[size];
        input.readBytes(
                Slices.wrappedIntArray(counts),
                size * SizeOf.SIZE_OF_INT);
        checkState(IntStream.range(0, samples.length - 1).noneMatch(i -> samples[i] > samples[i + 1]), "weights must be sorted");
        checkState(!Arrays.stream(counts).filter(c -> c < 0).findFirst().isPresent(), "Counts must be non-negative");

        return new UnweightedJacknifeStateStrategy(samples, counts);
    }

    @Override
    public void serialize(SliceOutput out)
    {
        out.appendInt(samples.length);
        IntStream.range(0, samples.length).forEach(i -> out.appendInt(samples[i]));
        IntStream.range(0, counts.length).forEach(i -> out.appendInt(counts[i]));
    }

    @Override
    public DiscreteEntropyStateStrategy clone()
    {
        return new UnweightedJacknifeStateStrategy(this);
    }

    private void increment(int sample, int count)
    {
        checkArgument(count > 0, "Count must be positive");
        int foundIndex = lowerBoundBinarySearch(sample);
        if (foundIndex < samples.length && samples[foundIndex] == sample) {
            counts[foundIndex] += count;
            return;
        }

        int[] newSamples = new int[samples.length + 1];
        System.arraycopy(samples, 0, newSamples, 0, foundIndex);
        newSamples[foundIndex] = sample;
        System.arraycopy(samples, foundIndex, newSamples, foundIndex + 1, samples.length - foundIndex);
        samples = newSamples;

        int[] newCounts = new int[counts.length + 1];
        System.arraycopy(counts, 0, newCounts, 0, foundIndex);
        newCounts[foundIndex] = count;
        System.arraycopy(counts, foundIndex, newCounts, foundIndex + 1, counts.length - foundIndex);
        counts = newCounts;
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
