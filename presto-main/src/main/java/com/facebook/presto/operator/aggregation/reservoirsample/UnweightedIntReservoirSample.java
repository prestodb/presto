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

public class UnweightedIntReservoirSample
        implements Cloneable
{
    public static final int MAX_SAMPLES_LIMIT = 1_000_000;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(UnweightedIntReservoirSample.class).instanceSize();

    private int seenCount;
    private int[] samples;

    public UnweightedIntReservoirSample(int maxSamples)
    {
        checkArgument(
                maxSamples > 0,
                format("Maximum number of samples must be positive: %s", maxSamples));
        checkArgument(
                maxSamples <= MAX_SAMPLES_LIMIT,
                format("Maximum number of samples must not exceed maximum: %s %s", maxSamples, MAX_SAMPLES_LIMIT));

        this.samples = new int[maxSamples];
    }

    private UnweightedIntReservoirSample(UnweightedIntReservoirSample other)
    {
        this.seenCount = other.seenCount;
        this.samples = Arrays.copyOf(requireNonNull(other.samples, "samples is null"), other.samples.length);
    }

    private UnweightedIntReservoirSample(int seenCount, int[] samples)
    {
        this.seenCount = seenCount;
        this.samples = samples;
    }

    public int getMaxSamples()
    {
        return samples.length;
    }

    public void add(int sample)
    {
        seenCount++;
        if (seenCount <= samples.length) {
            samples[seenCount - 1] = sample;
            return;
        }
        int index = ThreadLocalRandom.current().nextInt(0, seenCount);
        if (index < samples.length) {
            samples[index] = sample;
        }
    }

    public void mergeWith(UnweightedIntReservoirSample other)
    {
        checkArgument(
                samples.length == other.samples.length,
                format("Maximum number of samples %s must be equal to that of other %s", samples.length, other.samples.length));
        if (other.seenCount < other.samples.length) {
            for (int i = 0; i < other.seenCount; i++) {
                add(other.samples[i]);
            }
            return;
        }
        if (seenCount < samples.length) {
            UnweightedIntReservoirSample target = ((UnweightedIntReservoirSample) other.clone());
            for (int i = 0; i < seenCount; i++) {
                target.add(samples[i]);
            }
            seenCount = target.seenCount;
            samples = target.samples;
            return;
        }

        shuffleArray(samples);
        shuffleArray(other.samples);
        int nextIndex = 0;
        int otherNextIndex = 0;
        int[] merged = new int[samples.length];
        for (int i = 0; i < samples.length; i++) {
            if (ThreadLocalRandom.current().nextLong(0, seenCount + other.seenCount) < seenCount) {
                merged[i] = samples[nextIndex++];
            }
            else {
                merged[i] = other.samples[otherNextIndex++];
            }
        }
        seenCount += other.seenCount;
        samples = merged;
    }

    public int getTotalPopulationCount()
    {
        return seenCount;
    }

    @Override
    public UnweightedIntReservoirSample clone()
    {
        return new UnweightedIntReservoirSample(this);
    }

    public int[] getSamples()
    {
        return Arrays.copyOf(samples, Math.min(seenCount, samples.length));
    }

    private static void shuffleArray(int[] samples)
    {
        for (int i = samples.length - 1; i > 0; i--) {
            int index = ThreadLocalRandom.current().nextInt(0, i + 1);
            int sample = samples[index];
            samples[index] = samples[i];
            samples[i] = sample;
        }
    }

    public static UnweightedIntReservoirSample deserialize(SliceInput input)
    {
        int seenCount = input.readInt();
        int maxSamples = input.readInt();
        int[] samples = new int[maxSamples];
        input.readBytes(Slices.wrappedIntArray(samples), Math.min(seenCount, samples.length) * SizeOf.SIZE_OF_DOUBLE);
        return new UnweightedIntReservoirSample(seenCount, samples);
    }

    public void serialize(SliceOutput output)
    {
        output.appendInt(seenCount);
        output.appendInt(samples.length);
        for (int i = 0; i < Math.min(seenCount, samples.length); i++) {
            output.appendDouble(samples[i]);
        }
    }

    public int getRequiredBytesForSerialization()
    {
        return SizeOf.SIZE_OF_INT + // seenCount
                SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_DOUBLE * Math.min(seenCount, samples.length); // samples
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE +
                SizeOf.sizeOf(samples);
    }
}
