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

public class UnweightedDoubleReservoirSample
        implements Cloneable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(WeightedDoubleReservoirSample.class).instanceSize();
    private static final int MAX_SAMPLES_LIMIT = 1_000_000;

    private int count;
    private double[] samples;

    public UnweightedDoubleReservoirSample(int maxSamples)
    {
        checkArgument(maxSamples > 0, "Cannot use 0 number of samples for a reservoir");
        checkArgument(maxSamples <= MAX_SAMPLES_LIMIT, "Maximum number of reservoir samples is capped");

        this.count = 0;
        this.samples = new double[maxSamples];
    }

    public UnweightedDoubleReservoirSample(SliceInput input)
    {
        count = input.readInt();
        int maxSamples = input.readInt();
        this.samples = new double[maxSamples];
        input.readBytes(
                Slices.wrappedDoubleArray(samples),
                maxSamples * SizeOf.SIZE_OF_DOUBLE);
    }

    protected UnweightedDoubleReservoirSample(UnweightedDoubleReservoirSample other)
    {
        this.count = other.count;
        this.count = other.count;
        this.samples = new double[other.samples.length];
        System.arraycopy(other.samples, 0, samples, 0, samples.length);
    }

    public int getMaxSamples()
    {
        return samples.length;
    }

    public void add(double element)
    {
        if (++count <= samples.length) {
            samples[count - 1] = element;
            return;
        }
        int index = ThreadLocalRandom.current().nextInt(0, count);
        if (index < samples.length) {
            samples[index] = element;
        }
    }

    public void mergeWith(UnweightedDoubleReservoirSample other)
    {
        checkArgument(samples.length == other.samples.length, "Max samples must be equal");
        if (other.count < other.samples.length) {
            Arrays.stream(other.samples).forEach(o -> add(o));
            return;
        }
        if (count < samples.length) {
            final UnweightedDoubleReservoirSample target = ((UnweightedDoubleReservoirSample) other.clone());
            Arrays.stream(samples).forEach(o -> target.add(o));
            count = target.count;
            samples = target.samples;
            return;
        }

        shuffleArray(samples);
        shuffleArray(other.samples);
        int nextIndex = 0;
        int otherNextIndex = 0;
        double[] merged = new double[samples.length];
        for (int i = 0; i < samples.length; ++i) {
            if (ThreadLocalRandom.current().nextLong(0, count + other.count) < count) {
                merged[i] = samples[nextIndex++];
            }
            else {
                merged[i] = other.samples[otherNextIndex++];
            }
        }
        count += other.count;
        samples = merged;
    }

    @Override
    public UnweightedDoubleReservoirSample clone()
    {
        return new UnweightedDoubleReservoirSample(this);
    }

    public DoubleStream stream()
    {
        int effectiveSize = Math.min(count, samples.length);
        return Arrays.stream(samples, 0, effectiveSize);
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

    public void serialize(SliceOutput out)
    {
        out.appendInt(count);
        out.appendInt(samples.length);
        IntStream.range(0, Math.min(count, samples.length)).forEach(i -> out.appendDouble(samples[i]));
    }

    public int getRequiredBytesForSerialization()
    {
        return SizeOf.SIZE_OF_INT + // count
                SizeOf.SIZE_OF_INT + // length
                2 * SizeOf.SIZE_OF_DOUBLE * Math.min(count, samples.length); // samples
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE +
                SizeOf.SIZE_OF_INT + // count
                SizeOf.SIZE_OF_INT + // length
                2 * SizeOf.SIZE_OF_DOUBLE * Math.min(count, samples.length); // samples
    }
}
