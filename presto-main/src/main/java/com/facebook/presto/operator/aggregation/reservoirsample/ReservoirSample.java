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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ReservoirSample
        implements Cloneable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ReservoirSampleState.class).instanceSize();
    private final Type type;
    //    private BlockBuilder blockBuilder;
    private Block[] samples;
    private int seenCount;
    private boolean isEmpty = true;
    private boolean sampleInitialized;

    public ReservoirSample(Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    public ReservoirSample(ReservoirSample other)
    {
        this.type = other.type;
        this.seenCount = other.seenCount;
        this.samples = Arrays.copyOf(requireNonNull(other.samples, "samples is null"), other.samples.length);
        this.sampleInitialized = true;
    }

    public ReservoirSample(int seenCount, Block[] samples, Type type)
    {
        this.type = type;
        this.seenCount = seenCount;
        this.samples = samples;
        this.sampleInitialized = true;
    }

    private static Block[] mergeBlockSamples(Block[] samples1, Block[] samples2, int seenCount1, int seenCount2)
    {
        int nextIndex = 0;
        int otherNextIndex = 0;
        Block[] merged = new Block[samples1.length];
        for (int i = 0; i < samples1.length; i++) {
            if (ThreadLocalRandom.current().nextLong(0, seenCount1 + seenCount2) < seenCount1) {
                merged[i] = samples1[nextIndex++];
            }
            else {
                merged[i] = samples2[otherNextIndex++];
            }
        }
        return merged;
    }

    private static void shuffleBlockArray(Block[] samples)
    {
        for (int i = samples.length - 1; i > 0; i--) {
            int index = ThreadLocalRandom.current().nextInt(0, i + 1);
            Block sample = samples[index];
            samples[index] = samples[i];
            samples[i] = sample;
        }
    }

    public void initializeSample(long n)
    {
        samples = new Block[(int) n];
        sampleInitialized = true;
    }

    public int getSampleSize()
    {
        if (isSampleInitialized()) {
            return samples.length;
        }
        return 0;
    }

    public void add(Block block, int position)
    {
        isEmpty = false;
        seenCount++;
        int sampleSize = getSampleSize();
        if (seenCount <= sampleSize) {
            BlockBuilder sampleBlock = type.createBlockBuilder(null, 16);
            type.appendTo(block, position, sampleBlock);
            samples[seenCount - 1] = sampleBlock.build();
        }
        else {
            int index = ThreadLocalRandom.current().nextInt(0, seenCount);
            if (index < samples.length) {
                BlockBuilder sampleBlock = type.createBlockBuilder(null, 16);
                type.appendTo(block, position, sampleBlock);
                samples[index] = sampleBlock.build();
            }
        }
    }

    private void addSingleBlock(Block block)
    {
        seenCount++;
        int sampleSize = getSampleSize();
        if (seenCount <= sampleSize) {
            samples[seenCount - 1] = block;
        }
        else {
            int index = ThreadLocalRandom.current().nextInt(0, seenCount);
            if (index < samples.length) {
                samples[index] = block;
            }
        }
    }

    public void merge(ReservoirSample other)
    {
        if (!sampleInitialized) {
            initializeSample(other.samples.length);
        }
        checkArgument(
                samples.length == other.samples.length,
                format("Maximum number of samples %s must be equal to that of other %s", samples.length, other.samples.length));
        if (other.seenCount < other.samples.length) {
            for (int i = 0; i < other.seenCount; i++) {
                addSingleBlock(other.samples[i]);
            }
            return;
        }
        if (seenCount < samples.length) {
            for (int i = 0; i < seenCount; i++) {
                other.addSingleBlock(samples[i]);
            }
            seenCount = other.seenCount;
            samples = other.samples;
            return;
        }
        shuffleBlockArray(samples);
        shuffleBlockArray(other.samples);
        samples = mergeBlockSamples(samples, other.samples, seenCount, other.seenCount);
        seenCount += other.seenCount;
    }

    public ReservoirSample clone()
    {
        return new ReservoirSample(this);
    }

    public Type getType()
    {
        return type;
    }

    public Block[] getSampleArray()
    {
        return samples;
    }

    public boolean isEmpty()
    {
        return isEmpty;
    }

    public int getSeenCount()
    {
        return seenCount;
    }

    public boolean isSampleInitialized()
    {
        return sampleInitialized;
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE +
                SizeOf.sizeOf(samples);
    }

    public void serialize(BlockBuilder out)
    {
        BlockBuilder sampleBlock = getSampleBlockBuilder();
        INTEGER.writeLong(out, seenCount);
        INTEGER.writeLong(out, samples.length);
        new ArrayType(type).appendTo(sampleBlock.build(), 0, out);
    }

    BlockBuilder getSampleBlockBuilder()
    {
        BlockBuilder sampleBlock = new ArrayType(type).createBlockBuilder(null, 16);
        BlockBuilder sampleEntryBuilder = sampleBlock.beginBlockEntry();
        for (Block sample : samples) {
            type.appendTo(sample, 0, sampleEntryBuilder);
        }
        sampleBlock.closeEntry();
        return sampleBlock;
    }
}
