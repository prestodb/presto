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

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ReservoirSample
        implements Cloneable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ReservoirSampleState.class).instanceSize();
    private final Type type;
    private final Type arrayType;
    private ArrayList<Block> samples;
    private int maxSampleSize = -1;
    private long seenCount;

    public ReservoirSample(Type type)
    {
        this.type = requireNonNull(type, "type is null");
        this.arrayType = new ArrayType(type);
        this.samples = new ArrayList<>();
    }

    public ReservoirSample(ReservoirSample other)
    {
        this.type = other.type;
        this.arrayType = new ArrayType(type);
        this.seenCount = other.seenCount;
        this.samples = (ArrayList<Block>) other.samples.clone();
        this.maxSampleSize = other.maxSampleSize;
    }

    public ReservoirSample(Type type, long seenCount, int maxSampleSize, ArrayList<Block> samples)
    {
        this.type = type;
        this.arrayType = new ArrayType(type);
        this.seenCount = seenCount;
        this.samples = samples;
        this.maxSampleSize = maxSampleSize;
    }

    private static ArrayList<Block> mergeBlockSamples(ArrayList<Block> samples1, ArrayList<Block> samples2, long seenCount1, long seenCount2)
    {
        int nextIndex = 0;
        int otherNextIndex = 0;
        ArrayList<Block> merged = new ArrayList<>(samples1.size());
        for (int i = 0; i < samples1.size(); i++) {
            if (ThreadLocalRandom.current().nextLong(0, seenCount1 + seenCount2) < seenCount1) {
                merged.add(samples1.get(nextIndex++));
            }
            else {
                merged.add(samples2.get(otherNextIndex++));
            }
        }
        return merged;
    }

    private static void shuffleBlockArray(ArrayList<Block> samples)
    {
        for (int i = samples.size() - 1; i > 0; i--) {
            int index = ThreadLocalRandom.current().nextInt(0, i + 1);
            Block sample = samples.get(index);
            samples.set(index, samples.get(i));
            samples.set(i, sample);
        }
    }

    public void initializeSample(int n)
    {
        samples = new ArrayList<>(max(n, 0));
        maxSampleSize = n;
    }

    private boolean sampleNotInitialized()
    {
        return maxSampleSize < 0;
    }

    public int getSampleSize()
    {
        if (sampleNotInitialized()) {
            return 0;
        }
        return samples.size();
    }

    public int getMaxSampleSize()
    {
        return maxSampleSize;
    }

    /**
     * Potentially add a value from a block at a given position into the sample.
     *
     * @param block the block containing the potential sample
     * @param position the position in the block to potentially insert
     * @param n the maximum size of the sample in case it is not initialized
     */
    public void add(Block block, int position, long n)
    {
        if (sampleNotInitialized()) {
            initializeSample((int) n);
        }
        seenCount++;
        int sampleSize = getMaxSampleSize();
        if (seenCount <= sampleSize) {
            BlockBuilder sampleBlock = type.createBlockBuilder(null, 1);
            type.appendTo(block, position, sampleBlock);
            samples.add(sampleBlock.build());
        }
        else {
            long index = ThreadLocalRandom.current().nextLong(0, seenCount);
            if (index < samples.size()) {
                BlockBuilder sampleBlock = type.createBlockBuilder(null, 1);
                type.appendTo(block, position, sampleBlock);
                samples.set((int) index, sampleBlock.build());
            }
        }
    }

    private void addSingleBlock(Block block)
    {
        seenCount++;
        int sampleSize = getMaxSampleSize();
        if (seenCount <= sampleSize) {
            samples.add(block);
        }
        else {
            long index = ThreadLocalRandom.current().nextLong(0L, seenCount);
            if (index < samples.size()) {
                samples.set((int) index, block);
            }
        }
    }

    public void merge(ReservoirSample other)
    {
        if (sampleNotInitialized()) {
            initializeSample(other.getMaxSampleSize());
        }
        checkArgument(
                getMaxSampleSize() == other.getMaxSampleSize(),
                format("maximum number of samples %s must be equal to that of other %s", getMaxSampleSize(), other.getMaxSampleSize()));
        if (other.seenCount < getMaxSampleSize()) {
            for (int i = 0; i < other.samples.size(); i++) {
                addSingleBlock(other.samples.get(i));
            }
            return;
        }
        if (seenCount < getMaxSampleSize()) {
            for (int i = 0; i < seenCount; i++) {
                other.addSingleBlock(samples.get(i));
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

    public ArrayList<Block> getSampleArray()
    {
        return samples;
    }

    public long getSeenCount()
    {
        return seenCount;
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE +
                SizeOf.sizeOfObjectArray(samples.size());
    }

    public void serialize(BlockBuilder out)
    {
        BlockBuilder sampleBlock = getSampleBlockBuilder();

        BIGINT.writeLong(out, seenCount);
        INTEGER.writeLong(out, maxSampleSize);
        INTEGER.writeLong(out, samples.size());
        arrayType.appendTo(sampleBlock.build(), 0, out);
    }

    BlockBuilder getSampleBlockBuilder()
    {
        int sampleSize = getSampleSize();
        BlockBuilder sampleBlock = arrayType.createBlockBuilder(null, sampleSize);
        BlockBuilder sampleEntryBuilder = sampleBlock.beginBlockEntry();
        for (int i = 0; i < sampleSize; i++) {
            type.appendTo(samples.get(i), 0, sampleEntryBuilder);
        }
        sampleBlock.closeEntry();
        return sampleBlock;
    }
}
