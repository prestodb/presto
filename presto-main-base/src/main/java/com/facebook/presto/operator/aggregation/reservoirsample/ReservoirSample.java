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

import com.facebook.presto.common.block.ArrayBlock;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ReservoirSample
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleReservoirSampleState.class).instanceSize();
    private final Type type;

    public Type getArrayType()
    {
        return arrayType;
    }

    private final Type arrayType;
    /**
     * Represents the list of sampled values.
     * <br>
     * We use an {@link ArrayList} instead of {@link Block} because the
     * algorithm that generates reservoir samples requires shuffling of elements
     * in the reservoir.
     * <br>
     * The {@link Block} interface doesn't have any method for setting values at
     * arbitrary positions, so we resort to internally representing the sample
     * as a list and then combining the samples into a single block later.
     */
    private ArrayList<Block> samples;
    private int maxSampleSize = -1;
    private long processedCount;

    private Block initialSample;

    public Block getInitialSample()
    {
        return initialSample;
    }

    public long getInitialProcessedCount()
    {
        return initialProcessedCount;
    }

    private long initialProcessedCount = -1;

    public ReservoirSample(Type type)
    {
        this.type = requireNonNull(type, "type is null");
        this.arrayType = new ArrayType(type);
        this.samples = new ArrayList<>();
    }

    protected ReservoirSample(Type type, long processedCount, int maxSampleSize, Block samples, Block initialSample, long initialSeenCount)
    {
        this.type = requireNonNull(type, "type is null");
        this.arrayType = new ArrayType(type);
        this.processedCount = processedCount;
        this.samples = blockToList(samples);
        this.maxSampleSize = maxSampleSize;
        initializeInitialSample(initialSample, initialSeenCount);
    }

    private static ArrayList<Block> blockToList(Block inputBlock)
    {
        // sometimes single values such as bigint/double are serialized as
        // LongArrayBlock which don't implement the Block::getBlock function.
        // ArrayBlock::getSingleValueBlock returns another ArrayBlock of size 1, whereas
        // we need to extract the internal block rather than have an array
        Function<Integer, Block> extractor = inputBlock instanceof ArrayBlock ? inputBlock::getBlock : inputBlock::getSingleValueBlock;
        return IntStream.range(0, inputBlock.getPositionCount())
                .mapToObj(extractor::apply)
                .collect(Collectors.toCollection(ArrayList::new));
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

    public void tryInitialize(int n)
    {
        if (sampleNotInitialized()) {
            samples = new ArrayList<>(max(n, 0));
            maxSampleSize = n;
        }
    }

    public void initializeInitialSample(@Nullable Block initialSample, long initialProcessedCount)
    {
        if (this.initialProcessedCount < 0) {
            if (initialSample != null && initialSample.getPositionCount() > 0) {
                checkArgument(initialProcessedCount >= initialSample.getPositionCount(),
                        "initialProcessedCount must be greater than or equal " +
                                "to the number of positions in the initial sample");
            }
            this.initialSample = initialSample;
            this.initialProcessedCount = initialProcessedCount;
        }
    }

    public void mergeWith(@Nullable ReservoirSample other)
    {
        if (other == null) {
            return;
        }
        merge(other);
        initializeInitialSample(other.initialSample, other.initialProcessedCount);
    }

    private boolean sampleNotInitialized()
    {
        return maxSampleSize < 0 || samples == null;
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
     */
    public void add(Block block, int position)
    {
        if (sampleNotInitialized()) {
            throw new IllegalArgumentException("reservoir sample not properly initialized");
        }
        processedCount++;
        int sampleSize = getMaxSampleSize();
        if (processedCount <= sampleSize) {
            BlockBuilder sampleBlock = type.createBlockBuilder(null, 1);
            type.appendTo(block, position, sampleBlock);
            samples.add(sampleBlock.build());
        }
        else {
            long index = ThreadLocalRandom.current().nextLong(0, processedCount);
            if (index < samples.size()) {
                BlockBuilder sampleBlock = type.createBlockBuilder(null, 1);
                type.appendTo(block, position, sampleBlock);
                samples.set((int) index, sampleBlock.build());
            }
        }
    }

    private void addSingleBlock(Block block)
    {
        processedCount++;
        int sampleSize = getMaxSampleSize();
        if (processedCount <= sampleSize) {
            samples.add(block);
        }
        else {
            long index = ThreadLocalRandom.current().nextLong(0L, processedCount);
            if (index < samples.size()) {
                samples.set((int) index, block);
            }
        }
    }

    public void merge(ReservoirSample other)
    {
        if (sampleNotInitialized()) {
            tryInitialize(other.getMaxSampleSize());
        }
        if (other.sampleNotInitialized()) {
            return;
        }
        checkArgument(
                getMaxSampleSize() == other.getMaxSampleSize(),
                format("maximum number of samples %s must be equal to that of other %s", getMaxSampleSize(), other.getMaxSampleSize()));
        if (other.processedCount < getMaxSampleSize()) {
            for (int i = 0; i < other.samples.size(); i++) {
                addSingleBlock(other.samples.get(i));
            }
            return;
        }
        if (processedCount < getMaxSampleSize()) {
            for (int i = 0; i < processedCount; i++) {
                other.addSingleBlock(samples.get(i));
            }
            processedCount = other.processedCount;
            samples = other.samples;
            return;
        }
        Collections.shuffle(samples);
        Collections.shuffle(other.samples);
        samples = mergeBlockSamples(samples, other.samples, processedCount, other.processedCount);
        processedCount += other.processedCount;
    }

    public Type getType()
    {
        return type;
    }

    public long getProcessedCount()
    {
        return processedCount;
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE +
                (initialSample != null ? initialSample.getSizeInBytes() : 0) +
                SizeOf.sizeOfObjectArray(samples.size());
    }

    public void serialize(BlockBuilder out)
    {
        BlockBuilder sampleBlock = getSampleBlockBuilder();
        if (initialSample == null) {
            out.appendNull();
        }
        else {
            out.appendStructure(initialSample);
        }
        BIGINT.writeLong(out, initialProcessedCount);
        BIGINT.writeLong(out, processedCount);
        INTEGER.writeLong(out, maxSampleSize);
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
