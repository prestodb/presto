package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorState;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ReservoirSampleState implements AccumulatorState {

//    private BlockBuilder blockBuilder;
    private Block[] samples;
    private final Type type;
    private int seenCount;

    private boolean isEmpty = true;

    private final int sampleSize = 200;

    public ReservoirSampleState(Type type)
    {
        this.type = requireNonNull(type, "type is null");
        this.samples = new Block[sampleSize];
    }

    public ReservoirSampleState(ReservoirSampleState other)
    {
        this.type = other.type;
        this.seenCount = other.seenCount;
        this.samples = Arrays.copyOf(requireNonNull(other.samples, "samples is null"), other.samples.length);;
    }

    public void add(Block block, int position)
    {
        isEmpty=false;
        seenCount++;
        if (seenCount <= sampleSize) {
            BlockBuilder sampleBlock = type.createBlockBuilder(null, 16);
            type.appendTo(block, position, sampleBlock);
            samples[seenCount-1] = sampleBlock.build();
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

    public void addSingleBlock(Block block) {
        seenCount++;
        if (seenCount <= sampleSize) {
            samples[seenCount-1] = block;
        }
        else {
            int index = ThreadLocalRandom.current().nextInt(0, seenCount);
            if (index < samples.length) {
                samples[index] = block;
            }
        }
    }

//    public void merge(ReservoirSampleState other)
//    {
//        other.blockBuilder.
//        if (blockBuilder == null) {
//            return;
//        }
//
//        for (int i = 0; i < blockBuilder.getPositionCount(); i++) {
//            consumer.accept(blockBuilder, i);
//        }
//    }

    public void merge(ReservoirSampleState other)
    {
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
            ReservoirSampleState target = ((ReservoirSampleState) other.clone());
            for (int i = 0; i < seenCount; i++) {
                target.addSingleBlock(samples[i]);
            }
            seenCount = target.seenCount;
            samples = target.samples;
            return;
        }
        shuffleBlockArray(samples);
        shuffleBlockArray(other.samples);
        samples = mergeBlockSamples(samples, other.samples, seenCount, other.seenCount);
        seenCount += other.seenCount;
        System.out.println("merge data");
    }

    private static Block[] mergeBlockSamples(Block[] samples1, Block[] samples2, int seenCount1, int seenCount2) {
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

    public int getTotalPopulationCount()
    {
        return seenCount;
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

    public static BlockBuilder mergeBlocks(BlockBuilder bb1, BlockBuilder bb2, int seenCount1, int seenCount2, Type type) {
        int nextIndex = 0;
        int otherNextIndex = 0;
        BlockBuilder merged = type.createBlockBuilder(null, bb1.getPositionCount());
        for (int i = 0; i < bb1.getPositionCount(); i++) {
            if (ThreadLocalRandom.current().nextLong(0, seenCount1 + seenCount2) < seenCount1) {
                type.appendTo(bb1.getBlock(nextIndex++), i, merged);
            }
            else {
                type.appendTo(bb2.getBlock(otherNextIndex++), i, merged);
            }
        }
        return merged;
    }


    public ReservoirSampleState clone()
    {
        return new ReservoirSampleState(this);
    }


    public Type getType() {
        return type;
    }

    public Block[] getSamples() {
        return samples;
    }

    public void reset()
    {
        samples = new Block[sampleSize];
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    @Override
    public long getEstimatedSize() {
        return 0;
    }



}
