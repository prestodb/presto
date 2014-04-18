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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.Page;
import com.facebook.presto.serde.PagesSerde;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class BootstrappedAggregation
        implements AggregationFunction
{
    private static final int SAMPLES = 100;
    private final BlockEncodingSerde blockEncodingSerde;
    private final AggregationFunction function;

    public BootstrappedAggregation(BlockEncodingSerde blockEncodingSerde, AggregationFunction function)
    {
        this.blockEncodingSerde = checkNotNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.function = checkNotNull(function, "function is null");
        checkArgument(function.getFinalType().equals(BIGINT) || function.getFinalType().equals(DOUBLE) , "bootstrap only supports functions that output a number");
    }

    @Override
    public List<Type> getParameterTypes()
    {
        return function.getParameterTypes();
    }

    @Override
    public Type getFinalType()
    {
        return VARCHAR;
    }

    @Override
    public Type getIntermediateType()
    {
        return VARCHAR;
    }

    @Override
    public boolean isDecomposable()
    {
        return function.isDecomposable();
    }

    @Override
    public Accumulator createAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int... argumentChannels)
    {
        checkArgument(sampleWeightChannel.isPresent(), "sample weight must be present for bootstrapping");
        return createDeterministicAggregation(maskChannel, sampleWeightChannel.get(), confidence, ThreadLocalRandom.current().nextLong(), argumentChannels);
    }

    @Override
    public Accumulator createIntermediateAggregation(double confidence)
    {
        return createDeterministicIntermediateAggregation(confidence, ThreadLocalRandom.current().nextLong());
    }

    @Override
    public GroupedAccumulator createGroupedAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int... argumentChannels)
    {
        checkArgument(sampleWeightChannel.isPresent(), "sample weight must be present for bootstrapping");
        return createDeterministicGroupedAggregation(maskChannel, sampleWeightChannel.get(), confidence, ThreadLocalRandom.current().nextLong(), argumentChannels);
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAggregation(double confidence)
    {
        return createDeterministicGroupedIntermediateAggregation(confidence, ThreadLocalRandom.current().nextLong());
    }

    @VisibleForTesting
    public Accumulator createDeterministicAggregation(Optional<Integer> maskChannel, int sampleWeightChannel, double confidence, long seed, int... argumentChannels)
    {
        ImmutableList.Builder<Accumulator> builder = ImmutableList.builder();
        for (int i = 0; i < SAMPLES; i++) {
            builder.add(function.createAggregation(maskChannel, Optional.of(sampleWeightChannel), 1.0, argumentChannels));
        }
        return new BootstrappedAccumulator(blockEncodingSerde, builder.build(), sampleWeightChannel, confidence, seed);
    }

    @VisibleForTesting
    public Accumulator createDeterministicIntermediateAggregation(double confidence, long seed)
    {
        ImmutableList.Builder<Accumulator> builder = ImmutableList.builder();
        for (int i = 0; i < SAMPLES; i++) {
            builder.add(function.createIntermediateAggregation(1.0));
        }
        return new BootstrappedAccumulator(blockEncodingSerde, builder.build(), -1, confidence, seed);
    }

    @VisibleForTesting
    public GroupedAccumulator createDeterministicGroupedAggregation(Optional<Integer> maskChannel, int sampleWeightChannel, double confidence, long seed, int... argumentChannels)
    {
        ImmutableList.Builder<GroupedAccumulator> builder = ImmutableList.builder();
        for (int i = 0; i < SAMPLES; i++) {
            builder.add(function.createGroupedAggregation(maskChannel, Optional.of(sampleWeightChannel), 1.0, argumentChannels));
        }
        return new BootstrappedGroupedAccumulator(blockEncodingSerde, builder.build(), sampleWeightChannel, confidence, seed);
    }

    @VisibleForTesting
    public GroupedAccumulator createDeterministicGroupedIntermediateAggregation(double confidence, long seed)
    {
        ImmutableList.Builder<GroupedAccumulator> builder = ImmutableList.builder();
        for (int i = 0; i < SAMPLES; i++) {
            builder.add(function.createGroupedIntermediateAggregation(1.0));
        }
        return new BootstrappedGroupedAccumulator(blockEncodingSerde, builder.build(), -1, confidence, seed);
    }

    public abstract static class AbstractBootstrappedAccumulator
    {
        protected final int sampleWeightChannel;
        protected final double confidence;
        private final RandomDataGenerator rand = new RandomDataGenerator();

        public AbstractBootstrappedAccumulator(int sampleWeightChannel, double confidence, long seed)
        {
            this.sampleWeightChannel = sampleWeightChannel;
            this.confidence = confidence;
            rand.reSeed(seed);
        }

        protected Block resampleWeightBlock(Block block)
        {
            return new PoissonizedBlock(block, rand.nextLong(0, Long.MAX_VALUE - 1));
        }

        protected static double getNumeric(BlockCursor cursor)
        {
            if (cursor.getType().equals(DOUBLE)) {
                return cursor.getDouble();
            }
            else if (cursor.getType().equals(BIGINT)) {
                return cursor.getLong();
            }
            else {
                throw new AssertionError("expected a numeric cursor");
            }
        }
    }

    public static class BootstrappedAccumulator
            extends AbstractBootstrappedAccumulator
            implements Accumulator
    {
        private final BlockEncodingSerde blockEncodingSerde;
        private final List<Accumulator> accumulators;

        public BootstrappedAccumulator(BlockEncodingSerde blockEncodingSerde, List<Accumulator> accumulators, int sampleWeightChannel, double confidence, long seed)
        {
            super(sampleWeightChannel, confidence, seed);
            this.blockEncodingSerde = blockEncodingSerde;

            this.accumulators = ImmutableList.copyOf(checkNotNull(accumulators, "accumulators is null"));
            checkArgument(accumulators.size() > 1, "accumulators size is less than 2");
        }

        @Override
        public Type getFinalType()
        {
            return VARCHAR;
        }

        @Override
        public Type getIntermediateType()
        {
            return VARCHAR;
        }

        @Override
        public void addInput(Page page)
        {
            Block[] blocks = Arrays.copyOf(page.getBlocks(), page.getChannelCount());
            for (int i = 0; i < accumulators.size(); i++) {
                blocks[sampleWeightChannel] = resampleWeightBlock(page.getBlock(sampleWeightChannel));
                accumulators.get(i).addInput(new Page(blocks));
            }
        }

        @Override
        public void addIntermediate(Block block)
        {
            BlockCursor cursor = block.cursor();
            checkArgument(cursor.advanceNextPosition());
            SliceInput sliceInput = new BasicSliceInput(cursor.getSlice());
            Page page = Iterators.getOnlyElement(PagesSerde.readPages(blockEncodingSerde, sliceInput));
            checkArgument(page.getChannelCount() == accumulators.size(), "number of blocks does not match accumulators");

            for (int i = 0; i < page.getChannelCount(); i++) {
                accumulators.get(i).addIntermediate(page.getBlock(i));
            }
        }

        @Override
        public Block evaluateIntermediate()
        {
            Block[] blocks = new Block[accumulators.size()];
            int sizeEstimate = 64 * accumulators.size();
            for (int i = 0; i < accumulators.size(); i++) {
                blocks[i] = accumulators.get(i).evaluateIntermediate();
                sizeEstimate += blocks[i].getSizeInBytes();
            }

            SliceOutput output = new DynamicSliceOutput(sizeEstimate);
            PagesSerde.writePages(blockEncodingSerde, output, new Page(blocks));
            BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());
            builder.append(output.slice());
            return builder.build();
        }

        @Override
        public Block evaluateFinal()
        {
            DescriptiveStatistics statistics = new DescriptiveStatistics();
            for (int i = 0; i < accumulators.size(); i++) {
                BlockCursor cursor = accumulators.get(i).evaluateFinal().cursor();
                checkArgument(cursor.advanceNextPosition(), "accumulator returned no results");
                statistics.addValue(getNumeric(cursor));
            }

            BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());
            String result = formatApproximateOutput(statistics, confidence);
            builder.append(Slices.utf8Slice(result));
            return builder.build();
        }
    }

    public static class BootstrappedGroupedAccumulator
            extends AbstractBootstrappedAccumulator
            implements GroupedAccumulator
    {
        private final BlockEncodingSerde blockEncodingSerde;
        private final List<GroupedAccumulator> accumulators;

        public BootstrappedGroupedAccumulator(BlockEncodingSerde blockEncodingSerde,
                List<GroupedAccumulator> accumulators,
                int sampleWeightChannel,
                double confidence,
                long seed)
        {
            super(sampleWeightChannel, confidence, seed);
            this.blockEncodingSerde = blockEncodingSerde;

            this.accumulators = ImmutableList.copyOf(checkNotNull(accumulators, "accumulators is null"));
            checkArgument(accumulators.size() > 1, "accumulators size is less than 2");
        }

        @Override
        public long getEstimatedSize()
        {
            long size = 0;
            for (GroupedAccumulator accumulator : accumulators) {
                size += accumulator.getEstimatedSize();
            }
            return size;
        }

        @Override
        public Type getFinalType()
        {
            return VARCHAR;
        }

        @Override
        public Type getIntermediateType()
        {
            return VARCHAR;
        }

        @Override
        public void addInput(GroupByIdBlock groupIdsBlock, Page page)
        {
            Block[] blocks = Arrays.copyOf(page.getBlocks(), page.getChannelCount());
            for (int i = 0; i < accumulators.size(); i++) {
                blocks[sampleWeightChannel] = resampleWeightBlock(page.getBlock(sampleWeightChannel));
                accumulators.get(i).addInput(groupIdsBlock, new Page(blocks));
            }
        }

        @Override
        public void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            BlockCursor cursor = block.cursor();
            checkArgument(cursor.advanceNextPosition());
            SliceInput sliceInput = new BasicSliceInput(cursor.getSlice());
            Page page = Iterators.getOnlyElement(PagesSerde.readPages(blockEncodingSerde, sliceInput));
            checkArgument(page.getChannelCount() == accumulators.size(), "number of blocks does not match accumulators");

            for (int i = 0; i < page.getChannelCount(); i++) {
                accumulators.get(i).addIntermediate(groupIdsBlock, page.getBlock(i));
            }
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            Block[] blocks = new Block[accumulators.size()];
            int sizeEstimate = 64 * accumulators.size();
            for (int i = 0; i < accumulators.size(); i++) {
                BlockBuilder builder = accumulators.get(i).getIntermediateType().createBlockBuilder(new BlockBuilderStatus());
                accumulators.get(i).evaluateIntermediate(groupId, builder);
                blocks[i] = builder.build();
                sizeEstimate += blocks[i].getSizeInBytes();
            }

            SliceOutput sliceOutput = new DynamicSliceOutput(sizeEstimate);
            PagesSerde.writePages(blockEncodingSerde, sliceOutput, new Page(blocks));
            output.append(sliceOutput.slice());
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            DescriptiveStatistics statistics = new DescriptiveStatistics();
            for (int i = 0; i < accumulators.size(); i++) {
                BlockBuilder builder = accumulators.get(i).getFinalType().createBlockBuilder(new BlockBuilderStatus());
                accumulators.get(i).evaluateFinal(groupId, builder);
                BlockCursor cursor = builder.build().cursor();
                checkArgument(cursor.advanceNextPosition(), "accumulator returned no results");
                statistics.addValue(getNumeric(cursor));
            }

            String result = formatApproximateOutput(statistics, confidence);
            output.append(Slices.utf8Slice(result));
        }
    }

    private static String formatApproximateOutput(DescriptiveStatistics statistics, double confidence)
    {
        StringBuilder sb = new StringBuilder();
        double p = 100 * (1 + confidence) / 2.0;
        double upper = statistics.getPercentile(p);
        double lower = statistics.getPercentile(100 - p);
        sb.append((upper + lower) / 2.0);
        sb.append(" +/- ");
        double error = (upper - lower) / 2.0;
        checkState(error >= 0, "error is negative");
        sb.append(error);

        return sb.toString();
    }
}
