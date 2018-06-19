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

import com.facebook.presto.Session;
import com.facebook.presto.operator.MarkDistinctHash;
import com.facebook.presto.operator.UpdateMemory;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.project.SelectedPositions;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.ByteArrayBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.stream.IntStream.range;

public class DistinctInternalAccumulatorFactory
        implements InternalAccumulatorFactory
{
    private final AccumulatorFactory accumulatorFactory;
    private final List<Type> argumentTypes;
    private final Type finalType;
    private final List<Integer> argumentChannels;
    private final Optional<Integer> maskChannel;
    private final Session session;

    private final JoinCompiler joinCompiler;

    public DistinctInternalAccumulatorFactory(
            AccumulatorFactory accumulatorFactory,
            List<Type> argumentTypes,
            List<Integer> argumentChannels,
            Optional<Integer> maskChannel,
            Session session,
            JoinCompiler joinCompiler)
    {
        this.accumulatorFactory = requireNonNull(accumulatorFactory, "accumulatorFactory is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        this.argumentChannels = ImmutableList.copyOf(requireNonNull(argumentChannels, "argumentChannels is null"));
        this.finalType = accumulatorFactory.createAccumulator().getFinalType();
        this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");
        this.session = requireNonNull(session, "session is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        verify(accumulatorFactory.getInputChannels().size() == argumentTypes.size());
    }

    @Override
    public Type getIntermediateType()
    {
        return BOOLEAN;
    }

    @Override
    public Type getFinalType()
    {
        return finalType;
    }

    @Override
    public boolean isDistinct()
    {
        return true;
    }

    @Override
    public InternalSingleAccumulator createSingleAccumulator()
    {
        return new DistinctInternalFinalAccumulator(accumulatorFactory.createAccumulator(), argumentTypes, argumentChannels, maskChannel, session, joinCompiler);
    }

    @Override
    public InternalPartialAccumulator createPartialAccumulator()
    {
        return new DistinctInternalPartialAccumulator(argumentTypes, argumentChannels, maskChannel, session, joinCompiler);
    }

    @Override
    public InternalIntermediateAccumulator createIntermediateAccumulator()
    {
        return new DistinctInternalPartialAccumulator(argumentTypes, argumentChannels, maskChannel, session, joinCompiler);
    }

    @Override
    public InternalFinalAccumulator createFinalAccumulator()
    {
        return new DistinctInternalFinalAccumulator(accumulatorFactory.createAccumulator(), argumentTypes, argumentChannels, maskChannel, session, joinCompiler);
    }

    public static class DistinctInternalPartialAccumulator
            implements InternalPartialAccumulator, InternalIntermediateAccumulator
    {
        private final Supplier<MarkDistinctHash> hashSupplier;
        private MarkDistinctHash hash;
        private final List<Integer> argumentChannels;
        private final Optional<Integer> maskChannel;

        public DistinctInternalPartialAccumulator(
                List<Type> argumentTypes,
                List<Integer> argumentChannels,
                Optional<Integer> maskChannel,
                Session session,
                JoinCompiler joinCompiler)
        {
            this.argumentChannels = ImmutableList.copyOf(requireNonNull(argumentChannels, "argumentChannels is null"));
            this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");

            hashSupplier = () -> new MarkDistinctHash(session, argumentTypes, range(0, argumentTypes.size()).toArray(), Optional.empty(), 1, joinCompiler, UpdateMemory.NOOP);
            hash = hashSupplier.get();
        }

        @Override
        public long getEstimatedSize()
        {
            return hash.getEstimatedSize();
        }

        @Override
        public Type getIntermediateType()
        {
            return BOOLEAN;
        }

        @Override
        public boolean isDistinct()
        {
            return true;
        }

        @Override
        public Optional<Block> addInput(Page page)
        {
            // 1. mask input to based on mask channel
            SelectedPositions selectedPositions = maskChannel
                    .map(page::getBlock)
                    .map(DistinctInternalAccumulatorFactory::booleanBlockToSelectedPositions)
                    .orElse(SelectedPositions.positionsRange(0, page.getPositionCount()));
            if (selectedPositions.isEmpty()) {
                return Optional.empty();
            }
            Page masked = selectedPositions.selectPositions(page);

            // 2. determine which of the selected positions are distinct
            Page hashChannels = new Page(masked.getPositionCount(), argumentChannels.stream().map(masked::getBlock).toArray(Block[]::new));
            Work<Block> work = hash.markDistinctRows(hashChannels);
            checkState(work.process());
            Block distinctSelectedPositions = work.getResult();

            // 3. return boolean block of the distinct positions
            return buildDistinctSelectedPositionsMask(page.getPositionCount(), selectedPositions, distinctSelectedPositions);
        }

        @Override
        public void addIntermediate(Block block)
        {
            for (int position = 0; position < block.getPositionCount(); position++) {
                verify(block.isNull(position), "Distinct accumulator does not have intermediate results");
            }
        }

        @Override
        public void evaluateIntermediate(BlockBuilder blockBuilder)
        {
            blockBuilder.appendNull();
        }

        @Override
        public void flush()
        {
            hash = hashSupplier.get();
        }
    }

    public static class DistinctInternalFinalAccumulator
            implements InternalFinalAccumulator, InternalSingleAccumulator
    {
        private final Accumulator accumulator;
        private final MarkDistinctHash hash;
        private final List<Integer> argumentChannels;
        private final Optional<Integer> maskChannel;

        public DistinctInternalFinalAccumulator(
                Accumulator accumulator,
                List<Type> argumentTypes,
                List<Integer> argumentChannels,
                Optional<Integer> maskChannel,
                Session session,
                JoinCompiler joinCompiler)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.argumentChannels = ImmutableList.copyOf(requireNonNull(argumentChannels, "argumentChannels is null"));
            this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");

            hash = new MarkDistinctHash(session, argumentTypes, range(0, argumentTypes.size()).toArray(), Optional.empty(), joinCompiler, UpdateMemory.NOOP);
        }

        @Override
        public long getEstimatedSize()
        {
            return hash.getEstimatedSize() + accumulator.getEstimatedSize();
        }

        @Override
        public Type getIntermediateType()
        {
            return BOOLEAN;
        }

        @Override
        public Type getFinalType()
        {
            return accumulator.getFinalType();
        }

        @Override
        public boolean isDistinct()
        {
            return true;
        }

        @Override
        public void addInput(Page page)
        {
            // 1. mask input rows based on mask channel
            SelectedPositions selectedPositions = maskChannel
                    .map(page::getBlock)
                    .map(DistinctInternalAccumulatorFactory::booleanBlockToSelectedPositions)
                    .orElse(SelectedPositions.positionsRange(0, page.getPositionCount()));
            Page masked = selectedPositions.selectPositions(page);
            if (masked.getPositionCount() == 0) {
                return;
            }

            // 2. determine which of the selected positions are distinct
            Page hashChannels = masked.selectChannels(argumentChannels);
            Work<Block> work = hash.markDistinctRows(hashChannels);
            checkState(work.process());
            Block distinctSelectedPositions = work.getResult();

            // 3. add distinct positions to the accumulator
            Page distinct = filter(masked, distinctSelectedPositions);
            if (distinct.getPositionCount() == 0) {
                return;
            }
            accumulator.addInput(distinct);
        }

        private static Page filter(Page page, Block mask)
        {
            int[] ids = new int[mask.getPositionCount()];
            int next = 0;
            for (int i = 0; i < page.getPositionCount(); ++i) {
                if (BOOLEAN.getBoolean(mask, i)) {
                    ids[next++] = i;
                }
            }

            return page.getPositions(ids, 0, next);
        }

        @Override
        public void addIntermediate(Block block)
        {
            for (int position = 0; position < block.getPositionCount(); position++) {
                verify(block.isNull(position), "Distinct accumulator does not have intermediate results");
            }
        }

        @Override
        public void evaluateFinal(BlockBuilder blockBuilder)
        {
            accumulator.evaluateFinal(blockBuilder);
        }
    }

    private static SelectedPositions booleanBlockToSelectedPositions(Block mask)
    {
        int[] ids = new int[mask.getPositionCount()];
        int next = 0;
        for (int i = 0; i < mask.getPositionCount(); ++i) {
            if (BOOLEAN.getBoolean(mask, i)) {
                ids[next++] = i;
            }
        }

        return SelectedPositions.positionsList(ids, 0, next);
    }

    private static Optional<Block> buildDistinctSelectedPositionsMask(int inputPositionCount, SelectedPositions selectedPositions, Block distinctSelectedPositions)
    {
        boolean hasDistinct = false;
        byte[] distinct = new byte[inputPositionCount];
        for (int filteredPosition = 0; filteredPosition < distinctSelectedPositions.getPositionCount(); filteredPosition++) {
            if (BOOLEAN.getBoolean(distinctSelectedPositions, filteredPosition)) {
                int originalPosition = selectedPositions.getSelectedPosition(filteredPosition);
                verify(originalPosition < inputPositionCount);
                hasDistinct = true;
                distinct[originalPosition] = 1;
            }
        }

        return hasDistinct ? Optional.of(new ByteArrayBlock(distinct.length, new boolean[distinct.length], distinct)) : Optional.empty();
    }
}
