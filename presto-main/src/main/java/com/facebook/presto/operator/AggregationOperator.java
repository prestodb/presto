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
package com.facebook.presto.operator;

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.aggregation.InternalAccumulatorFactory;
import com.facebook.presto.operator.aggregation.InternalAccumulatorFactory.InternalFinalAccumulator;
import com.facebook.presto.operator.aggregation.InternalAccumulatorFactory.InternalIntermediateAccumulator;
import com.facebook.presto.operator.aggregation.InternalAccumulatorFactory.InternalPartialAccumulator;
import com.facebook.presto.operator.aggregation.InternalAccumulatorFactory.InternalSingleAccumulator;
import com.facebook.presto.operator.project.SelectedPositions;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class AggregationOperator
        implements Operator
{
    public static class AggregationInputChannel
    {
        private final Symbol symbol;
        private final int channel;
        private final Type type;

        public AggregationInputChannel(Symbol symbol, int channel, Type type)
        {
            this.symbol = requireNonNull(symbol, "symbol is null");
            checkArgument(channel >= 0, "channel is negative");
            this.channel = channel;
            this.type = requireNonNull(type, "type is null");
        }

        public Symbol getSymbol()
        {
            return symbol;
        }

        public int getChannel()
        {
            return channel;
        }

        public Type getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("symbol", symbol)
                    .add("channel", channel)
                    .add("type", type)
                    .toString();
        }
    }

    public static class AggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Step step;
        private final List<InternalAccumulatorFactory> accumulatorFactories;
        private final List<AggregationInputChannel> inputs;
        private final DataSize maxPartialMemory;
        private boolean closed;

        public AggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Step step,
                List<InternalAccumulatorFactory> accumulatorFactories,
                List<AggregationInputChannel> inputs,
                DataSize maxPartialMemory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.step = step;
            this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
            this.inputs = ImmutableList.copyOf(requireNonNull(inputs, "inputs is null"));
            this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, AggregationOperator.class.getSimpleName());
            return new AggregationOperator(operatorContext, step, accumulatorFactories, inputs, maxPartialMemory);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new AggregationOperatorFactory(operatorId, planNodeId, step, accumulatorFactories, inputs, maxPartialMemory);
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        FINISHING,
        FINISHED
    }

    private final OperatorContext operatorContext;
    private final Processor processor;

    private Optional<Page> partialOutput = Optional.empty();
    private State state = State.NEEDS_INPUT;

    public AggregationOperator(
            OperatorContext operatorContext,
            Step step,
            List<InternalAccumulatorFactory> accumulatorFactories,
            List<AggregationInputChannel> inputs,
            DataSize maxPartialMemory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        IntermediateLayout intermediateLayout = new IntermediateLayout(accumulatorFactories, inputs);
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        requireNonNull(step, "step is null");
        switch (step) {
            case SINGLE:
                processor = new SingleProcessor(operatorContext.localUserMemoryContext(), accumulatorFactories);
                break;
            case PARTIAL:
                processor = new PartialProcessor(operatorContext.newLocalSystemMemoryContext(), accumulatorFactories, intermediateLayout, maxPartialMemory);
                break;
            case INTERMEDIATE:
                processor = new IntermediateProcessor(operatorContext.newLocalSystemMemoryContext(), accumulatorFactories, intermediateLayout, maxPartialMemory);
                break;
            case FINAL:
                processor = new FinalProcessor(operatorContext.localUserMemoryContext(), accumulatorFactories, intermediateLayout);
                break;
            default:
                throw new IllegalArgumentException("Unsupported step " + step);
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (state == State.NEEDS_INPUT) {
            state = State.FINISHING;
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT && !partialOutput.isPresent();
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput(), "Operator is already finishing");
        checkState(!partialOutput.isPresent());
        requireNonNull(page, "page is null");

        partialOutput = processor.addInput(page);
    }

    @Override
    public Page getOutput()
    {
        if (partialOutput.isPresent()) {
            Page output = partialOutput.get();
            partialOutput = Optional.empty();
            return output;
        }

        if (state != State.FINISHING) {
            return null;
        }

        state = State.FINISHED;
        return processor.getFinalOutput();
    }

    private interface Processor
    {
        Optional<Page> addInput(Page page);

        Page getFinalOutput();
    }

    private static class SingleProcessor
            implements Processor
    {
        private final LocalMemoryContext userMemoryContext;
        private final List<InternalSingleAccumulator> accumulators;

        public SingleProcessor(LocalMemoryContext userMemoryContext, List<InternalAccumulatorFactory> accumulatorFactories)
        {
            this.userMemoryContext = userMemoryContext;
            this.accumulators = accumulatorFactories.stream()
                    .map(InternalAccumulatorFactory::createSingleAccumulator)
                    .collect(toImmutableList());
        }

        @Override
        public Optional<Page> addInput(Page inputPage)
        {
            long memorySize = 0;
            for (InternalSingleAccumulator accumulator : accumulators) {
                accumulator.addInput(inputPage);
                memorySize += accumulator.getEstimatedSize();
            }
            userMemoryContext.setBytes(memorySize);
            return Optional.empty();
        }

        @Override
        public Page getFinalOutput()
        {
            List<Block> finalResults = new ArrayList<>();
            for (InternalSingleAccumulator accumulator : accumulators) {
                BlockBuilder blockBuilder = accumulator.getFinalType().createBlockBuilder(null, 1);
                accumulator.evaluateFinal(blockBuilder);
                finalResults.add(blockBuilder.build());
            }
            return new Page(1, finalResults.stream().toArray(Block[]::new));
        }
    }

    private static class PartialProcessor
            implements Processor
    {
        private final LocalMemoryContext systemMemoryContext;
        private final List<InternalPartialAccumulator> accumulators;
        private final IntermediateLayout intermediateLayout;
        private final long maxPartialMemory;

        public PartialProcessor(
                LocalMemoryContext systemMemoryContext,
                List<InternalAccumulatorFactory> accumulatorFactories,
                IntermediateLayout intermediateLayout,
                DataSize maxPartialMemory)
        {
            this.systemMemoryContext = systemMemoryContext;
            this.accumulators = accumulatorFactories.stream()
                    .map(InternalAccumulatorFactory::createPartialAccumulator)
                    .collect(toImmutableList());
            this.intermediateLayout = intermediateLayout;
            this.maxPartialMemory = maxPartialMemory.toBytes();
        }

        @Override
        public Optional<Page> addInput(Page inputPage)
        {
            List<Optional<Block>> partialDistinctMasks = new ArrayList<>(accumulators.size());
            for (InternalPartialAccumulator accumulator : accumulators) {
                partialDistinctMasks.add(accumulator.addInput(inputPage));
            }

            updateMemoryUsage();

            if (!intermediateLayout.hasDistinct()) {
                return Optional.empty();
            }
            return intermediateLayout.buildPartialDistinctRows(inputPage, partialDistinctMasks);
        }

        @Override
        public Page getFinalOutput()
        {
            List<Block> intermediateResults = new ArrayList<>();
            for (InternalPartialAccumulator accumulator : accumulators) {
                BlockBuilder blockBuilder = accumulator.getIntermediateType().createBlockBuilder(null, 1);
                accumulator.evaluateIntermediate(blockBuilder);
                intermediateResults.add(blockBuilder.build());
            }
            return intermediateLayout.buildIntermediateOutput(intermediateResults);
        }

        private void updateMemoryUsage()
        {
            long memorySize = accumulators.stream()
                    .mapToLong(InternalPartialAccumulator::getEstimatedSize)
                    .sum();
            if (memorySize > maxPartialMemory) {
                List<InternalPartialAccumulator> reverseFlushOrder = accumulators.stream()
                        .sorted(Comparator.comparing(InternalPartialAccumulator::getEstimatedSize))
                        .collect(toList());
                do {
                    // if there is nothing left to flush, just set the memory directly and it should work or fail
                    if (reverseFlushOrder.isEmpty()) {
                        break;
                    }

                    InternalPartialAccumulator accumulatorToFlush = reverseFlushOrder.remove(reverseFlushOrder.size() - 1);
                    memorySize -= accumulatorToFlush.getEstimatedSize();
                    accumulatorToFlush.flush();
                    memorySize += accumulatorToFlush.getEstimatedSize();
                } while (memorySize > maxPartialMemory);
            }
            systemMemoryContext.setBytes(memorySize);
        }
    }

    private static class IntermediateProcessor
            implements Processor
    {
        private final LocalMemoryContext systemMemoryContext;
        private final List<InternalIntermediateAccumulator> accumulators;
        private final IntermediateLayout intermediateLayout;
        private final long maxPartialMemory;

        public IntermediateProcessor(
                LocalMemoryContext systemMemoryContext,
                List<InternalAccumulatorFactory> accumulatorFactories,
                IntermediateLayout intermediateLayout,
                DataSize maxPartialMemory)
        {
            this.systemMemoryContext = systemMemoryContext;
            this.accumulators = accumulatorFactories.stream()
                    .map(InternalAccumulatorFactory::createIntermediateAccumulator)
                    .collect(toImmutableList());
            this.intermediateLayout = intermediateLayout;
            this.maxPartialMemory = maxPartialMemory.toBytes();
        }

        @Override
        public Optional<Page> addInput(Page intermediatePage)
        {
            Optional<Page> partialOutput = Optional.empty();
            if (intermediateLayout.hasDistinct()) {
                Page distinctInput = RowType.DISTINCT_PARTIAL.extractPositions(intermediatePage);
                if (distinctInput.getPositionCount() > 0) {
                    List<Optional<Block>> partialDistinctMasks = new ArrayList<>(accumulators.size());
                    for (InternalIntermediateAccumulator accumulator : accumulators) {
                        if (accumulator.isDistinct()) {
                            partialDistinctMasks.add(accumulator.addInput(distinctInput));
                        }
                    }
                    partialOutput = intermediateLayout.buildPartialDistinctRows(distinctInput, partialDistinctMasks);
                }
            }

            if (intermediateLayout.hasNonDistinct()) {
                Page intermediateInput = RowType.INTERMEDIATE.extractPositions(intermediatePage);
                if (intermediateInput.getPositionCount() > 0) {
                    for (int intermediateIndex = 0; intermediateIndex < accumulators.size(); intermediateIndex++) {
                        InternalIntermediateAccumulator accumulator = accumulators.get(intermediateIndex);
                        if (!accumulator.isDistinct()) {
                            Block intermediateInputBlock = intermediateInput.getBlock(intermediateLayout.getIntermediateChannel(intermediateIndex));
                            accumulator.addIntermediate(intermediateInputBlock);
                        }
                    }
                }
            }

            updateMemoryUsage();

            return partialOutput;
        }

        @Override
        public Page getFinalOutput()
        {
            List<Block> intermediateResults = new ArrayList<>();
            for (InternalIntermediateAccumulator accumulator : accumulators) {
                BlockBuilder blockBuilder = accumulator.getIntermediateType().createBlockBuilder(null, 1);
                accumulator.evaluateIntermediate(blockBuilder);
                intermediateResults.add(blockBuilder.build());
            }

            return intermediateLayout.buildIntermediateOutput(intermediateResults);
        }

        private void updateMemoryUsage()
        {
            long memorySize = accumulators.stream()
                    .mapToLong(InternalIntermediateAccumulator::getEstimatedSize)
                    .sum();
            if (memorySize > maxPartialMemory) {
                List<InternalIntermediateAccumulator> reverseFlushOrder = accumulators.stream()
                        .sorted(Comparator.comparing(InternalIntermediateAccumulator::getEstimatedSize))
                        .collect(toList());
                do {
                    // if there is nothing left to flush, just set the memory directly and it should work or fail
                    if (reverseFlushOrder.isEmpty()) {
                        break;
                    }

                    InternalIntermediateAccumulator accumulatorToFlush = reverseFlushOrder.remove(reverseFlushOrder.size() - 1);
                    memorySize -= accumulatorToFlush.getEstimatedSize();
                    accumulatorToFlush.flush();
                    memorySize += accumulatorToFlush.getEstimatedSize();
                } while (memorySize > maxPartialMemory);
            }
            systemMemoryContext.setBytes(memorySize);
        }
    }

    private static class FinalProcessor
            implements Processor
    {
        private final LocalMemoryContext userMemoryContext;
        private final List<InternalFinalAccumulator> accumulators;
        private final IntermediateLayout intermediateLayout;

        public FinalProcessor(LocalMemoryContext userMemoryContext, List<InternalAccumulatorFactory> accumulatorFactories, IntermediateLayout intermediateLayout)
        {
            this.userMemoryContext = userMemoryContext;
            this.accumulators = accumulatorFactories.stream()
                    .map(InternalAccumulatorFactory::createFinalAccumulator)
                    .collect(toImmutableList());
            this.intermediateLayout = intermediateLayout;
        }

        @Override
        public Optional<Page> addInput(Page intermediateInput)
        {
            if (intermediateLayout.hasDistinct()) {
                Page distinctRows = RowType.DISTINCT_PARTIAL.extractPositions(intermediateInput);
                if (distinctRows.getPositionCount() > 0) {
                    for (InternalFinalAccumulator accumulator : accumulators) {
                        if (accumulator.isDistinct()) {
                            accumulator.addInput(distinctRows);
                        }
                    }
                }
            }

            if (intermediateLayout.hasNonDistinct()) {
                Page intermediateRows = RowType.INTERMEDIATE.extractPositions(intermediateInput);
                if (intermediateRows.getPositionCount() > 0) {
                    for (int intermediateIndex = 0; intermediateIndex < accumulators.size(); intermediateIndex++) {
                        InternalFinalAccumulator accumulator = accumulators.get(intermediateIndex);
                        if (!accumulator.isDistinct()) {
                            Block intermediateInputBlock = intermediateInput.getBlock(intermediateLayout.getIntermediateChannel(intermediateIndex));
                            accumulator.addIntermediate(intermediateInputBlock);
                        }
                    }
                }
            }

            long memorySize = 0;
            for (InternalFinalAccumulator accumulator : accumulators) {
                memorySize += accumulator.getEstimatedSize();
            }
            userMemoryContext.setBytes(memorySize);
            return Optional.empty();
        }

        @Override
        public Page getFinalOutput()
        {
            List<Block> finalResults = new ArrayList<>();
            for (InternalFinalAccumulator accumulator : accumulators) {
                BlockBuilder blockBuilder = accumulator.getFinalType().createBlockBuilder(null, 1);
                accumulator.evaluateFinal(blockBuilder);
                finalResults.add(blockBuilder.build());
            }
            return new Page(1, finalResults.stream().toArray(Block[]::new));
        }
    }

    private static class IntermediateLayout
    {
        private static final Block FALSE_SINGLE_VALUE_BLOCK;

        static {
            BlockBuilder falseBlockBuilder = BOOLEAN.createBlockBuilder(null, 1);
            BOOLEAN.writeBoolean(falseBlockBuilder, false);
            FALSE_SINGLE_VALUE_BLOCK = falseBlockBuilder.build();
        }

        private final int intermediateIsDistinctCount;
        private final List<Boolean> intermediateIsDistinct;
        private final List<Type> intermediateTypes;
        private final List<AggregationInputChannel> inputs;
        private final List<Block> inputSingleNullBlocks;

        public IntermediateLayout(List<InternalAccumulatorFactory> accumulatorFactories, List<AggregationInputChannel> inputs)
        {
            requireNonNull(accumulatorFactories, "accumulatorFactories is null");
            requireNonNull(inputs, "inputs is null");
            this.intermediateIsDistinctCount = (int) accumulatorFactories.stream()
                    .filter(InternalAccumulatorFactory::isDistinct)
                    .count();
            this.intermediateIsDistinct = accumulatorFactories.stream()
                    .map(InternalAccumulatorFactory::isDistinct)
                    .collect(toImmutableList());
            this.intermediateTypes = accumulatorFactories.stream()
                    .map(InternalAccumulatorFactory::getIntermediateType)
                    .collect(toImmutableList());

            this.inputs = ImmutableList.copyOf(requireNonNull(inputs, "inputs is null"));
            this.inputSingleNullBlocks = accumulatorFactories.stream()
                    .map(accumulatorFactory -> accumulatorFactory.getIntermediateType().createBlockBuilder(null, 1)
                            .appendNull()
                            .build())
                    .collect(toImmutableList());
        }

        private int getOutputChannelCount()
        {
            return 1 + intermediateTypes.size() + inputs.size();
        }

        public boolean hasDistinct()
        {
            return intermediateIsDistinctCount > 0;
        }

        public boolean hasNonDistinct()
        {
            return intermediateIsDistinctCount < intermediateIsDistinct.size();
        }

        private int getIntermediateChannel(int accumulatorIndex)
        {
            checkElementIndex(accumulatorIndex, intermediateTypes.size());
            return 1 + accumulatorIndex;
        }

        private Optional<Page> buildPartialDistinctRows(Page page, List<Optional<Block>> partialDistinctMasks)
        {
            checkArgument(partialDistinctMasks.size() == intermediateTypes.size());
            verify(partialDistinctMasks.stream().filter(Optional::isPresent).allMatch(mask -> mask.get().getPositionCount() == page.getPositionCount()));

            // check if any positions are distinct, this shouldn't happen since functions shouldn't return an empty mask
            SelectedPositions allDistinctPositions = combineDistinctMasks(partialDistinctMasks);
            if (allDistinctPositions.isEmpty()) {
                return Optional.empty();
            }

            List<Block> blocks = new ArrayList<>(getOutputChannelCount());

            // add row type
            blocks.add(RowType.DISTINCT_PARTIAL.createRowTypeBlock(page.getPositionCount()));

            // add intermediates
            for (int intermediateIndex = 0; intermediateIndex < partialDistinctMasks.size(); intermediateIndex++) {
                Optional<Block> partialDistinctMask = partialDistinctMasks.get(intermediateIndex);
                if (partialDistinctMask.isPresent()) {
                    blocks.add(partialDistinctMask.get());
                }
                else if (intermediateIsDistinct.get(intermediateIndex)) {
                    blocks.add(new RunLengthEncodedBlock(FALSE_SINGLE_VALUE_BLOCK, page.getPositionCount()));
                }
                else {
                    blocks.add(new RunLengthEncodedBlock(inputSingleNullBlocks.get(intermediateIndex), page.getPositionCount()));
                }
            }

            // add inputs
            inputs.stream()
                    .map(AggregationInputChannel::getChannel)
                    .map(page::getBlock)
                    .forEach(blocks::add);

            Page outputPage = new Page(page.getPositionCount(), blocks.stream().toArray(Block[]::new));

            // filter output page to selected positions
            outputPage = allDistinctPositions.selectPositions(outputPage);
            return Optional.of(outputPage);
        }

        private static SelectedPositions combineDistinctMasks(List<Optional<Block>> partialDistinctMasks)
        {
            List<Block> presentMasks = partialDistinctMasks.stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toImmutableList());
            if (presentMasks.isEmpty()) {
                return SelectedPositions.empty();
            }

            int[] distinctPositions = new int[presentMasks.get(0).getPositionCount()];
            int distinctCount = 0;
            for (int position = 0; position < distinctPositions.length; position++) {
                boolean selected = false;
                for (Block presentMask : presentMasks) {
                    if (BOOLEAN.getBoolean(presentMask, position)) {
                        selected = true;
                        break;
                    }
                }
                if (selected) {
                    distinctPositions[distinctCount++] = position;
                }
            }
            return SelectedPositions.positionsList(distinctPositions, 0, distinctCount);
        }

        private Page buildIntermediateOutput(List<Block> intermediateResults)
        {
            List<Block> blocks = new ArrayList<>(getOutputChannelCount());
            blocks.add(RowType.INTERMEDIATE.createRowTypeBlock(1));
            blocks.addAll(intermediateResults);
            blocks.addAll(inputSingleNullBlocks);
            return new Page(1, blocks.stream().toArray(Block[]::new));
        }
    }

    private enum RowType
    {
        INPUT((byte) 1),
        INTERMEDIATE((byte) 2),
        DISTINCT_PARTIAL((byte) 3);

        private final byte value;
        private final Block singleValueBlock;

        RowType(byte value)
        {
            this.value = value;
            BlockBuilder blockBuilder = TINYINT.createBlockBuilder(null, 1);
            TINYINT.writeLong(blockBuilder, value);
            this.singleValueBlock = blockBuilder.build();
        }

        public Block createRowTypeBlock(int positionCount)
        {
            if (positionCount == 1) {
                return singleValueBlock;
            }
            return new RunLengthEncodedBlock(singleValueBlock, positionCount);
        }

        public Page extractPositions(Page page)
        {
            int[] selectedPositions = new int[page.getPositionCount()];
            Block rowTypeBlock = page.getBlock(0);
            int next = 0;
            for (int position = 0; position < selectedPositions.length; position++) {
                if (TINYINT.getLong(rowTypeBlock, position) == value) {
                    selectedPositions[next++] = position;
                }
            }
            return page.getPositions(selectedPositions, 0, next);
        }
    }
}
