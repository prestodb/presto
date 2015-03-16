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

import com.facebook.presto.operator.window.FrameInfo;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.operator.window.WindowPartition;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class PrePartitionedWindowOperator
        implements Operator
{
    public static class PrePartitionedWindowOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final List<WindowFunctionDefinition> windowFunctionDefinitions;
        private final List<Integer> partitionChannels;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final FrameInfo frameInfo;
        private final int expectedPositions;
        private final List<Type> types;
        private boolean closed;

        public PrePartitionedWindowOperatorFactory(
                int operatorId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<WindowFunctionDefinition> windowFunctionDefinitions,
                List<Integer> partitionChannels,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                FrameInfo frameInfo,
                int expectedPositions)
        {
            this.operatorId = operatorId;
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.outputChannels = ImmutableList.copyOf(checkNotNull(outputChannels, "outputChannels is null"));
            this.windowFunctionDefinitions = windowFunctionDefinitions;
            this.partitionChannels = ImmutableList.copyOf(checkNotNull(partitionChannels, "partitionChannels is null"));
            this.sortChannels = ImmutableList.copyOf(checkNotNull(sortChannels, "sortChannels is null"));
            this.sortOrder = ImmutableList.copyOf(checkNotNull(sortOrder, "sortOrder is null"));

            this.frameInfo = checkNotNull(frameInfo, "frameInfo is null");

            this.expectedPositions = expectedPositions;

            this.types = Stream.concat(
                    outputChannels.stream()
                            .map(sourceTypes::get),
                    windowFunctionDefinitions.stream()
                            .map(WindowFunctionDefinition::getType))
                    .collect(toImmutableList());
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, PrePartitionedWindowOperator.class.getSimpleName());
            return new PrePartitionedWindowOperator(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    windowFunctionDefinitions,
                    partitionChannels,
                    sortChannels,
                    sortOrder,
                    frameInfo,
                    expectedPositions);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHING,
        FINISHED
    }

    private final OperatorContext operatorContext;
    private final int[] outputChannels;
    private final List<WindowFunction> windowFunctions;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrder;
    private final List<Type> types;

    private final PagesHashStrategy partitionHashStrategy;
    private final PagesHashStrategy peerGroupHashStrategy;

    private final FrameInfo frameInfo;

    private final PagesIndex pagesIndex;

    private final PageBuilder pageBuilder;

    private State state = State.NEEDS_INPUT;

    private WindowPartition partition;

    private Page pendingInput;

    public PrePartitionedWindowOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> windowFunctionDefinitions,
            List<Integer> partitionChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            FrameInfo frameInfo,
            int expectedPositions)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.outputChannels = Ints.toArray(checkNotNull(outputChannels, "outputChannels is null"));
        this.windowFunctions = checkNotNull(windowFunctionDefinitions, "windowFunctionDefinitions is null").stream()
                .map(WindowFunctionDefinition::createWindowFunction)
                .collect(toImmutableList());
        this.sortChannels = ImmutableList.copyOf(checkNotNull(sortChannels, "sortChannels is null"));
        this.sortOrder = ImmutableList.copyOf(checkNotNull(sortOrder, "sortOrder is null"));
        this.frameInfo = checkNotNull(frameInfo, "frameInfo is null");

        this.types = Stream.concat(
                outputChannels.stream()
                        .map(sourceTypes::get),
                windowFunctions.stream()
                        .map(WindowFunction::getType))
                .collect(toImmutableList());

        this.pagesIndex = new PagesIndex(sourceTypes, expectedPositions);
        this.partitionHashStrategy = pagesIndex.createPagesHashStrategy(partitionChannels, operatorContext, Optional.<Integer>empty());
        this.peerGroupHashStrategy = pagesIndex.createPagesHashStrategy(sortChannels, operatorContext, Optional.<Integer>empty());

        this.pageBuilder = new PageBuilder(this.types);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        if (state == State.FINISHING || state == State.FINISHED) {
            return;
        }

        state = State.FINISHING;

        // if we do not have an open partition, open a partition with the pending input
        if (partition == null) {
            processPendingInput();
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
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(state == State.NEEDS_INPUT, "Operator is already finishing");
        checkNotNull(page, "page is null");
        checkState(pendingInput == null, "Operator already has pending input");

        if (page.getPositionCount() == 0) {
            return;
        }

        pendingInput = page;
        processPendingInput();

        operatorContext.setMemoryReservation(pagesIndex.getEstimatedSize().toBytes());
    }

    public void processPendingInput()
    {
        checkState(partition == null, "Operator is already processing output");

        if (pendingInput != null) {
            // find the section of the pending page to add to the open partition (or to start a new partition)
            if (pagesIndex.getPositionCount() > 0 && pagesIndex.positionEqualsRow(partitionHashStrategy, 0, pendingInput.getPositionCount() - 1, pendingInput.getBlocks())) {
                // whole page extends the existing partition
                pagesIndex.addPage(pendingInput);
                pendingInput = null;
            }
            else if (pagesIndex.getPositionCount() == 0 || pagesIndex.positionEqualsRow(partitionHashStrategy, 0, 0, pendingInput.getBlocks())) {
                // find the position where the partition changes
                int partitionEnd = 1;
                while (partitionEnd < pendingInput.getPositionCount() &&
                        partitionHashStrategy.rowEqualsRow(partitionEnd - 1, pendingInput.getBlocks(), partitionEnd, pendingInput.getBlocks())) {
                    partitionEnd++;
                }

                // add the section of the page that contains values for the current partition
                pagesIndex.addPage(pendingInput.getRegion(0, partitionEnd));

                // save the remaining page, which may contain multiple partitions
                if (pendingInput.getPositionCount() - partitionEnd > 0) {
                    pendingInput = pendingInput.getRegion(partitionEnd, pendingInput.getPositionCount() - partitionEnd);
                }
                else {
                    pendingInput = null;
                }
            }
        }

        // if we have a complete partition, prepare the output
        if ((pendingInput != null || state == State.FINISHING) && (pagesIndex.getPositionCount() != 0)) {
            // sort the index if necessary
            if (!sortChannels.isEmpty()) {
                pagesIndex.sort(sortChannels, sortOrder);
            }

            // we have a single partition in the pages index
            partition = new WindowPartition(pagesIndex, 0, pagesIndex.getPositionCount(), outputChannels, windowFunctions, frameInfo, peerGroupHashStrategy);

            if (state == State.NEEDS_INPUT) {
                state = State.HAS_OUTPUT;
            }
        }
    }

    @Override
    public Page getOutput()
    {
        if (state == State.NEEDS_INPUT || state == State.FINISHED) {
            return null;
        }

        // iterate through the positions sequentially until we have one full page
        pageBuilder.reset();
        while (!pageBuilder.isFull() && partition != null && partition.hasNext()) {
            partition.processNextRow(pageBuilder);
        }

        // return output if any
        if (!pageBuilder.isEmpty()) {
            return pageBuilder.build();
        }

        // partition is complete

        // reset partition and pages index
        partition = null;
        pagesIndex.clear();
        operatorContext.setMemoryReservation(pagesIndex.getEstimatedSize().toBytes());

        // prepare next partition
        if (state == State.HAS_OUTPUT) {
            state = State.NEEDS_INPUT;
        }
        processPendingInput();

        // check finished state
        if (partition == null && state == State.FINISHING) {
            state = State.FINISHED;
        }

        return null;
    }
}
