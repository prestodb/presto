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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;

public class WindowOperator
        implements Operator
{
    public static class WindowOperatorFactory
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

        public WindowOperatorFactory(
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

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, WindowOperator.class.getSimpleName());
            return new WindowOperator(
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
        FINISHED
    }

    private final OperatorContext operatorContext;
    private final int[] outputChannels;
    private final List<WindowFunction> windowFunctions;
    private final List<Integer> partitionChannels;
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

    public WindowOperator(
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
        this.partitionChannels = ImmutableList.copyOf(checkNotNull(partitionChannels, "partitionChannels is null"));
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
        this.partitionHashStrategy = pagesIndex.createPagesHashStrategy(partitionChannels, operatorContext, Optional.empty());
        this.peerGroupHashStrategy = pagesIndex.createPagesHashStrategy(sortChannels, operatorContext, Optional.empty());

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
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;

            // we partition by ordering the values so partitions are sequential values
            List<SortOrder> partitionOrder = Collections.nCopies(partitionChannels.size(), ASC_NULLS_LAST);

            // sort everything by partition channels, then sort channels
            List<Integer> orderChannels = ImmutableList.copyOf(concat(partitionChannels, sortChannels));
            List<SortOrder> ordering = ImmutableList.copyOf(concat(partitionOrder, sortOrder));

            // sort the index
            pagesIndex.sort(orderChannels, ordering);
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

        pagesIndex.addPage(page);
        operatorContext.setMemoryReservation(pagesIndex.getEstimatedSize().toBytes());
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        // iterate through the positions sequentially until we have one full page
        pageBuilder.reset();
        while (!pageBuilder.isFull()) {
            // check for new partition
            if (partition == null || !partition.hasNext()) {
                // the next partition starts as the end of the current partition
                int partitionStart = partition == null ? 0 : partition.getPartitionEnd();

                // are we at the end?
                if (partitionStart >= pagesIndex.getPositionCount()) {
                    break;
                }

                // find partition end
                int partitionEnd = partitionStart + 1;
                while ((partitionEnd < pagesIndex.getPositionCount()) && pagesIndex.positionEqualsPosition(partitionHashStrategy, partitionStart, partitionEnd)) {
                    partitionEnd++;
                }

                partition = new WindowPartition(pagesIndex, partitionStart, partitionEnd, outputChannels, windowFunctions, frameInfo, peerGroupHashStrategy);
            }

            partition.processNextRow(pageBuilder);
        }

        // output the page if we have any data
        if (pageBuilder.isEmpty()) {
            state = State.FINISHED;
            return null;
        }

        Page page = pageBuilder.build();
        return page;
    }
}
