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

import com.facebook.presto.operator.simple.OperatorBuilder;
import com.facebook.presto.operator.simple.ProcessorBase;
import com.facebook.presto.operator.simple.ProcessorInput;
import com.facebook.presto.operator.simple.ProcessorPageBuilderOutput;
import com.facebook.presto.operator.simple.SimpleOperator;
import com.facebook.presto.operator.window.FrameInfo;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.operator.window.WindowPartition;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.PageCompiler;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.operator.simple.SimpleOperator.ProcessorState.HAS_OUTPUT;
import static com.facebook.presto.operator.simple.SimpleOperator.ProcessorState.NEEDS_INPUT;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class WindowOperatorFactory
        implements OperatorFactory
{
    private final int operatorId;
    private final List<Type> sourceTypes;
    private final List<Integer> outputChannels;
    private final List<WindowFunctionDefinition> windowFunctionDefinitions;
    private final List<Integer> partitionChannels;
    private final List<Integer> preGroupedChannels;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrder;
    private final int preSortedChannelPrefix;
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
            List<Integer> preGroupedChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            int preSortedChannelPrefix,
            FrameInfo frameInfo,
            int expectedPositions)
    {
        requireNonNull(sourceTypes, "sourceTypes is null");
        requireNonNull(outputChannels, "outputChannels is null");
        requireNonNull(windowFunctionDefinitions, "windowFunctionDefinitions is null");
        requireNonNull(partitionChannels, "partitionChannels is null");
        requireNonNull(preGroupedChannels, "preGroupedChannels is null");
        checkArgument(partitionChannels.containsAll(preGroupedChannels), "preGroupedChannels must be a subset of partitionChannels");
        requireNonNull(sortChannels, "sortChannels is null");
        requireNonNull(sortOrder, "sortOrder is null");
        checkArgument(sortChannels.size() == sortOrder.size(), "Must have same number of sort channels as sort orders");
        checkArgument(preSortedChannelPrefix <= sortChannels.size(), "Cannot have more pre-sorted channels than specified sorted channels");
        checkArgument(preSortedChannelPrefix == 0 || ImmutableSet.copyOf(preGroupedChannels).equals(ImmutableSet.copyOf(partitionChannels)), "preSortedChannelPrefix can only be greater than zero if all partition channels are pre-grouped");
        requireNonNull(frameInfo, "frameInfo is null");

        this.operatorId = operatorId;
        this.sourceTypes = ImmutableList.copyOf(sourceTypes);
        this.outputChannels = ImmutableList.copyOf(outputChannels);
        this.windowFunctionDefinitions = ImmutableList.copyOf(windowFunctionDefinitions);
        this.partitionChannels = ImmutableList.copyOf(partitionChannels);
        this.preGroupedChannels = ImmutableList.copyOf(preGroupedChannels);
        this.sortChannels = ImmutableList.copyOf(sortChannels);
        this.sortOrder = ImmutableList.copyOf(sortOrder);
        this.preSortedChannelPrefix = preSortedChannelPrefix;
        this.frameInfo = frameInfo;
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

        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, WindowProcessor.class.getSimpleName());
        WindowProcessor processor = new WindowProcessor(
                operatorContext,
                sourceTypes,
                outputChannels,
                windowFunctionDefinitions,
                partitionChannels,
                preGroupedChannels,
                sortChannels,
                sortOrder,
                preSortedChannelPrefix,
                frameInfo,
                expectedPositions);
        PagePositionEqualitor equalitor = PageCompiler.INSTANCE.compilePagePositionEqualitor(Lists.transform(preGroupedChannels, sourceTypes::get), preGroupedChannels);
        return OperatorBuilder.newOperatorBuilder(processor)
                .bindClusteredInput(equalitor, processor)
                .bindOutput(processor)
                .withMemoryTracking(processor::getEstimatedByteSize)
                .build();
    }

    @Override
    public void close()
    {
        closed = true;
    }

    private static class WindowProcessor
            implements ProcessorBase, ProcessorInput<MultiPageChunkCursor>, ProcessorPageBuilderOutput
    {
        private final OperatorContext operatorContext;
        private final int[] outputChannels;
        private final List<WindowFunction> windowFunctions;
        private final List<Integer> orderChannels;
        private final List<SortOrder> ordering;
        private final List<Type> types;

        private final PagesHashStrategy unGroupedPartitionHashStrategy;
        private final PagesHashStrategy preSortedPartitionHashStrategy;
        private final PagesHashStrategy peerGroupHashStrategy;

        private final FrameInfo frameInfo;

        private final PagesIndex pagesIndex;

        private WindowPartition partition;
        private MultiPageChunkCursor chunkCursor;

        public WindowProcessor(
                OperatorContext operatorContext,
                List<Type> sourceTypes,
                List<Integer> outputChannels,
                List<WindowFunctionDefinition> windowFunctionDefinitions,
                List<Integer> partitionChannels,
                List<Integer> preGroupedChannels,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                int preSortedChannelPrefix,
                FrameInfo frameInfo,
                int expectedPositions)
        {
            requireNonNull(operatorContext, "operatorContext is null");
            requireNonNull(outputChannels, "outputChannels is null");
            requireNonNull(windowFunctionDefinitions, "windowFunctionDefinitions is null");
            requireNonNull(partitionChannels, "partitionChannels is null");
            requireNonNull(preGroupedChannels, "preGroupedChannels is null");
            checkArgument(partitionChannels.containsAll(preGroupedChannels), "preGroupedChannels must be a subset of partitionChannels");
            requireNonNull(sortChannels, "sortChannels is null");
            requireNonNull(sortOrder, "sortOrder is null");
            checkArgument(sortChannels.size() == sortOrder.size(), "Must have same number of sort channels as sort orders");
            checkArgument(preSortedChannelPrefix <= sortChannels.size(), "Cannot have more pre-sorted channels than specified sorted channels");
            checkArgument(preSortedChannelPrefix == 0 || ImmutableSet.copyOf(preGroupedChannels).equals(ImmutableSet.copyOf(partitionChannels)), "preSortedChannelPrefix can only be greater than zero if all partition channels are pre-grouped");
            requireNonNull(frameInfo, "frameInfo is null");

            this.operatorContext = operatorContext;
            this.outputChannels = Ints.toArray(outputChannels);
            this.windowFunctions = windowFunctionDefinitions.stream()
                    .map(WindowFunctionDefinition::createWindowFunction)
                    .collect(toImmutableList());
            this.frameInfo = frameInfo;

            this.types = Stream.concat(
                    outputChannels.stream()
                            .map(sourceTypes::get),
                    windowFunctions.stream()
                            .map(WindowFunction::getType))
                    .collect(toImmutableList());

            this.pagesIndex = new PagesIndex(sourceTypes, expectedPositions);
            List<Integer> unGroupedPartitionChannels = partitionChannels.stream()
                    .filter(channel -> !preGroupedChannels.contains(channel))
                    .collect(toImmutableList());
            this.unGroupedPartitionHashStrategy = pagesIndex.createPagesHashStrategy(unGroupedPartitionChannels, Optional.empty());
            List<Integer> preSortedChannels = sortChannels.stream()
                    .limit(preSortedChannelPrefix)
                    .collect(toImmutableList());
            this.preSortedPartitionHashStrategy = pagesIndex.createPagesHashStrategy(preSortedChannels, Optional.<Integer>empty());
            this.peerGroupHashStrategy = pagesIndex.createPagesHashStrategy(sortChannels, Optional.empty());

            if (preSortedChannelPrefix > 0) {
                // This already implies that set(preGroupedChannels) == set(partitionChannels) (enforced with checkArgument)
                this.orderChannels = ImmutableList.copyOf(Iterables.skip(sortChannels, preSortedChannelPrefix));
                this.ordering = ImmutableList.copyOf(Iterables.skip(sortOrder, preSortedChannelPrefix));
            }
            else {
                // Otherwise, we need to sort by the unGroupedPartitionChannels and all original sort channels
                this.orderChannels = ImmutableList.copyOf(concat(unGroupedPartitionChannels, sortChannels));
                this.ordering = ImmutableList.copyOf(concat(nCopies(unGroupedPartitionChannels.size(), ASC_NULLS_LAST), sortOrder));
            }
        }

        @Override
        public String getName()
        {
            return WindowProcessor.class.getSimpleName();
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        public long getEstimatedByteSize()
        {
            return pagesIndex.getEstimatedSize().toBytes();
        }

        @Override
        public SimpleOperator.ProcessorState addInput(@Nullable MultiPageChunkCursor chunkCursor)
        {
            checkState(this.chunkCursor == null || this.chunkCursor.isPageExhausted(), "Last cursor should have been fully processed");

            if (chunkCursor == null) {
                if (pagesIndex.getPositionCount() > 0) {
                    prepareForOutput();
                    return HAS_OUTPUT;
                }
                return NEEDS_INPUT;
            }

            checkArgument(!chunkCursor.isPageExhausted());
            this.chunkCursor = chunkCursor;
            return processNextPartition();
        }

        private void prepareForOutput()
        {
            checkState(pagesIndex.getPositionCount() > 0);
            checkState(partition == null);

            sortPagesIndexIfNecessary();
            int partitionEnd = Pages.findClusterEnd(pagesIndex, 0, unGroupedPartitionHashStrategy);
            partition = new WindowPartition(pagesIndex, 0, partitionEnd, outputChannels, windowFunctions, frameInfo, peerGroupHashStrategy);
        }

        private SimpleOperator.ProcessorState processNextPartition()
        {
            if (chunkCursor.isPageExhausted()) {
                return NEEDS_INPUT;
            }

            // New partition indicates that we have finished the last partition
            if (pagesIndex.getPositionCount() > 0 && chunkCursor.isNewPartition()) {
                prepareForOutput();
                return HAS_OUTPUT;
            }

            pagesIndex.addPage(chunkCursor.getChunk());

            // If able to advance to the next partition, then the last partition is completed
            if (chunkCursor.advance()) {
                prepareForOutput();
                return HAS_OUTPUT;
            }
            return NEEDS_INPUT;
        }

        @Override
        public SimpleOperator.ProcessorState appendOutputTo(PageBuilder pageBuilder)
        {
            checkState(partition != null);

            while (appendPartitionOutput(pageBuilder)) {
                reset();
                if (processNextPartition() == NEEDS_INPUT) {
                    return NEEDS_INPUT;
                }
            }
            return HAS_OUTPUT;
        }

        private boolean appendPartitionOutput(PageBuilder pageBuilder)
        {
            checkState(partition != null);

            while (!pageBuilder.isFull()) {
                if (!partition.hasNext()) {
                    int partitionStart = partition.getPartitionEnd();

                    if (partitionStart >= pagesIndex.getPositionCount()) {
                        return true;
                    }

                    int partitionEnd = Pages.findClusterEnd(pagesIndex, partitionStart, unGroupedPartitionHashStrategy);
                    partition = new WindowPartition(pagesIndex, partitionStart, partitionEnd, outputChannels, windowFunctions, frameInfo, peerGroupHashStrategy);
                }

                partition.processNextRow(pageBuilder);
            }

            return false;
        }

        private void reset()
        {
            partition = null;
            pagesIndex.clear();
        }

        private void sortPagesIndexIfNecessary()
        {
            if (pagesIndex.getPositionCount() > 1 && !orderChannels.isEmpty()) {
                int startPosition = 0;
                while (startPosition < pagesIndex.getPositionCount()) {
                    int endPosition = Pages.findClusterEnd(pagesIndex, startPosition, preSortedPartitionHashStrategy);
                    pagesIndex.sort(orderChannels, ordering, startPosition, endPosition);
                    startPosition = endPosition;
                }
            }
        }
    }
}
