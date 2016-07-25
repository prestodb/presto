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

import com.facebook.presto.operator.window.FramedWindowFunction;
import com.facebook.presto.operator.window.WindowPartition;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class WindowOperator
        implements Operator
{
    public static class WindowOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final List<WindowFunctionDefinition> windowFunctionDefinitions;
        private final List<Integer> partitionChannels;
        private final List<Integer> preGroupedChannels;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final int preSortedChannelPrefix;
        private final int expectedPositions;
        private final List<Type> types;
        private boolean closed;

        public WindowOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<WindowFunctionDefinition> windowFunctionDefinitions,
                List<Integer> partitionChannels,
                List<Integer> preGroupedChannels,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                int preSortedChannelPrefix,
                int expectedPositions)
        {
            requireNonNull(sourceTypes, "sourceTypes is null");
            requireNonNull(planNodeId, "planNodeId is null");
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

            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.outputChannels = ImmutableList.copyOf(outputChannels);
            this.windowFunctionDefinitions = ImmutableList.copyOf(windowFunctionDefinitions);
            this.partitionChannels = ImmutableList.copyOf(partitionChannels);
            this.preGroupedChannels = ImmutableList.copyOf(preGroupedChannels);
            this.sortChannels = ImmutableList.copyOf(sortChannels);
            this.sortOrder = ImmutableList.copyOf(sortOrder);
            this.preSortedChannelPrefix = preSortedChannelPrefix;
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

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, WindowOperator.class.getSimpleName());
            return new WindowOperator(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    windowFunctionDefinitions,
                    partitionChannels,
                    preGroupedChannels,
                    sortChannels,
                    sortOrder,
                    preSortedChannelPrefix,
                    expectedPositions);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new WindowOperatorFactory(
                operatorId,
                planNodeId,
                sourceTypes,
                outputChannels,
                windowFunctionDefinitions,
                partitionChannels,
                preGroupedChannels,
                sortChannels,
                sortOrder,
                preSortedChannelPrefix,
                expectedPositions);
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
    private final List<FramedWindowFunction> windowFunctions;
    private final List<Integer> orderChannels;
    private final List<SortOrder> ordering;
    private final List<Type> types;

    private final int[] preGroupedChannels;

    private final PagesHashStrategy preGroupedPartitionHashStrategy;
    private final PagesHashStrategy unGroupedPartitionHashStrategy;
    private final PagesHashStrategy preSortedPartitionHashStrategy;
    private final PagesHashStrategy peerGroupHashStrategy;

    private final PagesIndex pagesIndex;

    private final PageBuilder pageBuilder;

    private State state = State.NEEDS_INPUT;

    private WindowPartition partition;

    private Page pendingInput;

    public WindowOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> windowFunctionDefinitions,
            List<Integer> partitionChannels,
            List<Integer> preGroupedChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            int preSortedChannelPrefix,
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

        this.operatorContext = operatorContext;
        this.outputChannels = Ints.toArray(outputChannels);
        this.windowFunctions = windowFunctionDefinitions.stream()
                .map(functionDefinition -> new FramedWindowFunction(functionDefinition.createWindowFunction(), functionDefinition.getFrameInfo()))
                .collect(toImmutableList());

        this.types = Stream.concat(
                outputChannels.stream()
                        .map(sourceTypes::get),
                windowFunctionDefinitions.stream()
                        .map(WindowFunctionDefinition::getType))
                .collect(toImmutableList());

        this.pagesIndex = new PagesIndex(sourceTypes, expectedPositions);
        this.preGroupedChannels = Ints.toArray(preGroupedChannels);
        this.preGroupedPartitionHashStrategy = pagesIndex.createPagesHashStrategy(preGroupedChannels, Optional.<Integer>empty());
        List<Integer> unGroupedPartitionChannels = partitionChannels.stream()
                .filter(channel -> !preGroupedChannels.contains(channel))
                .collect(toImmutableList());
        this.unGroupedPartitionHashStrategy = pagesIndex.createPagesHashStrategy(unGroupedPartitionChannels, Optional.empty());
        List<Integer> preSortedChannels = sortChannels.stream()
                .limit(preSortedChannelPrefix)
                .collect(toImmutableList());
        this.preSortedPartitionHashStrategy = pagesIndex.createPagesHashStrategy(preSortedChannels, Optional.<Integer>empty());
        this.peerGroupHashStrategy = pagesIndex.createPagesHashStrategy(sortChannels, Optional.empty());

        this.pageBuilder = new PageBuilder(this.types);

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
        if (state == State.NEEDS_INPUT) {
            // Since was waiting for more input, prepare what we have for output since we will not be getting any more input
            sortPagesIndexIfNecessary();
        }
        state = State.FINISHING;
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
        checkState(state == State.NEEDS_INPUT, "Operator can not take input at this time");
        requireNonNull(page, "page is null");
        checkState(pendingInput == null, "Operator already has pending input");

        if (page.getPositionCount() == 0) {
            return;
        }

        pendingInput = page;
        if (processPendingInput()) {
            state = State.HAS_OUTPUT;
        }
        operatorContext.setMemoryReservation(pagesIndex.getEstimatedSize().toBytes());
    }

    /**
     * @return true if a full group has been buffered after processing the pendingInput, false otherwise
     */
    private boolean processPendingInput()
    {
        checkState(pendingInput != null);
        pendingInput = updatePagesIndex(pendingInput);

        // If we have unused input or are finishing, then we have buffered a full group
        if (pendingInput != null || state == State.FINISHING) {
            sortPagesIndexIfNecessary();
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * @return the unused section of the page, or null if fully applied.
     * pagesIndex guaranteed to have at least one row after this method returns
     */
    private Page updatePagesIndex(Page page)
    {
        checkArgument(page.getPositionCount() > 0);

        // TODO: Fix pagesHashStrategy to allow specifying channels for comparison, it currently requires us to rearrange the right side blocks in consecutive channel order
        Page preGroupedPage = rearrangePage(page, preGroupedChannels);
        if (pagesIndex.getPositionCount() == 0 || pagesIndex.positionEqualsRow(preGroupedPartitionHashStrategy, 0, 0, preGroupedPage)) {
            // Find the position where the pre-grouped columns change
            int groupEnd = findGroupEnd(preGroupedPage, preGroupedPartitionHashStrategy, 0);

            // Add the section of the page that contains values for the current group
            pagesIndex.addPage(page.getRegion(0, groupEnd));

            if (page.getPositionCount() - groupEnd > 0) {
                // Save the remaining page, which may contain multiple partitions
                return page.getRegion(groupEnd, page.getPositionCount() - groupEnd);
            }
            else {
                // Page fully consumed
                return null;
            }
        }
        else {
            // We had previous results buffered, but the new page starts with new group values
            return page;
        }
    }

    private static Page rearrangePage(Page page, int[] channels)
    {
        Block[] newBlocks = new Block[channels.length];
        for (int i = 0; i < channels.length; i++) {
            newBlocks[i] = page.getBlock(channels[i]);
        }
        return new Page(page.getPositionCount(), newBlocks);
    }

    @Override
    public Page getOutput()
    {
        if (state == State.NEEDS_INPUT || state == State.FINISHED) {
            return null;
        }

        Page page = extractOutput();
        operatorContext.setMemoryReservation(pagesIndex.getEstimatedSize().toBytes());
        return page;
    }

    private Page extractOutput()
    {
        // INVARIANT: pagesIndex contains the full grouped & sorted data for one or more partitions

        // Iterate through the positions sequentially until we have one full page
        while (!pageBuilder.isFull()) {
            if (partition == null || !partition.hasNext()) {
                int partitionStart = partition == null ? 0 : partition.getPartitionEnd();

                if (partitionStart >= pagesIndex.getPositionCount()) {
                    // Finished all of the partitions in the current pagesIndex
                    partition = null;
                    pagesIndex.clear();

                    // Try to extract more partitions from the pendingInput
                    if (pendingInput != null && processPendingInput()) {
                        partitionStart = 0;
                    }
                    else if (state == State.FINISHING) {
                        state = State.FINISHED;
                        // Output the remaining page if we have anything buffered
                        if (!pageBuilder.isEmpty()) {
                            Page page = pageBuilder.build();
                            pageBuilder.reset();
                            return page;
                        }
                        return null;
                    }
                    else {
                        state = State.NEEDS_INPUT;
                        return null;
                    }
                }

                int partitionEnd = findGroupEnd(pagesIndex, unGroupedPartitionHashStrategy, partitionStart);
                partition = new WindowPartition(pagesIndex, partitionStart, partitionEnd, outputChannels, windowFunctions, peerGroupHashStrategy);
            }

            partition.processNextRow(pageBuilder);
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private void sortPagesIndexIfNecessary()
    {
        if (pagesIndex.getPositionCount() > 1 && !orderChannels.isEmpty()) {
            int startPosition = 0;
            while (startPosition < pagesIndex.getPositionCount()) {
                int endPosition = findGroupEnd(pagesIndex, preSortedPartitionHashStrategy, startPosition);
                pagesIndex.sort(orderChannels, ordering, startPosition, endPosition);
                startPosition = endPosition;
            }
        }
    }

    // Assumes input grouped on relevant pagesHashStrategy columns
    private static int findGroupEnd(Page page, PagesHashStrategy pagesHashStrategy, int startPosition)
    {
        checkArgument(page.getPositionCount() > 0, "Must have at least one position");
        checkPositionIndex(startPosition, page.getPositionCount(), "startPosition out of bounds");

        // Short circuit if the whole page has the same value
        if (pagesHashStrategy.rowEqualsRow(startPosition, page, page.getPositionCount() - 1, page)) {
            return page.getPositionCount();
        }

        // TODO: do position binary search
        int endPosition = startPosition + 1;
        while (endPosition < page.getPositionCount() &&
                pagesHashStrategy.rowEqualsRow(endPosition - 1, page, endPosition, page)) {
            endPosition++;
        }
        return endPosition;
    }

    // Assumes input grouped on relevant pagesHashStrategy columns
    private static int findGroupEnd(PagesIndex pagesIndex, PagesHashStrategy pagesHashStrategy, int startPosition)
    {
        checkArgument(pagesIndex.getPositionCount() > 0, "Must have at least one position");
        checkPositionIndex(startPosition, pagesIndex.getPositionCount(), "startPosition out of bounds");

        // Short circuit if the whole page has the same value
        if (pagesIndex.positionEqualsPosition(pagesHashStrategy, startPosition, pagesIndex.getPositionCount() - 1)) {
            return pagesIndex.getPositionCount();
        }

        // TODO: do position binary search
        int endPosition = startPosition + 1;
        while ((endPosition < pagesIndex.getPositionCount()) &&
                pagesIndex.positionEqualsPosition(pagesHashStrategy, endPosition - 1, endPosition)) {
            endPosition++;
        }
        return endPosition;
    }
}
