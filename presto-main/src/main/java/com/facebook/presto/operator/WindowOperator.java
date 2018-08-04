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
import com.facebook.presto.operator.window.FramedWindowFunction;
import com.facebook.presto.operator.window.WindowPartition;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.stream.Stream;

import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
        private boolean closed;
        private final PagesIndex.Factory pagesIndexFactory;

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
                int expectedPositions,
                PagesIndex.Factory pagesIndexFactory)
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
            requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            checkArgument(sortChannels.size() == sortOrder.size(), "Must have same number of sort channels as sort orders");
            checkArgument(preSortedChannelPrefix <= sortChannels.size(), "Cannot have more pre-sorted channels than specified sorted channels");
            checkArgument(preSortedChannelPrefix == 0 || ImmutableSet.copyOf(preGroupedChannels).equals(ImmutableSet.copyOf(partitionChannels)), "preSortedChannelPrefix can only be greater than zero if all partition channels are pre-grouped");

            this.pagesIndexFactory = pagesIndexFactory;
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
                    expectedPositions,
                    pagesIndexFactory);
        }

        @Override
        public void noMoreOperators()
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
                    expectedPositions,
                    pagesIndexFactory);
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
    private final LocalMemoryContext localUserMemoryContext;

    private final int[] preGroupedChannels;

    private final PagesHashStrategy preGroupedPartitionHashStrategy;
    private final PagesHashStrategy unGroupedPartitionHashStrategy;
    private final PagesHashStrategy preSortedPartitionHashStrategy;
    private final PagesHashStrategy peerGroupHashStrategy;

    private final PagesIndex pagesIndex;

    private final PageBuilder pageBuilder;

    private final WindowInfo.DriverWindowInfoBuilder windowInfo;
    private final AtomicReference<Optional<WindowInfo.DriverWindowInfo>> driverWindowInfo = new AtomicReference<>(Optional.empty());

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
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory)
    {
        requireNonNull(operatorContext, "operatorContext is null");
        requireNonNull(outputChannels, "outputChannels is null");
        requireNonNull(windowFunctionDefinitions, "windowFunctionDefinitions is null");
        requireNonNull(partitionChannels, "partitionChannels is null");
        requireNonNull(preGroupedChannels, "preGroupedChannels is null");
        checkArgument(partitionChannels.containsAll(preGroupedChannels), "preGroupedChannels must be a subset of partitionChannels");
        requireNonNull(sortChannels, "sortChannels is null");
        requireNonNull(sortOrder, "sortOrder is null");
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        checkArgument(sortChannels.size() == sortOrder.size(), "Must have same number of sort channels as sort orders");
        checkArgument(preSortedChannelPrefix <= sortChannels.size(), "Cannot have more pre-sorted channels than specified sorted channels");
        checkArgument(preSortedChannelPrefix == 0 || ImmutableSet.copyOf(preGroupedChannels).equals(ImmutableSet.copyOf(partitionChannels)), "preSortedChannelPrefix can only be greater than zero if all partition channels are pre-grouped");

        this.operatorContext = operatorContext;
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.outputChannels = Ints.toArray(outputChannels);
        this.windowFunctions = windowFunctionDefinitions.stream()
                .map(functionDefinition -> new FramedWindowFunction(functionDefinition.createWindowFunction(), functionDefinition.getFrameInfo()))
                .collect(toImmutableList());

        List<Type> types = Stream.concat(
                outputChannels.stream()
                        .map(sourceTypes::get),
                windowFunctionDefinitions.stream()
                        .map(WindowFunctionDefinition::getType))
                .collect(toImmutableList());

        this.pagesIndex = pagesIndexFactory.newPagesIndex(sourceTypes, expectedPositions);
        this.preGroupedChannels = Ints.toArray(preGroupedChannels);
        this.preGroupedPartitionHashStrategy = pagesIndex.createPagesHashStrategy(preGroupedChannels, OptionalInt.empty());
        List<Integer> unGroupedPartitionChannels = partitionChannels.stream()
                .filter(channel -> !preGroupedChannels.contains(channel))
                .collect(toImmutableList());
        this.unGroupedPartitionHashStrategy = pagesIndex.createPagesHashStrategy(unGroupedPartitionChannels, OptionalInt.empty());
        List<Integer> preSortedChannels = sortChannels.stream()
                .limit(preSortedChannelPrefix)
                .collect(toImmutableList());
        this.preSortedPartitionHashStrategy = pagesIndex.createPagesHashStrategy(preSortedChannels, OptionalInt.empty());
        this.peerGroupHashStrategy = pagesIndex.createPagesHashStrategy(sortChannels, OptionalInt.empty());

        this.pageBuilder = new PageBuilder(types);

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

        windowInfo = new WindowInfo.DriverWindowInfoBuilder();
        operatorContext.setInfoSupplier(this::getWindowInfo);
    }

    private OperatorInfo getWindowInfo()
    {
        return new WindowInfo(driverWindowInfo.get().map(ImmutableList::of).orElse(ImmutableList.of()));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (state == State.FINISHING || state == State.FINISHED) {
            return;
        }
        if (state == State.NEEDS_INPUT) {
            // Since was waiting for more input, prepare what we have for output since we will not be getting any more input
            finishPagesIndex();
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
        localUserMemoryContext.setBytes(pagesIndex.getEstimatedSize().toBytes());
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
            finishPagesIndex();
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
        localUserMemoryContext.setBytes(pagesIndex.getEstimatedSize().toBytes());
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
                windowInfo.addPartition(partition);
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

    private void finishPagesIndex()
    {
        sortPagesIndexIfNecessary();
        windowInfo.addIndex(pagesIndex);
    }

    // Assumes input grouped on relevant pagesHashStrategy columns
    private static int findGroupEnd(Page page, PagesHashStrategy pagesHashStrategy, int startPosition)
    {
        checkArgument(page.getPositionCount() > 0, "Must have at least one position");
        checkPositionIndex(startPosition, page.getPositionCount(), "startPosition out of bounds");

        return findEndPosition(startPosition, page.getPositionCount(), (firstPosition, secondPosition) -> pagesHashStrategy.rowEqualsRow(firstPosition, page, secondPosition, page));
    }

    // Assumes input grouped on relevant pagesHashStrategy columns
    private static int findGroupEnd(PagesIndex pagesIndex, PagesHashStrategy pagesHashStrategy, int startPosition)
    {
        checkArgument(pagesIndex.getPositionCount() > 0, "Must have at least one position");
        checkPositionIndex(startPosition, pagesIndex.getPositionCount(), "startPosition out of bounds");

        return findEndPosition(startPosition, pagesIndex.getPositionCount(), (firstPosition, secondPosition) -> pagesIndex.positionEqualsPosition(pagesHashStrategy, firstPosition, secondPosition));
    }

    /**
     * @param startPosition - inclusive
     * @param endPosition - exclusive
     * @param comparator - returns true if positions given as parameters are equal
     * @return the end of the group position exclusive (the position the very next group starts)
     */
    @VisibleForTesting
    static int findEndPosition(int startPosition, int endPosition, BiPredicate<Integer, Integer> comparator)
    {
        checkArgument(startPosition >= 0, "startPosition must be greater or equal than zero: %s", startPosition);
        checkArgument(endPosition > 0, "endPosition must be greater than zero: %s", endPosition);
        checkArgument(startPosition < endPosition, "startPosition must be less than endPosition: %s < %s", startPosition, endPosition);

        int left = startPosition;
        int right = endPosition - 1;
        for (int i = 0; i < endPosition - startPosition; i++) {
            int distance = right - left;

            if (distance == 0) {
                return right + 1;
            }

            if (distance == 1) {
                if (comparator.test(left, right)) {
                    return right + 1;
                }
                return right;
            }

            int mid = left + distance / 2;
            if (comparator.test(left, mid)) {
                // explore to the right
                left = mid;
            }
            else {
                // explore to the left
                right = mid;
            }
        }

        // hasn't managed to find a solution after N iteration. Probably the input is not sorted. Lets verify it.
        for (int first = startPosition; first < endPosition; first++) {
            boolean previousPairsWereEqual = true;
            for (int second = first + 1; second < endPosition; second++) {
                if (!comparator.test(first, second)) {
                    previousPairsWereEqual = false;
                }
                else if (!previousPairsWereEqual) {
                    throw new IllegalArgumentException("The input is not sorted");
                }
            }
        }

        // the input is sorted, but the algorithm has still failed
        throw new IllegalArgumentException("failed to find a group ending");
    }

    @Override
    public void close()
    {
        driverWindowInfo.set(Optional.of(windowInfo.build()));
    }
}
