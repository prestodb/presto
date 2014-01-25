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

import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.util.Collections;
import java.util.List;

import static com.facebook.presto.operator.SortOrder.ASC_NULLS_LAST;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class WindowOperator
        implements Operator
{
    public static class WindowOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final List<WindowFunction> windowFunctions;
        private final List<Integer> partitionChannels;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final int expectedPositions;
        private final List<Type> types;
        private boolean closed;

        public WindowOperatorFactory(
                int operatorId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<WindowFunction> windowFunctions,
                List<Integer> partitionChannels,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                int expectedPositions)
        {
            this.operatorId = operatorId;
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.outputChannels = ImmutableList.copyOf(checkNotNull(outputChannels, "outputChannels is null"));
            this.windowFunctions = windowFunctions;
            this.partitionChannels = ImmutableList.copyOf(checkNotNull(partitionChannels, "partitionChannels is null"));
            this.sortChannels = ImmutableList.copyOf(checkNotNull(sortChannels, "sortChannels is null"));
            this.sortOrder = ImmutableList.copyOf(checkNotNull(sortOrder, "sortOrder is null"));
            this.expectedPositions = expectedPositions;

            this.types = toTypes(sourceTypes, outputChannels, windowFunctions);
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
                    windowFunctions,
                    partitionChannels,
                    sortChannels,
                    sortOrder,
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

    private final PagesIndex pagesIndex;

    private final PageBuilder pageBuilder;

    private State state = State.NEEDS_INPUT;

    private int currentPosition;

    private IntComparator partitionComparator;
    private IntComparator orderComparator;

    private int partitionEnd;
    private int peerGroupEnd;
    private int peerGroupCount;

    public WindowOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunction> windowFunctions,
            List<Integer> partitionChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            int expectedPositions)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.outputChannels = Ints.toArray(checkNotNull(outputChannels, "outputChannels is null"));
        this.windowFunctions = checkNotNull(windowFunctions, "windowFunctions is null");
        this.partitionChannels = ImmutableList.copyOf(checkNotNull(partitionChannels, "partitionChannels is null"));
        this.sortChannels = ImmutableList.copyOf(checkNotNull(sortChannels, "sortChannels is null"));
        this.sortOrder = ImmutableList.copyOf(checkNotNull(sortOrder, "sortOrder is null"));

        this.types = toTypes(sourceTypes, outputChannels, windowFunctions);

        this.pagesIndex = new PagesIndex(sourceTypes, expectedPositions, operatorContext);
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
            List<Integer> orderChannels = ImmutableList.copyOf(Iterables.concat(partitionChannels, sortChannels));
            List<SortOrder> ordering = ImmutableList.copyOf(Iterables.concat(partitionOrder, sortOrder));

            // sort the index
            pagesIndex.sort(orderChannels, ordering);

            // create partition comparator
            partitionComparator = pagesIndex.createComparator(partitionChannels, partitionOrder);

            // create order comparator
            orderComparator = pagesIndex.createComparator(sortChannels, sortOrder);
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
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
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        if (currentPosition >= pagesIndex.getPositionCount()) {
            state = State.FINISHED;
            return null;
        }

        // iterate through the positions sequentially until we have one full page
        pageBuilder.reset();
        while (!pageBuilder.isFull() && currentPosition < pagesIndex.getPositionCount()) {
            // check for new partition
            boolean newPartition = (currentPosition == 0) || (currentPosition == partitionEnd);
            if (newPartition) {
                // find end of partition
                partitionEnd++;
                while ((partitionEnd < pagesIndex.getPositionCount()) &&
                        (partitionComparator.compare(partitionEnd - 1, partitionEnd) == 0)) {
                    partitionEnd++;
                }

                // reset functions for new partition
                for (WindowFunction function : windowFunctions) {
                    function.reset(partitionEnd - currentPosition);
                }
            }

            // copy output channels
            int channel = 0;
            while (channel < outputChannels.length) {
                pagesIndex.appendTo(outputChannels[channel], currentPosition, pageBuilder.getBlockBuilder(channel));
                channel++;
            }

            // check for new peer group
            boolean newPeerGroup = newPartition || (currentPosition == peerGroupEnd);
            if (newPeerGroup) {
                // find end of peer group
                peerGroupEnd++;
                while ((peerGroupEnd < partitionEnd) &&
                        (orderComparator.compare(peerGroupEnd - 1, peerGroupEnd) == 0)) {
                    peerGroupEnd++;
                }
                peerGroupCount = peerGroupEnd - currentPosition;
            }

            // process window functions
            for (WindowFunction function : windowFunctions) {
                function.processRow(pageBuilder.getBlockBuilder(channel), newPeerGroup, peerGroupCount);
                channel++;
            }

            currentPosition++;
        }

        // output the page if we have any data
        if (pageBuilder.isEmpty()) {
            state = State.FINISHED;
            return null;
        }

        Page page = pageBuilder.build();
        return page;
    }

    private static List<Type> toTypes(List<? extends Type> sourceTypes, List<Integer> outputChannels, List<WindowFunction> windowFunctions)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (int channel : outputChannels) {
            types.add(sourceTypes.get(channel));
        }
        for (WindowFunction function : windowFunctions) {
            types.add(function.getType());
        }
        return types.build();
    }
}
