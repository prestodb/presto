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

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class OrderByOperator
        implements Operator
{
    public static class OrderByOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<TupleInfo> sourceTupleInfos;
        private final int[] outputChannels;
        private final int expectedPositions;
        private final int[] sortChannels;
        private final SortOrder[] sortOrder;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public OrderByOperatorFactory(
                int operatorId,
                List<TupleInfo> sourceTupleInfos,
                int[] sortChannels,
                int[] outputChannels,
                int expectedPositions)
        {
            this(
                    operatorId,
                    sourceTupleInfos,
                    outputChannels,
                    expectedPositions,
                    sortChannels,
                    defaultSortOrder(sortChannels));
        }

        public OrderByOperatorFactory(
                int operatorId,
                List<TupleInfo> sourceTupleInfos,
                int[] outputChannels,
                int expectedPositions,
                int[] sortChannels,
                SortOrder[] sortOrder)
        {
            this.operatorId = operatorId;
            this.sourceTupleInfos = ImmutableList.copyOf(checkNotNull(sourceTupleInfos, "sourceTupleInfos is null"));
            this.outputChannels = checkNotNull(outputChannels, "outputChannels is null");
            this.expectedPositions = expectedPositions;
            this.sortChannels = checkNotNull(sortChannels, "sortChannels is null");
            this.sortOrder = checkNotNull(sortOrder, "sortOrder is null");

            this.tupleInfos = toTupleInfos(sourceTupleInfos, outputChannels);
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, OrderByOperator.class.getSimpleName());
            return new OrderByOperator(
                    operatorContext,
                    sourceTupleInfos,
                    outputChannels,
                    expectedPositions,
                    sortChannels,
                    sortOrder);
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
    private final int[] sortChannels;
    private final SortOrder[] sortOrder;
    private final int[] outputChannels;
    private final List<TupleInfo> tupleInfos;

    private final PagesIndex pageIndex;

    private final PageBuilder pageBuilder;
    private int currentPosition;

    private State state = State.NEEDS_INPUT;

    public OrderByOperator(
            OperatorContext operatorContext,
            List<TupleInfo> sourceTupleInfos,
            int[] outputChannels,
            int expectedPositions,
            int[] sortChannels,
            SortOrder[] sortOrder)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.outputChannels = checkNotNull(outputChannels, "outputChannels is null");
        this.tupleInfos = toTupleInfos(sourceTupleInfos, outputChannels);
        this.sortChannels = checkNotNull(sortChannels, "sortChannels is null");
        this.sortOrder = checkNotNull(sortOrder, "sortOrder is null");

        this.pageIndex = new PagesIndex(sourceTupleInfos, expectedPositions, operatorContext);

        this.pageBuilder = new PageBuilder(this.tupleInfos);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public void finish()
    {
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;

            // sort the index
            pageIndex.sort(sortChannels, sortOrder);
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

        pageIndex.addPage(page);
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        if (currentPosition >= pageIndex.getPositionCount()) {
            state = State.FINISHED;
            return null;
        }

        // iterate through the positions sequentially until we have one full page
        pageBuilder.reset();
        while (!pageBuilder.isFull() && currentPosition < pageIndex.getPositionCount()) {
            for (int i = 0; i < outputChannels.length; i++) {
                pageIndex.appendTupleTo(outputChannels[i], currentPosition, pageBuilder.getBlockBuilder(i));
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

    private static SortOrder[] defaultSortOrder(int[] orderByChannels)
    {
        SortOrder[] sortOrder = new SortOrder[orderByChannels.length];
        Arrays.fill(sortOrder, SortOrder.ASC_NULLS_LAST);
        return sortOrder;
    }

    private static List<TupleInfo> toTupleInfos(List<TupleInfo> sourceTupleInfos, int[] outputChannels)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (int channel : outputChannels) {
            tupleInfos.add(sourceTupleInfos.get(channel));
        }
        return tupleInfos.build();
    }
}
