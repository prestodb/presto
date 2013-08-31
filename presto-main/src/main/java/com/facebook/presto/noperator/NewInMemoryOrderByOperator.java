package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.booleans.BooleanArrays;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class NewInMemoryOrderByOperator
        implements NewOperator
{
    public static class NewInMemoryOrderByOperatorFactory
            implements NewOperatorFactory
    {
        private final List<TupleInfo> sourceTupleInfos;
        private final int orderByChannel;
        private final int[] outputChannels;
        private final int expectedPositions;
        private final int[] sortFields;
        private final boolean[] sortOrder;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public NewInMemoryOrderByOperatorFactory(
                List<TupleInfo> sourceTupleInfos,
                int orderByChannel,
                int[] outputChannels,
                int expectedPositions)
        {
            this(
                    sourceTupleInfos,
                    orderByChannel,
                    outputChannels,
                    expectedPositions,
                    defaultSortFields(sourceTupleInfos, orderByChannel),
                    defaultSortOrder(sourceTupleInfos, orderByChannel));
        }

        public NewInMemoryOrderByOperatorFactory(
                List<TupleInfo> sourceTupleInfos,
                int orderByChannel,
                int[] outputChannels,
                int expectedPositions,
                int[] sortFields,
                boolean[] sortOrder)
        {
            this.sourceTupleInfos = sourceTupleInfos;
            this.orderByChannel = orderByChannel;
            this.outputChannels = outputChannels;
            this.expectedPositions = expectedPositions;
            this.sortFields = sortFields;
            this.sortOrder = sortOrder;

            this.tupleInfos = toTupleInfos(sourceTupleInfos, outputChannels);
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public NewOperator createOperator(OperatorStats operatorStats, TaskMemoryManager taskMemoryManager)
        {
            checkState(!closed, "Factory is already closed");

            return new NewInMemoryOrderByOperator(
                    sourceTupleInfos,
                    orderByChannel,
                    outputChannels,
                    expectedPositions,
                    sortFields,
                    sortOrder,
                    taskMemoryManager);
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

    private final int orderByChannel;
    private final int[] sortFields;
    private final boolean[] sortOrder;
    private final int[] outputChannels;
    private final List<TupleInfo> tupleInfos;

    private final NewPagesIndex pageIndex;

    private final PageBuilder pageBuilder;
    private int currentPosition;

    private State state = State.NEEDS_INPUT;

    public NewInMemoryOrderByOperator(
            List<TupleInfo> sourceTupleInfos,
            int orderByChannel,
            int[] outputChannels,
            int expectedPositions,
            TaskMemoryManager taskMemoryManager)
    {
        this(
                sourceTupleInfos,
                orderByChannel,
                outputChannels,
                expectedPositions,
                defaultSortFields(sourceTupleInfos, orderByChannel),
                defaultSortOrder(sourceTupleInfos, orderByChannel),
                taskMemoryManager);
    }

    public NewInMemoryOrderByOperator(
            List<TupleInfo> sourceTupleInfos,
            int orderByChannel,
            int[] outputChannels,
            int expectedPositions,
            int[] sortFields,
            boolean[] sortOrder,
            TaskMemoryManager taskMemoryManager)
    {
        this.orderByChannel = orderByChannel;
        this.outputChannels = outputChannels;
        this.tupleInfos = toTupleInfos(sourceTupleInfos, outputChannels);
        this.sortFields = sortFields;
        this.sortOrder = sortOrder;

        this.pageIndex = new NewPagesIndex(sourceTupleInfos, expectedPositions, taskMemoryManager);

        this.pageBuilder = new PageBuilder(this.tupleInfos);
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
            pageIndex.sort(orderByChannel, sortFields, sortOrder);
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

    private static boolean[] defaultSortOrder(List<TupleInfo> sourceTupleInfos, int orderByChannel)
    {
        boolean[] sortOrder;
        TupleInfo orderByTupleInfo = sourceTupleInfos.get(orderByChannel);
        sortOrder = new boolean[orderByTupleInfo.getFieldCount()];
        BooleanArrays.fill(sortOrder, true);
        return sortOrder;
    }

    private static int[] defaultSortFields(List<TupleInfo> sourceTupleInfos, int orderByChannel)
    {
        int[] sortFields;
        TupleInfo orderByTupleInfo = sourceTupleInfos.get(orderByChannel);
        sortFields = new int[orderByTupleInfo.getFieldCount()];
        for (int i = 0; i < sortFields.length; i++) {
            sortFields[i] = i;
        }
        return sortFields;
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
