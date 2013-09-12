package com.facebook.presto.noperator;

import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
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
        private final int operatorId;
        private final List<TupleInfo> sourceTupleInfos;
        private final int orderByChannel;
        private final int[] outputChannels;
        private final int expectedPositions;
        private final int[] sortFields;
        private final boolean[] sortOrder;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public NewInMemoryOrderByOperatorFactory(
                int operatorId,
                List<TupleInfo> sourceTupleInfos,
                int orderByChannel,
                int[] outputChannels,
                int expectedPositions)
        {
            this(
                    operatorId,
                    sourceTupleInfos,
                    orderByChannel,
                    outputChannels,
                    expectedPositions,
                    defaultSortFields(sourceTupleInfos, orderByChannel),
                    defaultSortOrder(sourceTupleInfos, orderByChannel));
        }

        public NewInMemoryOrderByOperatorFactory(
                int operatorId,
                List<TupleInfo> sourceTupleInfos,
                int orderByChannel,
                int[] outputChannels,
                int expectedPositions,
                int[] sortFields,
                boolean[] sortOrder)
        {
            this.operatorId = operatorId;
            this.sourceTupleInfos = ImmutableList.copyOf(checkNotNull(sourceTupleInfos, "sourceTupleInfos is null"));
            this.orderByChannel = orderByChannel;
            this.outputChannels = checkNotNull(outputChannels, "outputChannels is null");
            this.expectedPositions = expectedPositions;
            this.sortFields = checkNotNull(sortFields, "sortFields is null");
            this.sortOrder = checkNotNull(sortOrder, "sortOrder is null");

            this.tupleInfos = toTupleInfos(sourceTupleInfos, outputChannels);
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public NewOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, NewInMemoryOrderByOperator.class.getSimpleName());
            return new NewInMemoryOrderByOperator(
                    operatorContext,
                    sourceTupleInfos,
                    orderByChannel,
                    outputChannels,
                    expectedPositions,
                    sortFields,
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
            OperatorContext operatorContext,
            List<TupleInfo> sourceTupleInfos,
            int orderByChannel,
            int[] outputChannels,
            int expectedPositions,
            int[] sortFields,
            boolean[] sortOrder)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.orderByChannel = orderByChannel;
        this.outputChannels = checkNotNull(outputChannels, "outputChannels is null");
        this.tupleInfos = toTupleInfos(sourceTupleInfos, outputChannels);
        this.sortFields = checkNotNull(sortFields, "sortFields is null");
        this.sortOrder = checkNotNull(sortOrder, "sortOrder is null");

        this.pageIndex = new NewPagesIndex(sourceTupleInfos, expectedPositions, operatorContext);

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
            pageIndex.sort(orderByChannel, sortFields, sortOrder);
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

    private static boolean[] defaultSortOrder(List<TupleInfo> sourceTupleInfos, int orderByChannel)
    {
        TupleInfo orderByTupleInfo = sourceTupleInfos.get(orderByChannel);
        boolean[] sortOrder = new boolean[orderByTupleInfo.getFieldCount()];
        BooleanArrays.fill(sortOrder, true);
        return sortOrder;
    }

    private static int[] defaultSortFields(List<TupleInfo> sourceTupleInfos, int orderByChannel)
    {
        TupleInfo orderByTupleInfo = sourceTupleInfos.get(orderByChannel);
        int[] sortFields = new int[orderByTupleInfo.getFieldCount()];
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
