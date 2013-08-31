package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.ChannelIndex;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.PagesIndex.MultiSliceFieldOrderedTupleComparator;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class NewInMemoryWindowOperator
        implements NewOperator
{
    public static class NewInMemoryWindowOperatorFactory
            implements NewOperatorFactory
    {
        private final List<TupleInfo> sourceTupleInfos;
        private final int orderingChannel;
        private final int[] outputChannels;
        private final List<WindowFunction> windowFunctions;
        private final int[] partitionFields;
        private final int[] sortFields;
        private final boolean[] sortOrder;
        private final int expectedPositions;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public NewInMemoryWindowOperatorFactory(
                List<TupleInfo> sourceTupleInfos,
                int orderingChannel,
                int[] outputChannels,
                List<WindowFunction> windowFunctions,
                int[] partitionFields,
                int[] sortFields,
                boolean[] sortOrder,
                int expectedPositions)
        {
            this.sourceTupleInfos = sourceTupleInfos;
            this.orderingChannel = orderingChannel;
            this.outputChannels = outputChannels;
            this.windowFunctions = windowFunctions;
            this.partitionFields = partitionFields;
            this.sortFields = sortFields;
            this.sortOrder = sortOrder;
            this.expectedPositions = expectedPositions;

            this.tupleInfos = toTupleInfos(sourceTupleInfos, outputChannels, windowFunctions);
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

            return new NewInMemoryWindowOperator(
                    sourceTupleInfos,
                    orderingChannel,
                    outputChannels,
                    windowFunctions,
                    partitionFields,
                    sortFields,
                    sortOrder,
                    expectedPositions,
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

    private final int orderingChannel;
    private final int[] outputChannels;
    private final List<WindowFunction> windowFunctions;
    private final int[] partitionFields;
    private final int[] sortFields;
    private final boolean[] sortOrder;
    private final List<TupleInfo> tupleInfos;

    private final NewPagesIndex pageIndex;

    private final PageBuilder pageBuilder;

    private State state = State.NEEDS_INPUT;

    private int currentPosition;

    private IntComparator partitionComparator;
    private IntComparator orderComparator;

    private int partitionEnd;
    private int peerGroupEnd;
    private int peerGroupCount;

    public NewInMemoryWindowOperator(
            List<TupleInfo> sourceTupleInfos,
            int orderingChannel,
            int[] outputChannels,
            List<WindowFunction> windowFunctions,
            int[] partitionFields,
            int[] sortFields,
            boolean[] sortOrder,
            int expectedPositions,
            TaskMemoryManager taskMemoryManager)
    {
        this.orderingChannel = orderingChannel;
        this.outputChannels = checkNotNull(outputChannels, "outputChannels is null").clone();
        this.windowFunctions = checkNotNull(windowFunctions, "windowFunctions is null");
        this.partitionFields = checkNotNull(partitionFields, "partitionFields is null").clone();
        this.sortFields = checkNotNull(sortFields, "sortFields is null").clone();
        this.sortOrder = checkNotNull(sortOrder, "sortOrder is null").clone();

        this.tupleInfos = toTupleInfos(sourceTupleInfos, outputChannels, windowFunctions);

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

            // sort by partition fields, then sort fields
            int[] orderFields = Ints.concat(partitionFields, sortFields);

            boolean[] partitionOrder = new boolean[partitionFields.length];
            Arrays.fill(partitionOrder, true);
            boolean[] ordering = Booleans.concat(partitionOrder, sortOrder);

            // sort the index
            pageIndex.sort(orderingChannel, orderFields, ordering);

            // create partition comparator
            ChannelIndex index = pageIndex.getIndex(orderingChannel);
            partitionComparator = new MultiSliceFieldOrderedTupleComparator(partitionFields, partitionOrder, index);

            // create order comparator
            index = pageIndex.getIndex(orderingChannel);
            orderComparator = new MultiSliceFieldOrderedTupleComparator(sortFields, sortOrder, index);
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
            // check for new partition
            boolean newPartition = (currentPosition == 0) || (currentPosition == partitionEnd);
            if (newPartition) {
                // find end of partition
                partitionEnd++;
                while ((partitionEnd < pageIndex.getPositionCount()) &&
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
                pageIndex.appendTupleTo(outputChannels[channel], currentPosition, pageBuilder.getBlockBuilder(channel));
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

    private static List<TupleInfo> toTupleInfos(List<TupleInfo> sourceTupleInfos, int[] outputChannels, List<WindowFunction> windowFunctions)
    {
        ImmutableList.Builder<TupleInfo> tupleInfosBuilder = ImmutableList.builder();
        for (int channel : outputChannels) {
            tupleInfosBuilder.add(sourceTupleInfos.get(channel));
        }
        for (WindowFunction function : windowFunctions) {
            tupleInfosBuilder.add(function.getTupleInfo());
        }
        return tupleInfosBuilder.build();
    }
}
