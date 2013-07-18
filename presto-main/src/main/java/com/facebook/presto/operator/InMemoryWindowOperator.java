package com.facebook.presto.operator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.operator.PagesIndex.MultiSliceFieldOrderedTupleComparator;
import static com.google.common.base.Preconditions.checkNotNull;

public class InMemoryWindowOperator
        implements Operator
{
    private final Operator source;
    private final int orderingChannel;
    private final int[] outputChannels;
    private final int expectedPositions;
    private final List<WindowFunction> windowFunctions;
    private final int[] partitionFields;
    private final int[] sortFields;
    private final boolean[] sortOrder;
    private final TaskMemoryManager taskMemoryManager;
    private final List<TupleInfo> tupleInfos;

    public InMemoryWindowOperator(
            Operator source,
            int orderingChannel,
            int[] outputChannels,
            List<WindowFunction> windowFunctions,
            int[] partitionFields,
            int[] sortFields,
            boolean[] sortOrder,
            int expectedPositions,
            TaskMemoryManager taskMemoryManager)
    {
        this.source = checkNotNull(source, "source is null");
        this.orderingChannel = orderingChannel;
        this.outputChannels = checkNotNull(outputChannels, "outputChannels is null").clone();
        this.windowFunctions = ImmutableList.copyOf(checkNotNull(windowFunctions, "windowFunctions is null"));
        this.partitionFields = checkNotNull(partitionFields, "partitionFields is null").clone();
        this.sortFields = checkNotNull(sortFields, "sortFields is null").clone();
        this.sortOrder = checkNotNull(sortOrder, "sortOrder is null").clone();
        this.expectedPositions = expectedPositions;
        this.taskMemoryManager = checkNotNull(taskMemoryManager, "taskMemoryManager is null");

        ImmutableList.Builder<TupleInfo> tupleInfosBuilder = ImmutableList.builder();
        for (int channel : outputChannels) {
            tupleInfosBuilder.add(source.getTupleInfos().get(channel));
        }
        for (WindowFunction function : windowFunctions) {
            tupleInfosBuilder.add(function.getTupleInfo());
        }
        this.tupleInfos = tupleInfosBuilder.build();
    }

    @Override
    public int getChannelCount()
    {
        return tupleInfos.size();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        return new InMemoryWindowOperatorIterator(
                source,
                orderingChannel,
                tupleInfos,
                outputChannels,
                windowFunctions,
                partitionFields,
                sortFields,
                sortOrder,
                expectedPositions,
                taskMemoryManager,
                operatorStats);
    }

    private static class InMemoryWindowOperatorIterator
            extends AbstractPageIterator
    {
        private final PageIterator source;
        private final int orderByChannel;
        private final int[] outputChannels;
        private final List<WindowFunction> windowFunctions;
        private final int[] partitionFields;
        private final int[] sortFields;
        private final boolean[] sortOrder;
        private final int expectedPositions;
        private final TaskMemoryManager taskMemoryManager;
        private final PageBuilder pageBuilder;
        private final OperatorStats operatorStats;
        private PagesIndex pageIndex;
        private int currentPosition;
        private IntComparator partitionComparator;
        private IntComparator orderComparator;
        private int partitionEnd;
        private int peerGroupEnd;
        private int peerGroupCount;

        private InMemoryWindowOperatorIterator(
                Operator source,
                int orderByChannel,
                List<TupleInfo> tupleInfos,
                int[] outputChannels,
                List<WindowFunction> windowFunctions,
                int[] partitionFields,
                int[] sortFields,
                boolean[] sortOrder,
                int expectedPositions,
                TaskMemoryManager taskMemoryManager1,
                OperatorStats operatorStats)
        {
            super(tupleInfos);
            this.source = source.iterator(operatorStats);
            this.orderByChannel = orderByChannel;
            this.outputChannels = outputChannels;
            this.windowFunctions = windowFunctions;
            this.partitionFields = partitionFields;
            this.sortFields = sortFields;
            this.sortOrder = sortOrder;
            this.expectedPositions = expectedPositions;
            this.taskMemoryManager = taskMemoryManager1;
            this.pageBuilder = new PageBuilder(getTupleInfos());
            this.operatorStats = operatorStats;
        }

        @Override
        protected Page computeNext()
        {
            if (pageIndex == null) {
                // index all pages
                pageIndex = new PagesIndex(source, operatorStats, expectedPositions, taskMemoryManager);

                if (operatorStats.isDone()) {
                    return endOfData();
                }

                // sort by partition fields, then sort fields
                int[] orderFields = Ints.concat(partitionFields, sortFields);

                boolean[] partitionOrder = new boolean[partitionFields.length];
                Arrays.fill(partitionOrder, true);
                boolean[] ordering = Booleans.concat(partitionOrder, sortOrder);

                // sort the index
                pageIndex.sort(orderByChannel, orderFields, ordering);

                // create partition comparator
                ChannelIndex index = pageIndex.getIndex(orderByChannel);
                partitionComparator = new MultiSliceFieldOrderedTupleComparator(partitionFields, partitionOrder, index);

                // create order comparator
                index = pageIndex.getIndex(orderByChannel);
                orderComparator = new MultiSliceFieldOrderedTupleComparator(sortFields, sortOrder, index);
            }

            if (currentPosition >= pageIndex.getPositionCount()) {
                return endOfData();
            }

            // iterate through the positions sequentially until we have one full page
            pageBuilder.reset();
            while ((!pageBuilder.isFull()) && (currentPosition < pageIndex.getPositionCount())) {
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
                return endOfData();
            }
            return pageBuilder.build();
        }

        @Override
        protected void doClose()
        {
            source.close();
        }
    }
}
