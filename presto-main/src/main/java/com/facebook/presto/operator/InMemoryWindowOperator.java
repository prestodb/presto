package com.facebook.presto.operator;

import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
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
    private final DataSize maxSize;
    private final List<TupleInfo> tupleInfos;

    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    public InMemoryWindowOperator(
            Operator source,
            int orderingChannel,
            int[] outputChannels,
            List<WindowFunction> windowFunctions,
            int[] partitionFields,
            int[] sortFields,
            boolean[] sortOrder,
            int expectedPositions,
            DataSize maxSize)
    {
        this.source = checkNotNull(source, "source is null");
        this.orderingChannel = orderingChannel;
        this.outputChannels = outputChannels;
        this.windowFunctions = ImmutableList.copyOf(checkNotNull(windowFunctions, "windowFunctions is null"));
        this.partitionFields = partitionFields;
        this.sortFields = sortFields;
        this.sortOrder = sortOrder;
        this.expectedPositions = expectedPositions;
        this.maxSize = checkNotNull(maxSize, "maxSize is null");

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
                maxSize,
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
        private final DataSize maxSize;
        private PagesIndex pageIndex;
        private int currentPosition;
        private IntComparator partitionComparator;

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
                DataSize maxSize,
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
            this.maxSize = maxSize;
        }

        @Override
        protected Page computeNext()
        {
            if (pageIndex == null) {
                // index all pages
                pageIndex = new PagesIndex(source, expectedPositions, maxSize);

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
            }

            if (currentPosition >= pageIndex.getPositionCount()) {
                return endOfData();
            }

            // iterate through the positions sequentially until we have one full page
            PageBuilder pageBuilder = new PageBuilder(getTupleInfos());
            while ((!pageBuilder.isFull()) && (currentPosition < pageIndex.getPositionCount())) {
                // check for new partition
                if ((currentPosition == 0) || (partitionComparator.compare(currentPosition - 1, currentPosition) != 0)) {
                    for (WindowFunction function : windowFunctions) {
                        function.reset();
                    }
                }

                // copy output channels
                int channel = 0;
                while (channel < outputChannels.length) {
                    pageIndex.appendTupleTo(outputChannels[channel], currentPosition, pageBuilder.getBlockBuilder(channel));
                    channel++;
                }

                // process window functions
                for (WindowFunction function : windowFunctions) {
                    function.processRow(pageBuilder.getBlockBuilder(channel));
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
