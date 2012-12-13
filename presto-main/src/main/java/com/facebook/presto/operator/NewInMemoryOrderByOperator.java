package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.booleans.BooleanArrays;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class NewInMemoryOrderByOperator
        implements Operator
{
    private final Operator source;
    private final int expectedPositions;
    private final int orderByChannel;
    private final int[] sortFields;
    private final boolean[] sortOrder;
    private final int[] outputChannels;
    private final List<TupleInfo> tupleInfos;

    public NewInMemoryOrderByOperator(Operator source, int orderByChannel, int[] outputChannels, int expectedPositions)
    {
        this(source,
                orderByChannel,
                outputChannels,
                expectedPositions,
                defaultSortFields(source, orderByChannel),
                defaultSortOrder(source, orderByChannel));
    }

    public NewInMemoryOrderByOperator(Operator source, int orderByChannel, int[] outputChannels, int expectedPositions, int[] sortFields, boolean[] sortOrder)
    {
        checkNotNull(source, "source is null");

        this.source = source;
        this.expectedPositions = expectedPositions;
        this.orderByChannel = orderByChannel;
        this.outputChannels = outputChannels;
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (int channel : outputChannels) {
            tupleInfos.add(source.getTupleInfos().get(channel));
        }
        this.tupleInfos = tupleInfos.build();
        this.sortFields = sortFields;
        this.sortOrder = sortOrder;
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
        return new NewInMemoryOrderByOperatorIterator(source, orderByChannel, tupleInfos, outputChannels, expectedPositions, sortFields, sortOrder, operatorStats);
    }

    private static class NewInMemoryOrderByOperatorIterator
            extends AbstractPageIterator
    {
        private final List<TupleInfo> tupleInfos;
        private final int[] outputChannels;
        private final PagesIndex pageIndex;
        private int currentPosition;

        private NewInMemoryOrderByOperatorIterator(Operator source,
                int orderByChannel,
                List<TupleInfo> tupleInfos,
                int[] outputChannels,
                int expectedPositions,
                int[] sortFields,
                boolean[] sortOrder,
                OperatorStats operatorStats)
        {
            super(source.getTupleInfos());

            this.tupleInfos = tupleInfos;
            this.outputChannels = outputChannels;

            // index all pages
            pageIndex = new PagesIndex(source, expectedPositions, operatorStats);

            // sort the index
            pageIndex.sort(orderByChannel, sortFields, sortOrder);
        }

        @Override
        protected Page computeNext()
        {
            if (currentPosition >= pageIndex.getPositionCount()) {
                return endOfData();
            }

            // iterate through the positions sequentially until we have one full page
            PageBuilder pageBuilder = new PageBuilder(tupleInfos);
            while (!pageBuilder.isFull() && currentPosition < pageIndex.getPositionCount()) {
                for (int i = 0; i < outputChannels.length; i++) {
                    pageIndex.appendTupleTo(outputChannels[i], currentPosition, pageBuilder.getBlockBuilder(i));
                }
                currentPosition++;
            }

            // output the page if we have any data
            if (pageBuilder.isEmpty()) {
                return endOfData();
            }
            Page page = pageBuilder.build();
            return page;
        }

        @Override
        protected void doClose()
        {
        }
    }

    private static boolean[] defaultSortOrder(Operator source, int orderByChannel)
    {
        boolean[] sortOrder;
        TupleInfo orderByTupleInfo = source.getTupleInfos().get(orderByChannel);
        sortOrder = new boolean[orderByTupleInfo.getFieldCount()];
        BooleanArrays.fill(sortOrder, true);
        return sortOrder;
    }

    private static int[] defaultSortFields(Operator source, int orderByChannel)
    {
        int[] sortFields;
        TupleInfo orderByTupleInfo = source.getTupleInfos().get(orderByChannel);
        sortFields = new int[orderByTupleInfo.getFieldCount()];
        for (int i = 0; i < sortFields.length; i++) {
            sortFields[i] = i;
        }
        return sortFields;
    }
}
