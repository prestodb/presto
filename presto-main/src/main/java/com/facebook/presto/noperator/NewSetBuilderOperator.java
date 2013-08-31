/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import com.facebook.presto.block.Block;
import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.ChannelSet;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

import static com.facebook.presto.operator.ChannelSet.ChannelSetBuilder;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class NewSetBuilderOperator
        implements NewOperator
{
    public static class NewSetSupplier
    {
        private final TupleInfo tupleInfo;
        private ChannelSet channelSet;

        public NewSetSupplier(TupleInfo tupleInfo)
        {
            this.tupleInfo = checkNotNull(tupleInfo, "tupleInfo is null");
        }

        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }

        public synchronized ChannelSet getChannelSet()
        {
            if (channelSet == null) {
                return null;
            }
            return new ChannelSet(channelSet);
        }

        public synchronized void setChannelSet(ChannelSet channelSet)
        {
            checkState(this.channelSet == null, "ChannelSet already set");
            this.channelSet = checkNotNull(channelSet, "channelSet is null");
        }
    }

    public static class NewSetBuilderOperatorFactory
            implements NewOperatorFactory
    {
        private final NewSetSupplier setProvider;
        private final int setChannel;
        private final int expectedPositions;
        private boolean closed;

        public NewSetBuilderOperatorFactory(
                List<TupleInfo> tupleInfos,
                int setChannel,
                int expectedPositions)
        {
            Preconditions.checkArgument(setChannel >= 0, "setChannel is negative");
            this.setProvider = new NewSetSupplier(checkNotNull(tupleInfos, "tupleInfos is null").get(setChannel));
            this.setChannel = setChannel;
            this.expectedPositions = checkNotNull(expectedPositions, "expectedPositions is null");
        }

        public NewSetSupplier getSetProvider()
        {
            return setProvider;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return ImmutableList.of();
        }

        @Override
        public NewOperator createOperator(OperatorStats operatorStats, TaskMemoryManager taskMemoryManager)
        {
            checkState(!closed, "Factory is already closed");
            return new NewSetBuilderOperator(setProvider, setChannel, expectedPositions, taskMemoryManager);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final NewSetSupplier setSupplier;
    private final int setChannel;

    private final ChannelSetBuilder channelSetBuilder;

    private boolean finished;

    public NewSetBuilderOperator(
            NewSetSupplier setSupplier,
            int setChannel,
            int expectedPositions,
            TaskMemoryManager taskMemoryManager)
    {
        this.setSupplier = checkNotNull(setSupplier, "setProvider is null");
        this.setChannel = setChannel;
        this.channelSetBuilder = new ChannelSetBuilder(
                setSupplier.getTupleInfo(),
                expectedPositions,
                checkNotNull(taskMemoryManager, "taskMemoryManager is null"));
    }

    public List<TupleInfo> getTupleInfos()
    {
        return ImmutableList.of();
    }

    @Override
    public void finish()
    {
        if (finished) {
            return;
        }

        ChannelSet channelSet = channelSetBuilder.build();
        setSupplier.setChannelSet(channelSet);
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        Block sourceBlock = page.getBlock(setChannel);
        channelSetBuilder.addBlock(sourceBlock);
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
