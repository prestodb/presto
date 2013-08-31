/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import com.facebook.presto.block.Block;
import com.facebook.presto.noperator.NewChannelSet.NewChannelSetBuilder;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class NewSetBuilderOperator
        implements NewOperator
{
    public static class NewSetSupplier
    {
        private final TupleInfo tupleInfo;
        private final SettableFuture<NewChannelSet> channelSetFuture = SettableFuture.create();

        public NewSetSupplier(TupleInfo tupleInfo)
        {
            this.tupleInfo = checkNotNull(tupleInfo, "tupleInfo is null");
        }

        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }

        public ListenableFuture<NewChannelSet> getChannelSet()
        {
            return Futures.transform(channelSetFuture, new Function<NewChannelSet, NewChannelSet>() {
                @Override
                public NewChannelSet apply(NewChannelSet channelSet)
                {
                    return new NewChannelSet(channelSet);
                }
            });
        }

        void setChannelSet(NewChannelSet channelSet)
        {
            boolean wasSet = channelSetFuture.set(checkNotNull(channelSet, "channelSet is null"));
            checkState(wasSet, "ChannelSet already set");
        }
    }

    public static class NewSetBuilderOperatorFactory
            implements NewOperatorFactory
    {
        private final int operatorId;
        private final NewSetSupplier setProvider;
        private final int setChannel;
        private final int expectedPositions;
        private boolean closed;

        public NewSetBuilderOperatorFactory(
                int operatorId,
                List<TupleInfo> tupleInfos,
                int setChannel,
                int expectedPositions)
        {
            this.operatorId = operatorId;
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
        public NewOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, NewSetBuilderOperator.class.getSimpleName());
            return new NewSetBuilderOperator(operatorContext, setProvider, setChannel, expectedPositions);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final NewSetSupplier setSupplier;
    private final int setChannel;

    private final NewChannelSetBuilder channelSetBuilder;

    private boolean finished;

    public NewSetBuilderOperator(
            OperatorContext operatorContext,
            NewSetSupplier setSupplier,
            int setChannel,
            int expectedPositions)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.setSupplier = checkNotNull(setSupplier, "setProvider is null");
        this.setChannel = setChannel;
        this.channelSetBuilder = new NewChannelSetBuilder(
                setSupplier.getTupleInfo(),
                expectedPositions,
                checkNotNull(operatorContext, "operatorContext is null"));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
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

        NewChannelSet channelSet = channelSetBuilder.build();
        setSupplier.setChannelSet(channelSet);
        operatorContext.recordGeneratedOutput(channelSet.getEstimatedSize(), channelSet.size());
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
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
