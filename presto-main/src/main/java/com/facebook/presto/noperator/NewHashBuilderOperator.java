/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class NewHashBuilderOperator
        implements NewOperator
{
    public static class NewHashSupplier
    {
        private final List<TupleInfo> tupleInfos;
        private final SettableFuture<HashData> hashFuture = SettableFuture.create();

        public NewHashSupplier(List<TupleInfo> tupleInfos)
        {
            this.tupleInfos = tupleInfos;
        }

        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        public ListenableFuture<NewSourceHash> getSourceHash()
        {
            return Futures.transform(hashFuture, new Function<HashData, NewSourceHash>()
            {
                @Override
                public NewSourceHash apply(HashData hashData)
                {
                    return new NewSourceHash(new NewChannelHash(hashData.channelHash), hashData.pagesIndex);
                }
            });
        }

        void setHash(NewChannelHash channelHash, NewPagesIndex pagesIndex)
        {
            HashData hashData = new HashData(
                    checkNotNull(channelHash, "channelHash is null"),
                    checkNotNull(pagesIndex, "pagesIndex is null"));

            boolean wasSet = hashFuture.set(hashData);
            checkState(wasSet, "Hash already set");
        }

        private static class HashData
        {
            private final NewChannelHash channelHash;
            private final NewPagesIndex pagesIndex;

            private HashData(NewChannelHash channelHash, NewPagesIndex pagesIndex)
            {
                this.channelHash = channelHash;
                this.pagesIndex = pagesIndex;
            }
        }
    }

    public static class NewHashBuilderOperatorFactory
            implements NewOperatorFactory
    {
        private final int operatorId;
        private final NewHashSupplier hashSupplier;
        private final int hashChannel;
        private final int expectedPositions;
        private boolean closed;

        public NewHashBuilderOperatorFactory(
                int operatorId,
                List<TupleInfo> tupleInfos,
                int hashChannel,
                int expectedPositions)
        {
            this.operatorId = operatorId;
            this.hashSupplier = new NewHashSupplier(checkNotNull(tupleInfos, "tupleInfos is null"));
            Preconditions.checkArgument(hashChannel >= 0, "hashChannel is negative");
            this.hashChannel = hashChannel;
            this.expectedPositions = checkNotNull(expectedPositions, "expectedPositions is null");
        }

        public NewHashSupplier getHashSupplier()
        {
            return hashSupplier;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return hashSupplier.tupleInfos;
        }

        @Override
        public NewOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, NewHashBuilderOperator.class.getSimpleName());
            return new NewHashBuilderOperator(
                    operatorContext,
                    hashSupplier,
                    hashChannel,
                    expectedPositions);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final NewHashSupplier hashSupplier;
    private final int hashChannel;

    private final NewPagesIndex pagesIndex;

    private boolean finished;

    public NewHashBuilderOperator(
            OperatorContext operatorContext,
            NewHashSupplier hashSupplier,
            int hashChannel,
            int expectedPositions)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.hashSupplier = checkNotNull(hashSupplier, "hashSupplier is null");
        this.hashChannel = hashChannel;
        this.pagesIndex = new NewPagesIndex(hashSupplier.getTupleInfos(), expectedPositions, operatorContext);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return hashSupplier.getTupleInfos();
    }

    @Override
    public void finish()
    {
        if (finished) {
            return;
        }

        NewChannelHash channelHash = new NewChannelHash(pagesIndex.getIndex(hashChannel), operatorContext);
        hashSupplier.setHash(channelHash, pagesIndex);
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

        pagesIndex.addPage(page);
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
