/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.ChannelHash;
import com.facebook.presto.operator.OperatorStats;
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
                    return new NewSourceHash(new ChannelHash(hashData.channelHash), hashData.pagesIndex);
                }
            });
        }

        void setHash(ChannelHash channelHash, NewPagesIndex pagesIndex)
        {
            HashData hashData = new HashData(
                    checkNotNull(channelHash, "channelHash is null"),
                    checkNotNull(pagesIndex, "pagesIndex is null"));

            boolean wasSet = hashFuture.set(hashData);
            checkState(wasSet, "Hash already set");
        }

        private static class HashData
        {
            private final ChannelHash channelHash;
            private final NewPagesIndex pagesIndex;

            private HashData(ChannelHash channelHash, NewPagesIndex pagesIndex)
            {
                this.channelHash = channelHash;
                this.pagesIndex = pagesIndex;
            }
        }
    }

    public static class NewHashBuilderOperatorFactory
            implements NewOperatorFactory
    {
        private final NewHashSupplier hashSupplier;
        private final int hashChannel;
        private final int expectedPositions;
        private boolean closed;

        public NewHashBuilderOperatorFactory(
                List<TupleInfo> tupleInfos,
                int hashChannel,
                int expectedPositions)
        {
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
        public NewOperator createOperator(OperatorStats operatorStats, TaskMemoryManager taskMemoryManager)
        {
            checkState(!closed, "Factory is already closed");
            return new NewHashBuilderOperator(hashSupplier, hashChannel, expectedPositions, taskMemoryManager);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final NewHashSupplier hashSupplier;
    private final int hashChannel;
    private final TaskMemoryManager taskMemoryManager;
    private final NewPagesIndex pagesIndex;

    private boolean finished;

    public NewHashBuilderOperator(
            NewHashSupplier hashSupplier,
            int hashChannel,
            int expectedPositions,
            TaskMemoryManager taskMemoryManager)
    {
        this.hashSupplier = checkNotNull(hashSupplier, "hashSupplier is null");
        this.hashChannel = hashChannel;
        this.taskMemoryManager = taskMemoryManager;
        this.pagesIndex = new NewPagesIndex(hashSupplier.getTupleInfos(), expectedPositions, taskMemoryManager);
    }

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

        ChannelHash channelHash = new ChannelHash(pagesIndex.getIndex(hashChannel), taskMemoryManager);
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
