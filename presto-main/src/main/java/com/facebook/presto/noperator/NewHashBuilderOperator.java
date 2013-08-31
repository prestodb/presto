/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.ChannelHash;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.GuardedBy;
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

        @GuardedBy("this")
        private NewPagesIndex pagesIndex;
        @GuardedBy("this")
        private ChannelHash channelHash;

        public NewHashSupplier(List<TupleInfo> tupleInfos)
        {
            this.tupleInfos = tupleInfos;
        }

        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        public synchronized void setHash(ChannelHash channelHash, NewPagesIndex pagesIndex)
        {
            this.pagesIndex = checkNotNull(pagesIndex, "pagesIndex is null");
            this.channelHash = checkNotNull(channelHash, "channelHash is null");
        }

        public synchronized NewSourceHash get()
        {
            if (channelHash == null) {
                return null;
            }
            return new NewSourceHash(new ChannelHash(channelHash), pagesIndex);
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
