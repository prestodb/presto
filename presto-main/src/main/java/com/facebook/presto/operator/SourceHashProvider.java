/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Throwables;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Provider;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@ThreadSafe
public class SourceHashProvider
        implements Provider<SourceHash>
{
    private final Operator source;
    private final int hashChannel;
    private final int expectedPositions;
    private final OperatorStats operatorStats;
    private final TaskMemoryManager taskMemoryManager;

    private final AtomicBoolean closed = new AtomicBoolean();

    @GuardedBy("this")
    private SourceHash sourceHash;

    @GuardedBy("this")
    private Throwable buildException;

    public SourceHashProvider(Operator source, int hashChannel, int expectedPositions, TaskMemoryManager taskMemoryManager, OperatorStats operatorStats)
    {
        this.source = source;
        this.hashChannel = hashChannel;
        this.expectedPositions = expectedPositions;
        this.operatorStats = operatorStats;
        this.taskMemoryManager = taskMemoryManager;
    }

    public int getChannelCount()
    {
        return source.getChannelCount();
    }

    public int getHashChannel()
    {
        return hashChannel;
    }

    public List<TupleInfo> getTupleInfos()
    {
        return source.getTupleInfos();
    }

    @Override
    public synchronized SourceHash get()
    {
        if (sourceHash == null) {
            if (buildException != null) {
                throw Throwables.propagate(buildException);
            }

            try {
                PageIterator iterator = new StoppablePageIterator(source.iterator(operatorStats));
                sourceHash = new SourceHash(iterator, hashChannel, expectedPositions, taskMemoryManager, operatorStats);
            }
            catch (Throwable e) {
                buildException = e;
                throw Throwables.propagate(buildException);
            }
        }
        return new SourceHash(sourceHash);
    }

    public void close()
    {
        closed.set(true);
    }

    private class StoppablePageIterator
            extends AbstractPageIterator
    {
        private final PageIterator iterator;

        private StoppablePageIterator(PageIterator iterator)
        {
            super(iterator.getTupleInfos());
            this.iterator = iterator;
        }

        @Override
        protected Page computeNext()
        {
            if (closed.get() || !iterator.hasNext()) {
                return endOfData();
            }
            return iterator.next();
        }

        @Override
        protected void doClose()
        {
            iterator.close();
        }
    }
}
