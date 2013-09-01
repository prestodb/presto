/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class SourceHashSupplier
        implements Supplier<SourceHash>
{
    private final StoppableOperator stoppableSource;
    private final int hashChannel;
    private final int expectedPositions;
    private final OperatorStats operatorStats;
    private final TaskMemoryManager taskMemoryManager;

    @GuardedBy("this")
    private SourceHash sourceHash;

    @GuardedBy("this")
    private Throwable buildException;

    public SourceHashSupplier(Operator source, int hashChannel, int expectedPositions, TaskMemoryManager taskMemoryManager, OperatorStats operatorStats)
    {
        checkNotNull(source, "source is null");
        checkArgument(hashChannel >= 0, "hashChannel must be greater than or equal to zero");
        checkArgument(expectedPositions >= 0, "expectedPositions must be greater than or equal to zero");
        checkNotNull(taskMemoryManager, "taskMemoryManager is null");
        checkNotNull(operatorStats, "operatorStats is null");

        this.stoppableSource = new StoppableOperator(source);
        this.hashChannel = hashChannel;
        this.expectedPositions = expectedPositions;
        this.operatorStats = operatorStats;
        this.taskMemoryManager = taskMemoryManager;
    }

    public int getChannelCount()
    {
        return stoppableSource.getChannelCount();
    }

    public List<TupleInfo> getTupleInfos()
    {
        return stoppableSource.getTupleInfos();
    }

    @Override
    public synchronized SourceHash get()
    {
        if (sourceHash == null) {
            if (buildException != null) {
                throw Throwables.propagate(buildException);
            }

            try {
                PageIterator iterator = stoppableSource.iterator(operatorStats);
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
        stoppableSource.stopIterators();
    }
}
