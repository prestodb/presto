package com.facebook.presto.operator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SourceSetSupplier
        implements Supplier<ChannelSet>
{
    private final StoppableOperator stoppableSource;
    private final int setChannel;
    private final int expectedPositions;
    private final OperatorStats operatorStats;
    private final TaskMemoryManager taskMemoryManager;

    @GuardedBy("this")
    private ChannelSet channelSet;

    @GuardedBy("this")
    private Throwable buildException;

    public SourceSetSupplier(Operator source, int setChannel, int expectedPositions, TaskMemoryManager taskMemoryManager, OperatorStats operatorStats)
    {
        checkNotNull(source, "source is null");
        checkArgument(setChannel >= 0, "setChannel must be greater than or equal to zero");
        checkArgument(expectedPositions >= 0, "expectedPositions must be greater than or equal to zero");
        checkNotNull(taskMemoryManager, "taskMemoryManager is null");
        checkNotNull(operatorStats, "operatorStats is null");

        this.stoppableSource = new StoppableOperator(source);
        this.setChannel = setChannel;
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
    public synchronized ChannelSet get()
    {
        if (channelSet == null) {
            if (buildException != null) {
                throw Throwables.propagate(buildException);
            }

            try {
                PageIterator iterator = stoppableSource.iterator(operatorStats);
                channelSet = new ChannelSet(iterator, setChannel, expectedPositions, taskMemoryManager, operatorStats);
            }
            catch (Throwable e) {
                buildException = e;
                throw Throwables.propagate(buildException);
            }
        }
        return new ChannelSet(channelSet);
    }

    public void close()
    {
        stoppableSource.stopIterators();
    }
}
