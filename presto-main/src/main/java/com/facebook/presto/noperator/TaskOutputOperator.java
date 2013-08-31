package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.execution.TaskOutput;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TaskOutputOperator
        implements NewOperator
{
    public static class TaskOutputFactory
            implements OutputFactory
    {
        private final TaskOutput taskOutput;

        public TaskOutputFactory(TaskOutput taskOutput)
        {
            this.taskOutput = checkNotNull(taskOutput, "taskOutput is null");
        }

        @Override
        public NewOperatorFactory createOutputOperator(List<TupleInfo> sourceTupleInfo)
        {
            return new TaskOutputOperatorFactory(taskOutput);
        }
    }

    public static class TaskOutputOperatorFactory
            implements NewOperatorFactory
    {
        private final TaskOutput taskOutput;

        public TaskOutputOperatorFactory(TaskOutput taskOutput)
        {
            this.taskOutput = checkNotNull(taskOutput, "taskOutput is null");
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return ImmutableList.of();
        }

        @Override
        public NewOperator createOperator(OperatorStats operatorStats, TaskMemoryManager taskMemoryManager)
        {
            return new TaskOutputOperator(taskOutput);
        }

        @Override
        public void close()
        {
        }
    }

    private final TaskOutput taskOutput;
    private ListenableFuture<?> blocked = NOT_BLOCKED;
    private boolean finished;

    public TaskOutputOperator(TaskOutput taskOutput)
    {
        this.taskOutput = checkNotNull(taskOutput, "taskOutput is null");
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return ImmutableList.of();
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        if (blocked != NOT_BLOCKED && blocked.isDone()) {
            blocked = NOT_BLOCKED;
        }

        return finished && blocked == NOT_BLOCKED;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (blocked != NOT_BLOCKED && blocked.isDone()) {
            blocked = NOT_BLOCKED;
        }
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        if (blocked != NOT_BLOCKED && blocked.isDone()) {
            blocked = NOT_BLOCKED;
        }
        return !finished && blocked == NOT_BLOCKED;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(blocked == NOT_BLOCKED, "output is already blocked");
        ListenableFuture<?> future = taskOutput.enqueuePage(page);
        if (!future.isDone()) {
            this.blocked = future;
        }
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
