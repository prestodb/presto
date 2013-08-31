package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.execution.TaskOutput;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

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
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        // todo check if the buffer is full
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        try {
            if (!taskOutput.addPage(page)) {
                finished = true;
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
