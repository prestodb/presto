package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.execution.TaskOutput;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DriverFactory
        implements Closeable
{
    private final List<NewOperatorFactory> operatorFactories;
    private final Set<PlanNodeId> sourceIds;
    private boolean closed;

    public DriverFactory(NewOperatorFactory firstOperatorFactory, NewOperatorFactory... otherOperatorFactories)
    {
        this(ImmutableList.<NewOperatorFactory>builder()
                .add(checkNotNull(firstOperatorFactory, "firstOperatorFactory is null"))
                .add(checkNotNull(otherOperatorFactories, "otherOperatorFactories is null"))
                .build());
    }

    public DriverFactory(List<NewOperatorFactory> operatorFactories)
    {
        this.operatorFactories = ImmutableList.copyOf(checkNotNull(operatorFactories, "operatorFactories is null"));
        checkArgument(!operatorFactories.isEmpty(), "There must be at least one operator");

        ImmutableSet.Builder<PlanNodeId> sourceIds = ImmutableSet.builder();
        for (NewOperatorFactory operatorFactory : operatorFactories) {
            if (operatorFactory instanceof NewSourceOperatorFactory) {
                NewSourceOperatorFactory sourceOperatorFactory = (NewSourceOperatorFactory) operatorFactory;
                sourceIds.add(sourceOperatorFactory.getSourceId());
            }
        }
        this.sourceIds = sourceIds.build();
    }

    public Set<PlanNodeId> getSourceIds()
    {
        return sourceIds;
    }

    public synchronized Driver createDriver(OperatorStats operatorStats, TaskMemoryManager taskMemoryManager)
    {
        return createDriver(null, operatorStats, taskMemoryManager);
    }

    // todo driver factory should not be aware of task output
    public synchronized Driver createDriver(TaskOutput taskOutput, TaskMemoryManager taskMemoryManager)
    {
        return createDriver(taskOutput, new OperatorStats(taskOutput), taskMemoryManager);
    }

    private synchronized Driver createDriver(TaskOutput taskOutput, OperatorStats operatorStats, TaskMemoryManager taskMemoryManager)
    {
        checkState(!closed, "DriverFactory is already closed");
        checkNotNull(operatorStats, "operatorStats is null");
        ImmutableList.Builder<NewOperator> operators = ImmutableList.builder();
        for (NewOperatorFactory operatorFactory : operatorFactories) {
            NewOperator operator = operatorFactory.createOperator(operatorStats, taskMemoryManager);
            if (operator instanceof NewTableWriterOperator) {
                checkArgument(taskOutput != null, "TaskOutput is null and a table writer is present");
                NewTableWriterOperator newTableWriterOperator = (NewTableWriterOperator) operator;
                newTableWriterOperator.setTaskOutput(taskOutput);
            }
            operators.add(operator);
        }
        return new Driver(operatorStats, operators.build());
    }

    @Override
    public synchronized void close()
    {
        closed = true;
        for (NewOperatorFactory operatorFactory : operatorFactories) {
            operatorFactory.close();
        }
    }
}
