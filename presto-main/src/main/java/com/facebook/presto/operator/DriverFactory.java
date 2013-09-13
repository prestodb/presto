package com.facebook.presto.operator;

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
    private final boolean inputDriver;
    private final boolean outputDriver;
    private final List<OperatorFactory> operatorFactories;
    private final Set<PlanNodeId> sourceIds;
    private boolean closed;

    public DriverFactory(boolean inputDriver, boolean outputDriver, OperatorFactory firstOperatorFactory, OperatorFactory... otherOperatorFactories)
    {
        this(inputDriver,
                outputDriver,
                ImmutableList.<OperatorFactory>builder()
                        .add(checkNotNull(firstOperatorFactory, "firstOperatorFactory is null"))
                        .add(checkNotNull(otherOperatorFactories, "otherOperatorFactories is null"))
                        .build()
        );
    }

    public DriverFactory(boolean inputDriver, boolean outputDriver, List<OperatorFactory> operatorFactories)
    {
        this.inputDriver = inputDriver;
        this.outputDriver = outputDriver;
        this.operatorFactories = ImmutableList.copyOf(checkNotNull(operatorFactories, "operatorFactories is null"));
        checkArgument(!operatorFactories.isEmpty(), "There must be at least one operator");

        ImmutableSet.Builder<PlanNodeId> sourceIds = ImmutableSet.builder();
        for (OperatorFactory operatorFactory : operatorFactories) {
            if (operatorFactory instanceof SourceOperatorFactory) {
                SourceOperatorFactory sourceOperatorFactory = (SourceOperatorFactory) operatorFactory;
                sourceIds.add(sourceOperatorFactory.getSourceId());
            }
        }
        this.sourceIds = sourceIds.build();
    }

    public boolean isInputDriver()
    {
        return inputDriver;
    }

    public boolean isOutputDriver()
    {
        return outputDriver;
    }

    public Set<PlanNodeId> getSourceIds()
    {
        return sourceIds;
    }

    public synchronized Driver createDriver(DriverContext driverContext)
    {
        checkState(!closed, "DriverFactory is already closed");
        checkNotNull(driverContext, "driverContext is null");
        ImmutableList.Builder<Operator> operators = ImmutableList.builder();
        for (OperatorFactory operatorFactory : operatorFactories) {
            Operator operator = operatorFactory.createOperator(driverContext);
            operators.add(operator);
        }
        return new Driver(driverContext, operators.build());
    }

    @Override
    public synchronized void close()
    {
        closed = true;
        for (OperatorFactory operatorFactory : operatorFactories) {
            operatorFactory.close();
        }
    }
}
