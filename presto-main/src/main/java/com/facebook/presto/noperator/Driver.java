package com.facebook.presto.noperator;

import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.CollocatedSplit;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.facebook.presto.noperator.NewOperator.NOT_BLOCKED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Driver
{
    private final DriverContext driverContext;
    private final List<NewOperator> operators;
    private final Map<PlanNodeId, NewSourceOperator> sourceOperators;

    public Driver(DriverContext driverContext, NewOperator firstOperator, NewOperator... otherOperators)
    {
        this(checkNotNull(driverContext, "driverContext is null"),
                ImmutableList.<NewOperator>builder()
                        .add(checkNotNull(firstOperator, "firstOperator is null"))
                        .add(checkNotNull(otherOperators, "otherOperators is null"))
                        .build());
    }

    public Driver(DriverContext driverContext, List<NewOperator> operators)
    {
        this.driverContext = checkNotNull(driverContext, "driverContext is null");
        this.operators = ImmutableList.copyOf(checkNotNull(operators, "operators is null"));
        checkArgument(!operators.isEmpty(), "There must be at least one operator");

        ImmutableMap.Builder<PlanNodeId, NewSourceOperator> sourceOperators = ImmutableMap.builder();
        for (NewOperator operator : operators) {
            if (operator instanceof NewSourceOperator) {
                NewSourceOperator sourceOperator = (NewSourceOperator) operator;
                sourceOperators.put(sourceOperator.getSourceId(), sourceOperator);
            }
        }
        this.sourceOperators = sourceOperators.build();
    }

    public DriverContext getDriverContext()
    {
        return driverContext;
    }

    public Set<PlanNodeId> getSourceIds()
    {
        return sourceOperators.keySet();
    }

    public synchronized void addSplit(PlanNodeId sourceId, Split split)
    {
        checkNotNull(sourceId, "sourceId is null");
        checkNotNull(split, "split is null");

        if (split instanceof CollocatedSplit) {
            CollocatedSplit collocatedSplit = (CollocatedSplit) split;
            // unwind collocated splits
            for (Entry<PlanNodeId, Split> entry : collocatedSplit.getSplits().entrySet()) {
                addSplit(entry.getKey(), entry.getValue());
            }
        }
        else {
            NewSourceOperator sourceOperator = sourceOperators.get(sourceId);
            if (sourceOperator != null) {
                sourceOperator.addSplit(split);
            }
        }
    }

    public synchronized void noMoreSplits(PlanNodeId sourceId)
    {
        checkNotNull(sourceId, "sourceId is null");

        NewSourceOperator sourceOperator = sourceOperators.get(sourceId);
        if (sourceOperator != null) {
            sourceOperator.noMoreSplits();
        }
    }

    public synchronized void finish()
    {
        for (NewOperator operator : operators) {
            operator.finish();
        }
    }

    public synchronized boolean isFinished()
    {
        boolean finished = driverContext.isDone() || operators.get(operators.size() - 1).isFinished();
        if (finished) {
            driverContext.finished();
        }
        return finished;
    }

    public synchronized ListenableFuture<?> process()
    {
        driverContext.start();

        try {
            for (int i = 0; i < operators.size() - 1 && !driverContext.isDone(); i++) {
                // check if current operator is blocked
                NewOperator current = operators.get(i);
                ListenableFuture<?> blocked = current.isBlocked();
                if (!blocked.isDone()) {
                    current.getOperatorContext().recordBlocked(blocked);
                    return blocked;
                }

                // check if next operator is blocked
                NewOperator next = operators.get(i + 1);
                blocked = next.isBlocked();
                if (!blocked.isDone()) {
                    next.getOperatorContext().recordBlocked(blocked);
                    return blocked;
                }

                // if current operator is finished...
                if (current.isFinished()) {
                    // let next operator know there will be no more data
                    next.getOperatorContext().startIntervalTimer();
                    next.finish();
                    next.getOperatorContext().recordFinish();
                }
                else {
                    // if next operator needs input...
                    if (next.needsInput()) {
                        // get an output page from current operator
                        current.getOperatorContext().startIntervalTimer();
                        Page page = current.getOutput();
                        current.getOperatorContext().recordGetOutput(page);

                        // if we got an output page, add it to the next operator
                        if (page != null) {
                            next.getOperatorContext().startIntervalTimer();
                            next.addInput(page);
                            next.getOperatorContext().recordAddInput(page);
                        }
                    }
                }
            }
            return NOT_BLOCKED;
        }
        catch (Throwable t) {
            driverContext.failed(t);
            throw t;
        }
    }
}
