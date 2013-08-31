package com.facebook.presto.noperator;

import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.CollocatedSplit;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Driver
{
    private final OperatorStats operatorStats;
    private final List<NewOperator> operators;
    private final Map<PlanNodeId, NewSourceOperator> sourceOperators;

    public Driver(OperatorStats operatorStats, NewOperator firstOperator, NewOperator... otherOperators)
    {
        this(checkNotNull(operatorStats, "operatorStats is null"),
                ImmutableList.<NewOperator>builder()
                        .add(checkNotNull(firstOperator, "firstOperator is null"))
                        .add(checkNotNull(otherOperators, "otherOperators is null"))
                        .build());
    }

    public Driver(OperatorStats operatorStats, List<NewOperator> operators)
    {
        this.operatorStats = checkNotNull(operatorStats, "operatorStats is null");
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

    public OperatorStats getOperatorStats()
    {
        return operatorStats;
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
        boolean finished = operatorStats.isDone() || operators.get(operators.size() - 1).isFinished();
        if (finished) {
            operatorStats.finish();
        }
        return finished;
    }

    public synchronized void process()
    {
        operatorStats.start();

        try {
            for (int i = 0; i < operators.size() - 1 && !operatorStats.isDone(); i++) {
                NewOperator current = operators.get(i);
                NewOperator next = operators.get(i + 1);

                if (current.isFinished()) {
                    // let next operator know there will be no more data
                    next.finish();
                }
                else if (next.needsInput()) {
                    Page page = current.getOutput();
                    if (page != null) {

                        // todo record per operator
                        if (i == 0) {
                            operatorStats.addCompletedDataSize(page.getDataSize().toBytes());
                            operatorStats.addCompletedPositions(page.getPositionCount());
                        }
                        next.addInput(page);
                    }
                }
            }
        }
        catch (Throwable t) {
            operatorStats.fail(t);
            throw t;
        }
    }
}
