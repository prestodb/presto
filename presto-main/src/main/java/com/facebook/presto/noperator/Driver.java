package com.facebook.presto.noperator;

import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.Split;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
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

    /**
     * @deprecated temp hack to allow transformation to old Operator
     */
    @Deprecated
    public List<NewOperator> getOperators()
    {
        return operators;
    }

    /**
     * @deprecated temp hack to allow transformation to old Operator
     */
    @Deprecated
    public NewOperator getOutputOperator()
    {
        return Iterables.getLast(operators);
    }

    public Set<PlanNodeId> getSourceIds()
    {
        return sourceOperators.keySet();
    }

    public void addSplit(PlanNodeId sourceId, Split split)
    {
        checkNotNull(sourceId, "sourceId is null");
        checkNotNull(split, "split is null");

        NewSourceOperator sourceOperator = sourceOperators.get(sourceId);
        checkArgument(sourceOperator != null, "No source %s", sourceId);
        sourceOperator.addSplit(split);
    }

    public void noMoreSplits(PlanNodeId sourceId)
    {
        checkNotNull(sourceId, "sourceId is null");

        NewSourceOperator sourceOperator = sourceOperators.get(sourceId);
        checkArgument(sourceOperator != null, "No source %s", sourceId);
        sourceOperator.noMoreSplits();
    }

    public void finish()
    {
        for (NewOperator operator : operators) {
            operator.finish();
        }
    }

    public boolean isFinished()
    {
        return operatorStats.isDone() || operators.get(operators.size() - 1).isFinished();
    }

    public void process()
    {
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
}
