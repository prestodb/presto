package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.sql.planner.plan.PlanNodeId;

public interface NewSourceOperatorFactory
        extends NewOperatorFactory
{
    PlanNodeId getSourceId();

    @Override
    NewSourceOperator createOperator(OperatorStats operatorStats, TaskMemoryManager taskMemoryManager);
}
