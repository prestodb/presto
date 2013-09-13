package com.facebook.presto.operator;

import com.facebook.presto.sql.planner.plan.PlanNodeId;

public interface SourceOperatorFactory
        extends OperatorFactory
{
    PlanNodeId getSourceId();

    @Override
    SourceOperator createOperator(DriverContext driverContext);
}
