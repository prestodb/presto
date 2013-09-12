package com.facebook.presto.noperator;

import com.facebook.presto.sql.planner.plan.PlanNodeId;

public interface NewSourceOperatorFactory
        extends NewOperatorFactory
{
    PlanNodeId getSourceId();

    @Override
    NewSourceOperator createOperator(DriverContext driverContext);
}
