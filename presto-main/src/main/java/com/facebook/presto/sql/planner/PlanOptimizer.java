package com.facebook.presto.sql.planner;

public abstract class PlanOptimizer
{
    public abstract PlanNode optimize(PlanNode plan);
}
