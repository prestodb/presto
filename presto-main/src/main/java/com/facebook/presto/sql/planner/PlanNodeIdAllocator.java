package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.plan.PlanNodeId;

public class PlanNodeIdAllocator
{
    private int nextId = 0;

    public PlanNodeId getNextId()
    {
        return new PlanNodeId(Integer.toString(nextId++));
    }
}
