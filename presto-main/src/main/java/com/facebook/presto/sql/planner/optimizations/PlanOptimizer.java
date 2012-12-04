package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.compiler.Symbol;
import com.facebook.presto.sql.compiler.Type;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Map;

public abstract class PlanOptimizer
{
    public abstract PlanNode optimize(PlanNode plan, Map<Symbol, Type> types);
}
