package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Symbol;
import com.facebook.presto.sql.compiler.Type;

import java.util.Map;

public abstract class PlanOptimizer
{
    public abstract PlanNode optimize(PlanNode plan, Map<Symbol, Type> types);
}
