package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Map;

public abstract class PlanOptimizer
{
    public abstract PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types);
}
