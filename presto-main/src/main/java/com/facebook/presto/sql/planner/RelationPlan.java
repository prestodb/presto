package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.base.Preconditions;

import java.util.Map;

class RelationPlan
{
    private final PlanNode root;
    private final Map<Field, Symbol> outputMappings;

    public RelationPlan(Map<Field, Symbol> outputMappings, PlanNode root)
    {
        this.root = root;
        this.outputMappings = outputMappings;
    }

    public Symbol getSymbol(Field field)
    {
        Preconditions.checkArgument(outputMappings.containsKey(field), "No field->symbol mapping for %s", field);
        return outputMappings.get(field);
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public Map<Field, Symbol> getOutputMappings()
    {
        return outputMappings;
    }
}
