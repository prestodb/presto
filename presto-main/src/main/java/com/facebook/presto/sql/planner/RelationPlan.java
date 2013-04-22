package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.base.Preconditions;

import java.util.Map;

import static com.google.common.base.Preconditions.*;

class RelationPlan
{
    private final PlanNode root;
    private final Map<Field, Symbol> outputMappings;
    private final TupleDescriptor descriptor;

    public RelationPlan(Map<Field, Symbol> outputMappings, PlanNode root, TupleDescriptor descriptor)
    {
        checkNotNull(root, "root is null");
        checkNotNull(outputMappings, "outputMappings is null");
        checkNotNull(descriptor, "descriptor is null");

        this.root = root;
        this.outputMappings = outputMappings;
        this.descriptor = descriptor;
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

    TupleDescriptor getDescriptor()
    {
        return descriptor;
    }
}
