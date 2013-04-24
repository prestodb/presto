package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

class RelationPlan
{
    private final PlanNode root;
    private final List<Symbol> outputSymbols;
    private final TupleDescriptor descriptor;

    public RelationPlan(PlanNode root, TupleDescriptor descriptor, List<Symbol> outputSymbols)
    {
        checkNotNull(root, "root is null");
        checkNotNull(outputSymbols, "outputSymbols is null");
        checkNotNull(descriptor, "descriptor is null");

        checkArgument(descriptor.getFields().size() == outputSymbols.size(), "Number of outputs (%s) doesn't match descriptor size (%s)", outputSymbols.size(), descriptor.getFields().size());

        this.root = root;
        this.descriptor = descriptor;
        this.outputSymbols = ImmutableList.copyOf(outputSymbols);
    }

    public Symbol getSymbol(int fieldIndex)
    {
        Preconditions.checkArgument(fieldIndex >= 0 && fieldIndex < outputSymbols.size() && outputSymbols.get(fieldIndex) != null, "No field->symbol mapping for field %s", fieldIndex);
        return outputSymbols.get(fieldIndex);
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    public TupleDescriptor getDescriptor()
    {
        return descriptor;
    }
}
