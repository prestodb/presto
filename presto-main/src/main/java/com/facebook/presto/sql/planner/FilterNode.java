package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class FilterNode
    extends PlanNode
{
    private final PlanNode source;
    private final Expression predicate;
    private final List<Slot> outputs;

    public FilterNode(PlanNode source, Expression predicate, List<Slot> outputs)
    {
        this.source = source;
        this.predicate = predicate;
        this.outputs = outputs;
    }

    public Expression getPredicate()
    {
        return predicate;
    }

    @Override
    public List<Slot> getOutputs()
    {
        return outputs;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitFilter(this, context);
    }

}
