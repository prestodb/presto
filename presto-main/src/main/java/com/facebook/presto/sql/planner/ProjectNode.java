package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

public class ProjectNode
        extends PlanNode
{
    private final PlanNode source;
    private final Map<Slot, Expression> outputs;

    public ProjectNode(PlanNode source, Map<Slot, Expression> outputs)
    {
        this.source = source;
        this.outputs = outputs;
    }

    public List<Expression> getExpressions()
    {
        return ImmutableList.copyOf(outputs.values());
    }

    public List<Slot> getOutputs()
    {
        return ImmutableList.copyOf(outputs.keySet());
    }

    public Map<Slot, Expression> getOutputMap()
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
        return visitor.visitProject(this, context);
    }

}
