package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

public class ProjectNode
        extends PlanNode
{
    private final PlanNode source;
    private final Map<Symbol, Expression> outputs;

    public ProjectNode(PlanNode source, Map<Symbol, Expression> outputs)
    {
        this.source = source;
        this.outputs = outputs;
    }

    public List<Expression> getExpressions()
    {
        return ImmutableList.copyOf(outputs.values());
    }

    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.copyOf(outputs.keySet());
    }

    public Map<Symbol, Expression> getOutputMap()
    {
        return outputs;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    public PlanNode getSource()
    {
        return source;
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitProject(this, context);
    }

}
