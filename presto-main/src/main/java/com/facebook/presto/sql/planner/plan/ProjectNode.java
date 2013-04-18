package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

@Immutable
public class ProjectNode
        extends PlanNode
{
    private final PlanNode source;
    private final Map<Symbol, Expression> outputs;

    // TODO: pass in the "assignments" and the "outputs" separately (i.e., get rid if the symbol := symbol idiom)
    @JsonCreator
    public ProjectNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("assignments") Map<Symbol, Expression> outputs)
    {
        super(id);

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

    @JsonProperty("assignments")
    public Map<Symbol, Expression> getOutputMap()
    {
        return outputs;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitProject(this, context);
    }

}
