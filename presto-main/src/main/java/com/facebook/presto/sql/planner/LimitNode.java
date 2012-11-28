package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Symbol;
import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

public class LimitNode
        extends PlanNode
{
    private final PlanNode source;
    private final long count;

    @JsonCreator
    public LimitNode(@JsonProperty("source") PlanNode source, @JsonProperty("count") long count)
    {
        this.source = source;
        this.count = count;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty("count")
    public long getCount()
    {
        return count;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitLimit(this, context);
    }
}
