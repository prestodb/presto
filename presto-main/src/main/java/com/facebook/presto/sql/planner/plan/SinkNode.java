package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.analyzer.Symbol;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

import static com.google.common.base.Preconditions.*;

public class SinkNode
    extends PlanNode
{
    private final PlanNode source;

    @JsonCreator
    public SinkNode(@JsonProperty("id") PlanNodeId id, @JsonProperty("source") PlanNode source)
    {
        super(id);

        Preconditions.checkNotNull(source, "source is null");

        this.source = source;
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitSink(this, context);
    }
}
