package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class SinkNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<Symbol> outputSymbols; // Expected output symbol layout

    @JsonCreator
    public SinkNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("outputSymbols") List<Symbol> outputSymbols)
    {
        super(id);

        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(outputSymbols, "outputSymbols is null");

        this.source = source;
        this.outputSymbols = ImmutableList.copyOf(outputSymbols);

        Preconditions.checkArgument(source.getOutputSymbols().containsAll(this.outputSymbols), "Source output needs to be able to produce all of the required outputSymbols");
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
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitSink(this, context);
    }
}
