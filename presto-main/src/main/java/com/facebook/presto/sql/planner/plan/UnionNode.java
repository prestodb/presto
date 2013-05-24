package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class UnionNode
    extends PlanNode
{
    private final List<PlanNode> sources;
    private final List<Symbol> outputSymbols;

    @JsonCreator
    public UnionNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("sources") List<PlanNode> sources,
            @JsonProperty("outputSymbols") List<Symbol> outputSymbols)
    {
        super(id);

        checkNotNull(sources, "sources is null");
        checkArgument(!sources.isEmpty(), "Must have at least one source");
        checkNotNull(outputSymbols, "outputSymbols is null");

        this.sources = ImmutableList.copyOf(sources);
        this.outputSymbols = ImmutableList.copyOf(outputSymbols);

        for (PlanNode source : this.sources) {
            checkArgument(source.getOutputSymbols().size() == this.outputSymbols.size(), "Must have same number of output symbols as sources");
        }
    }

    @Override
    @JsonProperty("sources")
    public List<PlanNode> getSources()
    {
        return sources;
    }

    @Override
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitUnion(this, context);
    }
}
