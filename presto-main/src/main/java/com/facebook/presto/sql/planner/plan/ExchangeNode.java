package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;
import java.util.List;

@Immutable
public class ExchangeNode
        extends PlanNode
{
    private final List<PlanFragmentId> sourceFragmentIds;
    private final List<Symbol> outputs;

    @JsonCreator
    public ExchangeNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("sourceFragmentIds") List<PlanFragmentId> sourceFragmentIds,
            @JsonProperty("outputs") List<Symbol> outputs)
    {
        super(id);

        Preconditions.checkNotNull(outputs, "outputs is null");

        this.sourceFragmentIds = sourceFragmentIds;
        this.outputs = ImmutableList.copyOf(outputs);
    }

    public ExchangeNode(PlanNodeId id, PlanFragmentId sourceFragmentId, List<Symbol> outputs)
    {
        this(id, ImmutableList.of(sourceFragmentId), outputs);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @JsonProperty("outputs")
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @JsonProperty("sourceFragmentIds")
    public List<PlanFragmentId> getSourceFragmentIds()
    {
        return sourceFragmentIds;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitExchange(this, context);
    }

    public static Function<ExchangeNode, List<PlanFragmentId>> sourceFragmentIdsGetter()
    {
        return new Function<ExchangeNode, List<PlanFragmentId>>()
        {
            @Override
            public List<PlanFragmentId> apply(ExchangeNode input)
            {
                return input.getSourceFragmentIds();
            }
        };
    }
}
