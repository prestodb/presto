package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.analyzer.Symbol;
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
    private final PlanFragmentId sourceFragmentId;
    private final List<Symbol> outputs;

    @JsonCreator
    public ExchangeNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("sourceFragmentId") PlanFragmentId sourceFragmentId,
            @JsonProperty("outputs") List<Symbol> outputs)
    {
        super(id);

        Preconditions.checkNotNull(outputs, "outputs is null");

        this.sourceFragmentId = sourceFragmentId;
        this.outputs = ImmutableList.copyOf(outputs);
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

    @JsonProperty("sourceFragmentId")
    public PlanFragmentId getSourceFragmentId()
    {
        return sourceFragmentId;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitExchange(this, context);
    }

    public static Function<ExchangeNode, PlanFragmentId> sourceFragmentIdGetter()
    {
        return new Function<ExchangeNode, PlanFragmentId>()
        {
            @Override
            public PlanFragmentId apply(ExchangeNode input)
            {
                return input.getSourceFragmentId();
            }
        };
    }
}
