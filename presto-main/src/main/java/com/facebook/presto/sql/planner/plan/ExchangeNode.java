package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.analyzer.Symbol;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.List;

@Immutable
public class ExchangeNode
        extends PlanNode
{
    private final int sourceFragmentId;
    private final List<Symbol> outputs;

    @JsonCreator
    public ExchangeNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("sourceFragmentId") int sourceFragmentId,
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
    public int getSourceFragmentId()
    {
        return sourceFragmentId;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitExchange(this, context);
    }

    public static Function<ExchangeNode, Integer> sourceFragmentIdGetter()
    {
        return new Function<ExchangeNode, Integer>()
        {
            @Override
            public Integer apply(ExchangeNode input)
            {
                return input.getSourceFragmentId();
            }
        };
    }
}
