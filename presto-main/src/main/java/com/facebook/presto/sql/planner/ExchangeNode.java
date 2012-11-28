package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Symbol;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

public class ExchangeNode
        extends PlanNode
{
    private final int sourceFragmentId;
    private final List<Symbol> outputs;

    @JsonCreator
    public ExchangeNode(@JsonProperty("sourceFragmentId") int sourceFragmentId, @JsonProperty("outputs") List<Symbol> outputs)
    {
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
}
