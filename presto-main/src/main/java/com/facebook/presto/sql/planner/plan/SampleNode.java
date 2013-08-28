package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;
import java.util.List;

@Immutable
public class SampleNode
        extends PlanNode {
    private final PlanNode source;
    private final double samplingRatio;

    @JsonCreator
    public SampleNode(@JsonProperty("id") PlanNodeId id, @JsonProperty("source") PlanNode source, @JsonProperty("samplingRatio") double samplingRatio)
    {
        super(id);

        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkArgument(samplingRatio >= 0.0 && samplingRatio <= 1.0, "sampling ratio must be between zero and one");

        this.source = source;
        this.samplingRatio = samplingRatio;
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

    @JsonProperty("samplingRatio")
    public double getSamplingRatio()
    {
        return samplingRatio;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitSample(this, context);
    }
}
