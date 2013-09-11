package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SampledRelation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class SampleNode
        extends PlanNode
{
    private final PlanNode source;
    private final Expression samplePercentage;
    private final Type sampleType;

    public enum Type
    {
        BERNOULLI,
        SYSTEM;

        public static Type typeConvert(SampledRelation.Type sampleType)
        {
            switch (sampleType) {
                case BERNOULLI:
                    return Type.BERNOULLI;
                case SYSTEM:
                    return Type.SYSTEM;
                default:
                    throw new UnsupportedOperationException("Unsupported sample type: " + sampleType);
            }
        }
    }

    @JsonCreator
    public SampleNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("samplePercentage") Expression samplePercentage,
            @JsonProperty("sampleType") Type sampleType)
    {
        super(id);

        this.source = checkNotNull(source, "source is null");
        this.samplePercentage = checkNotNull(samplePercentage);
        this.sampleType = checkNotNull(sampleType);
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

    @JsonProperty
    public Expression getSamplePercentage()
    {
        return samplePercentage;
    }

    @JsonProperty
    public Type getSampleType()
    {
        return sampleType;
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
