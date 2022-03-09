/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class SampleNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final double sampleRatio;
    private final Type sampleType;

    public enum Type
    {
        BERNOULLI,
        SYSTEM
    }

    @JsonCreator
    public SampleNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("sampleRatio") double sampleRatio,
            @JsonProperty("sampleType") Type sampleType)
    {
        super(sourceLocation, id);

        checkArgument(sampleRatio >= 0.0, "sample ratio must be greater than or equal to 0");
        checkArgument((sampleRatio <= 1.0), "sample ratio must be less than or equal to 1");

        this.sampleType = requireNonNull(sampleType, "sample type is null");
        this.source = requireNonNull(source, "source is null");
        this.sampleRatio = sampleRatio;
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
    public double getSampleRatio()
    {
        return sampleRatio;
    }

    @JsonProperty
    public Type getSampleType()
    {
        return sampleType;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return source.getOutputVariables();
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitSample(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new SampleNode(getSourceLocation(), getId(), Iterables.getOnlyElement(newChildren), sampleRatio, sampleType);
    }
}
