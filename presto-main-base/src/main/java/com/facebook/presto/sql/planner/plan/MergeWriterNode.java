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
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableWriterNode.MergeTarget;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class MergeWriterNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final MergeTarget target;
    private final List<VariableReferenceExpression> projectedSymbols;
    private final Optional<PartitioningScheme> partitioningScheme;
    private final List<VariableReferenceExpression> outputs;

    @JsonCreator
    public MergeWriterNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") MergeTarget target,
            @JsonProperty("projectedSymbols") List<VariableReferenceExpression> projectedSymbols, // TODO #20578: Rename this parameter to "mergeProcessorProjectedSymbols" or "inputs"
            @JsonProperty("partitioningScheme") Optional<PartitioningScheme> partitioningScheme,
            @JsonProperty("outputs") List<VariableReferenceExpression> outputs)
    {
        this(sourceLocation, id, Optional.empty(), source, target, projectedSymbols, partitioningScheme, outputs);
    }

    public MergeWriterNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            MergeTarget target,
            List<VariableReferenceExpression> projectedSymbols,
            Optional<PartitioningScheme> partitioningScheme,
            List<VariableReferenceExpression> outputs)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);

        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.projectedSymbols = requireNonNull(projectedSymbols, "projectedSymbols is null");
        this.partitioningScheme = requireNonNull(partitioningScheme, "partitioningScheme is null");
        this.outputs = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public MergeTarget getTarget()
    {
        return target;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getProjectedSymbols()
    {
        return projectedSymbols;
    }

    @JsonProperty
    public Optional<PartitioningScheme> getPartitioningScheme()
    {
        return partitioningScheme;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    @JsonProperty("outputs")
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputs;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitMergeWriter(this, context);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new MergeWriterNode(getSourceLocation(), getId(), statsEquivalentPlanNode, source, target,
                projectedSymbols, partitioningScheme, outputs);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new MergeWriterNode(getSourceLocation(), getId(), Iterables.getOnlyElement(newChildren),
                target, projectedSymbols, partitioningScheme, outputs);
    }
}
