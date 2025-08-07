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
import com.facebook.presto.spi.plan.TableWriterNode.MergeTarget;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The node processes the result of the Searched CASE and RIGHT JOIN
 * derived from a MERGE statement.
 */
public class MergeProcessorNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final MergeTarget target;
    private final VariableReferenceExpression rowIdSymbol;
    private final VariableReferenceExpression mergeRowSymbol;
    private final List<VariableReferenceExpression> dataColumnSymbols;
    private final List<VariableReferenceExpression> redistributionColumnSymbols;
    private final List<VariableReferenceExpression> outputs;

    @JsonCreator
    public MergeProcessorNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") MergeTarget target,
            @JsonProperty("rowIdSymbol") VariableReferenceExpression rowIdSymbol,
            @JsonProperty("mergeRowSymbol") VariableReferenceExpression mergeRowSymbol,
            @JsonProperty("dataColumnSymbols") List<VariableReferenceExpression> dataColumnSymbols,
            @JsonProperty("redistributionColumnSymbols") List<VariableReferenceExpression> redistributionColumnSymbols,
            @JsonProperty("outputs") List<VariableReferenceExpression> outputs)
    {
        this(sourceLocation, id, Optional.empty(), source, target, rowIdSymbol, mergeRowSymbol, dataColumnSymbols, redistributionColumnSymbols, outputs);
    }

    public MergeProcessorNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            MergeTarget target,
            VariableReferenceExpression rowIdSymbol,
            VariableReferenceExpression mergeRowSymbol,
            List<VariableReferenceExpression> dataColumnSymbols,
            List<VariableReferenceExpression> redistributionColumnSymbols,
            List<VariableReferenceExpression> outputs)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);

        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.mergeRowSymbol = requireNonNull(mergeRowSymbol, "mergeRowSymbol is null");
        this.rowIdSymbol = requireNonNull(rowIdSymbol, "rowIdSymbol is null");
        this.dataColumnSymbols = requireNonNull(dataColumnSymbols, "dataColumnSymbols is null");
        this.redistributionColumnSymbols = requireNonNull(redistributionColumnSymbols, "redistributionColumnSymbols is null");
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
    public VariableReferenceExpression getMergeRowSymbol()
    {
        return mergeRowSymbol;
    }

    @JsonProperty
    public VariableReferenceExpression getRowIdSymbol()
    {
        return rowIdSymbol;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getDataColumnSymbols()
    {
        return dataColumnSymbols;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getRedistributionColumnSymbols()
    {
        return redistributionColumnSymbols;
    }

    @JsonProperty("outputs")
    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputs;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitMergeProcessor(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new MergeProcessorNode(getSourceLocation(), getId(), Iterables.getOnlyElement(newChildren),
                target, rowIdSymbol, mergeRowSymbol, dataColumnSymbols, redistributionColumnSymbols, outputs);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new MergeProcessorNode(getSourceLocation(), getId(), statsEquivalentPlanNode, source, target,
                rowIdSymbol, mergeRowSymbol, dataColumnSymbols, redistributionColumnSymbols, outputs);
    }
}
