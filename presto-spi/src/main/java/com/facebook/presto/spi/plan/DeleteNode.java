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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.Utils.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public final class DeleteNode
        extends PlanNode
{
    private final PlanNode source;
    private final VariableReferenceExpression rowId;
    private final List<VariableReferenceExpression> outputVariables;
    private final Optional<InputDistribution> inputDistribution;

    @JsonCreator
    public DeleteNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("rowId") VariableReferenceExpression rowId,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables,
            @JsonProperty("inputDistribution") Optional<InputDistribution> inputDistribution)
    {
        this(sourceLocation, id, Optional.empty(), source, rowId, outputVariables, inputDistribution);
    }

    public DeleteNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            VariableReferenceExpression rowId,
            List<VariableReferenceExpression> outputVariables,
            Optional<InputDistribution> inputDistribution)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);

        this.source = requireNonNull(source, "source is null");
        this.rowId = requireNonNull(rowId, "rowId is null");
        this.outputVariables = Collections.unmodifiableList(new ArrayList<>(requireNonNull(outputVariables, "outputVariables is null")));
        this.inputDistribution = requireNonNull(inputDistribution, "dataPartition is null");
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public VariableReferenceExpression getRowId()
    {
        return rowId;
    }

    @JsonProperty
    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @JsonProperty
    public Optional<InputDistribution> getInputDistribution()
    {
        return inputDistribution;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return Collections.singletonList(source);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitDelete(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1);
        return new DeleteNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), newChildren.get(0), rowId, outputVariables, inputDistribution);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new DeleteNode(getSourceLocation(), getId(), statsEquivalentPlanNode, source, rowId, outputVariables, inputDistribution);
    }

    public interface InputDistribution
    {
        default List<VariableReferenceExpression> getPartitionBy()
        {
            return Collections.emptyList();
        }

        default Optional<OrderingScheme> getOrderingScheme()
        {
            return Optional.empty();
        }

        default List<VariableReferenceExpression> getInputVariables()
        {
            return Collections.emptyList();
        }
    }
}
