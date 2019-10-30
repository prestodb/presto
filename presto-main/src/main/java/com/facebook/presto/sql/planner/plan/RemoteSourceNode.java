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

import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class RemoteSourceNode
        extends InternalPlanNode
{
    private final List<PlanFragmentId> sourceFragmentIds;
    private final List<VariableReferenceExpression> outputVariables;
    private final boolean ensureSourceOrdering;
    private final Optional<OrderingScheme> orderingScheme;
    private final ExchangeNode.Type exchangeType; // This is needed to "unfragment" to compute stats correctly.

    @JsonCreator
    public RemoteSourceNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("sourceFragmentIds") List<PlanFragmentId> sourceFragmentIds,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables,
            @JsonProperty("ensureSourceOrdering") boolean ensureSourceOrdering,
            @JsonProperty("orderingScheme") Optional<OrderingScheme> orderingScheme,
            @JsonProperty("exchangeType") ExchangeNode.Type exchangeType)
    {
        super(id);

        this.sourceFragmentIds = sourceFragmentIds;
        this.outputVariables = ImmutableList.copyOf(requireNonNull(outputVariables, "outputVariables is null"));
        this.ensureSourceOrdering = ensureSourceOrdering;
        this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
        this.exchangeType = requireNonNull(exchangeType, "exchangeType is null");
    }

    public RemoteSourceNode(
            PlanNodeId id,
            PlanFragmentId sourceFragmentId,
            List<VariableReferenceExpression> outputVariables,
            boolean ensureSourceOrdering,
            Optional<OrderingScheme> orderingScheme,
            ExchangeNode.Type exchangeType)
    {
        this(id, ImmutableList.of(sourceFragmentId), outputVariables, ensureSourceOrdering, orderingScheme, exchangeType);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @JsonProperty
    public List<PlanFragmentId> getSourceFragmentIds()
    {
        return sourceFragmentIds;
    }

    @JsonProperty
    public boolean isEnsureSourceOrdering()
    {
        return ensureSourceOrdering;
    }

    @JsonProperty
    public Optional<OrderingScheme> getOrderingScheme()
    {
        return orderingScheme;
    }

    @JsonProperty
    public ExchangeNode.Type getExchangeType()
    {
        return exchangeType;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitRemoteSource(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return this;
    }
}
