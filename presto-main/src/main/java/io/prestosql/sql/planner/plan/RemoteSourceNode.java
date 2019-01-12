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
package io.prestosql.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.sql.planner.OrderingScheme;
import io.prestosql.sql.planner.Symbol;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class RemoteSourceNode
        extends PlanNode
{
    private final List<PlanFragmentId> sourceFragmentIds;
    private final List<Symbol> outputs;
    private final Optional<OrderingScheme> orderingScheme;
    private final ExchangeNode.Type exchangeType; // This is needed to "unfragment" to compute stats correctly.

    @JsonCreator
    public RemoteSourceNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("sourceFragmentIds") List<PlanFragmentId> sourceFragmentIds,
            @JsonProperty("outputs") List<Symbol> outputs,
            @JsonProperty("orderingScheme") Optional<OrderingScheme> orderingScheme,
            @JsonProperty("exchangeType") ExchangeNode.Type exchangeType)
    {
        super(id);

        requireNonNull(outputs, "outputs is null");

        this.sourceFragmentIds = sourceFragmentIds;
        this.outputs = ImmutableList.copyOf(outputs);
        this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
        this.exchangeType = requireNonNull(exchangeType, "exchangeType is null");
    }

    public RemoteSourceNode(PlanNodeId id, PlanFragmentId sourceFragmentId, List<Symbol> outputs, Optional<OrderingScheme> orderingScheme, ExchangeNode.Type exchangeType)
    {
        this(id, ImmutableList.of(sourceFragmentId), outputs, orderingScheme, exchangeType);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    @JsonProperty("outputs")
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @JsonProperty("sourceFragmentIds")
    public List<PlanFragmentId> getSourceFragmentIds()
    {
        return sourceFragmentIds;
    }

    @JsonProperty("orderingScheme")
    public Optional<OrderingScheme> getOrderingScheme()
    {
        return orderingScheme;
    }

    @JsonProperty("exchangeType")
    public ExchangeNode.Type getExchangeType()
    {
        return exchangeType;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
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
