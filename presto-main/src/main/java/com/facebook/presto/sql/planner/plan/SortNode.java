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
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class SortNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final OrderingScheme orderingScheme;
    private final boolean isPartial;

    @JsonCreator
    public SortNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("orderingScheme") OrderingScheme orderingScheme,
            @JsonProperty("isPartial") boolean isPartial)
    {
        super(sourceLocation, id);

        requireNonNull(source, "source is null");
        requireNonNull(orderingScheme, "orderingScheme is null");

        this.source = source;
        this.orderingScheme = orderingScheme;
        this.isPartial = isPartial;
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

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return source.getOutputVariables();
    }

    @JsonProperty("orderingScheme")
    public OrderingScheme getOrderingScheme()
    {
        return orderingScheme;
    }

    @JsonProperty("isPartial")
    public boolean isPartial()
    {
        return isPartial;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitSort(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new SortNode(getSourceLocation(), getId(), Iterables.getOnlyElement(newChildren), orderingScheme, isPartial);
    }

    public SortNode deepCopy(
            PlanNodeIdAllocator planNodeIdAllocator,
            VariableAllocator variableAllocator,
            Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings)
    {
        getOutputVariables().stream().forEach(v -> variableMappings.put(v, variableAllocator.newVariable(v.getSourceLocation(), v.getName(), v.getType())));
        PlanNode sourcesDeepCopy = getSource().deepCopy(planNodeIdAllocator, variableAllocator, variableMappings);
        OrderingScheme orderingSchemeDeepCopy = new OrderingScheme(getOrderingScheme().getOrderBy().stream()
                .map(ordering -> new Ordering(variableMappings.get(ordering.getVariable()), ordering.getSortOrder()))
                .collect(Collectors.toList()));
        return new SortNode(
                getSourceLocation(),
                planNodeIdAllocator.getNextId(),
                sourcesDeepCopy,
                orderingSchemeDeepCopy,
                isPartial());
    }
}
