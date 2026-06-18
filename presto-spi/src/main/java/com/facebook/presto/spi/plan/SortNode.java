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

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class SortNode
        extends PlanNode
{
    private final PlanNode source;
    private final OrderingScheme orderingScheme;
    private final boolean isPartial;
    // Sort node will sort within each partition, empty partitionBy means global sort
    private final List<VariableReferenceExpression> partitionBy;

    @JsonCreator
    public SortNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("orderingScheme") OrderingScheme orderingScheme,
            @JsonProperty("isPartial") boolean isPartial,
            @JsonProperty("partitionBy") List<VariableReferenceExpression> partitionBy)
    {
        this(sourceLocation, id, Optional.empty(), source, orderingScheme, isPartial, partitionBy);
    }

    public SortNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            OrderingScheme orderingScheme,
            boolean isPartial,
            List<VariableReferenceExpression> partitionBy)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);

        requireNonNull(source, "source is null");
        requireNonNull(orderingScheme, "orderingScheme is null");

        this.source = source;
        this.orderingScheme = orderingScheme;
        this.isPartial = isPartial;
        this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
    }

    @Override
    public List<PlanNode> getSources()
    {
        return Collections.singletonList(source);
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public LogicalProperties computeLogicalProperties(LogicalPropertiesProvider logicalPropertiesProvider)
    {
        requireNonNull(logicalPropertiesProvider, "logicalPropertiesProvider cannot be null.");
        return logicalPropertiesProvider.getSortProperties(this);
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

    @JsonProperty("partitionBy")
    public List<VariableReferenceExpression> getPartitionBy()
    {
        return partitionBy;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitSort(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1, "Unexpected number of elements in list newChildren");
        return new SortNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), newChildren.get(0), orderingScheme, isPartial, partitionBy);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new SortNode(getSourceLocation(), getId(), statsEquivalentPlanNode, source, orderingScheme, isPartial, partitionBy);
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
