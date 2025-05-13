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

import java.util.List;
import java.util.Optional;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

@Immutable
public final class CteProducerNode
        extends PlanNode
{
    private final PlanNode source;
    private final String cteId;
    private final VariableReferenceExpression rowCountVariable;
    private final List<VariableReferenceExpression> originalOutputVariables;

    @JsonCreator
    public CteProducerNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("cteId") String cteId,
            @JsonProperty("rowCountVariable") VariableReferenceExpression rowCountVariable,
            @JsonProperty("originalOutputVariables") List<VariableReferenceExpression> originalOutputVariables)
    {
        this(sourceLocation, id, Optional.empty(), source, cteId, rowCountVariable, originalOutputVariables);
    }

    public CteProducerNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            String cteId,
            VariableReferenceExpression rowCountVariable,
            List<VariableReferenceExpression> originalOutputVariables)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        // Inside your method or constructor
        this.cteId = requireNonNull(cteId, "cteName must not be null");
        this.source = requireNonNull(source, "source must not be null");
        this.rowCountVariable = requireNonNull(rowCountVariable, "rowCountVariable must not be null");
        this.originalOutputVariables = requireNonNull(originalOutputVariables, "originalOutputVariables must not be null");
    }

    @Override
    public List<PlanNode> getSources()
    {
        return singletonList(source);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return originalOutputVariables;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1, "expected newChildren to contain 1 node");
        return new CteProducerNode(newChildren.get(0).getSourceLocation(), getId(), getStatsEquivalentPlanNode(), newChildren.get(0),
                cteId, rowCountVariable, originalOutputVariables);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new CteProducerNode(getSourceLocation(), getId(), statsEquivalentPlanNode, source, cteId, rowCountVariable, originalOutputVariables);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitCteProducer(this, context);
    }

    @JsonProperty
    public String getCteId()
    {
        return cteId;
    }

    public VariableReferenceExpression getRowCountVariable()
    {
        return rowCountVariable;
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
