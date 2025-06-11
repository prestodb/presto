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

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public final class CteConsumerNode
        extends PlanNode
{
    private final String cteId;
    private final List<VariableReferenceExpression> originalOutputVariables;
    private final PlanNode originalSource;

    @JsonCreator
    public CteConsumerNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputvars") List<VariableReferenceExpression> originalOutputVariables,
            @JsonProperty("cteId") String cteId,
            @JsonProperty("originalSource") PlanNode originalSource)
    {
        this(sourceLocation, id, Optional.empty(), originalOutputVariables, cteId, originalSource);
    }

    public CteConsumerNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            List<VariableReferenceExpression> originalOutputVariables,
            String cteId,
            PlanNode originalSource)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        this.cteId = requireNonNull(cteId, "cteName must not be null");
        this.originalOutputVariables = requireNonNull(originalOutputVariables, "originalOutputVariables must not be null");
        this.originalSource = originalSource;
    }

    @Override
    public List<PlanNode> getSources()
    {
        // CteConsumer should be the leaf node
        return Collections.emptyList();
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return originalOutputVariables;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 0, "expected newChildren to contain 0 node");
        return this;
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new CteConsumerNode(getSourceLocation(), getId(),
                statsEquivalentPlanNode, originalOutputVariables, cteId, originalSource);
    }

    public PlanNode getOriginalSource()
    {
        return originalSource;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitCteConsumer(this, context);
    }

    @JsonProperty
    public String getCteId()
    {
        return cteId;
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
