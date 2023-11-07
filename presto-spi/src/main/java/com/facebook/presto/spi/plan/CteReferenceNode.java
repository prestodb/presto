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

@Immutable
public final class CteReferenceNode
        extends PlanNode
{
    private final PlanNode source;
    private final String cteName;

    @JsonCreator
    public CteReferenceNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("cteName") String cteName)
    {
        this(sourceLocation, id, Optional.empty(), source, cteName);
    }

    public CteReferenceNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            String cteName)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        this.cteName = cteName;
        this.source = source;
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
        return source.getOutputVariables();
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1, "expected newChildren to contain 1 node");
        return new CteReferenceNode(newChildren.get(0).getSourceLocation(), getId(), getStatsEquivalentPlanNode(), newChildren.get(0), cteName);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new CteReferenceNode(getSourceLocation(), getId(), statsEquivalentPlanNode, source, cteName);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitCteReference(this, context);
    }

    public String getCteName()
    {
        return cteName;
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
