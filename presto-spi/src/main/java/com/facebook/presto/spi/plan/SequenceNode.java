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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SequenceNode
        extends PlanNode
{
    // cteProducers {l1,l2,l3} will be in {l3, l2,l1} order
    private final List<PlanNode> cteProducers;
    private final PlanNode primarySource;

    @JsonCreator
    public SequenceNode(Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId planNodeId,
            @JsonProperty("cteProducers") List<PlanNode> left,
            @JsonProperty("primarySource") PlanNode primarySource)
    {
        this(sourceLocation, planNodeId, Optional.empty(), left, primarySource);
    }

    public SequenceNode(Optional<SourceLocation> sourceLocation,
            PlanNodeId planNodeId,
            Optional<PlanNode> statsEquivalentPlanNode,
            List<PlanNode> leftList,
            PlanNode primarySource)
    {
        super(sourceLocation, planNodeId, statsEquivalentPlanNode);
        this.cteProducers = leftList;
        this.primarySource = primarySource;
    }

    @JsonProperty
    public List<PlanNode> getCteProducers()
    {
        return this.cteProducers;
    }

    @JsonProperty
    public PlanNode getPrimarySource()
    {
        return this.primarySource;
    }

    @Override
    public List<PlanNode> getSources()
    {
        List<PlanNode> children = new ArrayList<>(cteProducers);
        children.add(primarySource);
        return children;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return primarySource.getOutputVariables();
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new SequenceNode(newChildren.get(0).getSourceLocation(), getId(), getStatsEquivalentPlanNode(),
                newChildren.subList(0, newChildren.size() - 1), newChildren.get(newChildren.size() - 1));
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new SequenceNode(getSourceLocation(), getId(), statsEquivalentPlanNode, cteProducers, this.getPrimarySource());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitSequence(this, context);
    }
}
