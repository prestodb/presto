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
import java.util.Set;

import static com.facebook.presto.spi.plan.CteConsumerNode.checkArgument;

public class SequenceNode
        extends PlanNode
{
    // cteProducers {l1,l2,l3} will be executed in {l3, l2,l1} order
    private final List<PlanNode> cteProducers;
    private final PlanNode primarySource;

    // Marks index of the cte producer where a dependent subgraph ends
    private final Set<Integer> markerSet;

    @JsonCreator
    public SequenceNode(Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId planNodeId,
            @JsonProperty("cteProducers") List<PlanNode> cteProducers,
            @JsonProperty("primarySource") PlanNode primarySource,
            @JsonProperty("markerSet") Set<Integer> markerSet)
    {
        this(sourceLocation, planNodeId, Optional.empty(), cteProducers, primarySource, markerSet);
    }

    public SequenceNode(Optional<SourceLocation> sourceLocation,
            PlanNodeId planNodeId,
            Optional<PlanNode> statsEquivalentPlanNode,
            List<PlanNode> cteProducers,
            PlanNode primarySource,
            Set<Integer> markerSet)
    {
        super(sourceLocation, planNodeId, statsEquivalentPlanNode);
        this.cteProducers = cteProducers;
        this.primarySource = primarySource;
        this.markerSet = markerSet;
    }

    @JsonProperty
    public Set<Integer> getMarkerSet()
    {
        return markerSet;
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
        checkArgument(newChildren.size() == cteProducers.size() + 1, "expected newChildren to contain same number of nodes as current." +
                " If the child count decreased please update the markers");
        return new SequenceNode(newChildren.get(0).getSourceLocation(), getId(), getStatsEquivalentPlanNode(),
                newChildren.subList(0, newChildren.size() - 1), newChildren.get(newChildren.size() - 1), markerSet);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new SequenceNode(getSourceLocation(), getId(), statsEquivalentPlanNode, cteProducers, this.getPrimarySource(), markerSet);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitSequence(this, context);
    }

    public List<List<PlanNode>> getIndependentCteProducers()
    {
        List<List<PlanNode>> independentCteProducerList = new ArrayList<>();
        List<PlanNode> currentCteProducerSubgraph = new ArrayList<>();
        for (int i = 0; i < cteProducers.size(); i++) {
            currentCteProducerSubgraph.add(cteProducers.get(i));
            if (markerSet.contains(i)) {
                // This is the end of the current subgraph
                if (!currentCteProducerSubgraph.isEmpty()) {
                    independentCteProducerList.add(new ArrayList<>(currentCteProducerSubgraph));
                    currentCteProducerSubgraph.clear();
                }
            }
        }
        if (!currentCteProducerSubgraph.isEmpty()) {
            independentCteProducerList.add(currentCteProducerSubgraph);
        }
        return independentCteProducerList;
    }
}
