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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.graph.Graph;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.ImmutableGraph;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.Traverser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;

public class SequenceNode
        extends InternalPlanNode
{
    // cteProducers {l1,l2,l3} will be executed in {l3, l2,l1} order
    private final List<PlanNode> cteProducers;
    private final PlanNode primarySource;

    // Directed graph of cte Producer Indexes (0 indexed)
    // a -> b indicates that producer at index a needs to be processed before producer at index b
    private final Graph<Integer> cteDependencyGraph;

    @JsonCreator
    public SequenceNode(Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId planNodeId,
            @JsonProperty("cteProducers") List<PlanNode> cteProducerList,
            @JsonProperty("primarySource") PlanNode primarySource,
            Graph<Integer> cteDependencyGraph)
    {
        this(sourceLocation, planNodeId, Optional.empty(), cteProducerList, primarySource, cteDependencyGraph);
    }

    public SequenceNode(Optional<SourceLocation> sourceLocation,
            PlanNodeId planNodeId,
            Optional<PlanNode> statsEquivalentPlanNode,
            List<PlanNode> cteProducerList,
            PlanNode primarySource,
            Graph<Integer> cteDependencyGraph)
    {
        super(sourceLocation, planNodeId, statsEquivalentPlanNode);
        this.cteProducers = ImmutableList.copyOf(cteProducerList);
        this.primarySource = primarySource;
        checkArgument(cteDependencyGraph.isDirected(), "Sequence Node expects a directed graph");
        IntStream.range(0, cteProducerList.size())
                .forEach(i -> checkArgument(cteDependencyGraph.nodes().contains(i),
                        "Sequence Node Error: The cteProducer at index " + i + " is missing in the cte dependency graph."));
        this.cteDependencyGraph = ImmutableGraph.copyOf(cteDependencyGraph);
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
                " If the child count please update the dependency graph");
        return new SequenceNode(newChildren.get(0).getSourceLocation(), getId(), getStatsEquivalentPlanNode(),
                newChildren.subList(0, newChildren.size() - 1), newChildren.get(newChildren.size() - 1), cteDependencyGraph);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new SequenceNode(getSourceLocation(), getId(), statsEquivalentPlanNode, cteProducers, this.getPrimarySource(), cteDependencyGraph);
    }

    public Graph<Integer> getCteDependencyGraph()
    {
        return cteDependencyGraph;
    }

    // Returns a Graph after removing indexes
    public Graph<Integer> removeCteProducersFromCteDependencyGraph(Set<Integer> indexesToRemove)
    {
        if (indexesToRemove.isEmpty()) {
            return ImmutableGraph.copyOf(getCteDependencyGraph());
        }
        Graph<Integer> originalGraph = getCteDependencyGraph();
        MutableGraph newCteDependencyGraph = GraphBuilder.from(getCteDependencyGraph()).build();
        Map<Integer, Integer> indexMapping = new HashMap<>();
        // update the dependency graph remove the indexes from dependency graph
        int removed = 0;
        for (int prevIndex = 0; prevIndex < cteProducers.size(); prevIndex++) {
            if (indexesToRemove.contains(prevIndex)) {
                removed++;
            }
            else {
                int newIndex = prevIndex - removed;
                indexMapping.put(prevIndex, newIndex);
            }
        }
        for (int oldIndex : originalGraph.nodes()) {
            if (!indexesToRemove.contains(oldIndex)) {
                Integer newIndex = indexMapping.get(oldIndex);
                newCteDependencyGraph.addNode(newIndex);
                for (Integer successor : originalGraph.successors(oldIndex)) {
                    if (!indexesToRemove.contains(successor)) {
                        Integer newSuccessorIndex = indexMapping.get(successor);
                        newCteDependencyGraph.putEdge(newIndex, newSuccessorIndex);
                    }
                }
            }
        }
        return ImmutableGraph.copyOf(newCteDependencyGraph);
    }

    public List<List<PlanNode>> getIndependentCteProducers()
    {
        MutableGraph<Integer> undirectedDependencyGraph = GraphBuilder.undirected().allowsSelfLoops(false).build();
        cteDependencyGraph.nodes().forEach(undirectedDependencyGraph::addNode);
        cteDependencyGraph.edges().forEach(edge -> undirectedDependencyGraph.putEdge(edge.nodeU(), edge.nodeV()));

        Set<Integer> visitedCteSet = new HashSet<>();
        ImmutableList.Builder<List<PlanNode>> independentCteProducerList = ImmutableList.builder();
        // Construct Subgraphs
        List<PlanNode> result = new ArrayList<>();
        for (Integer cteIndex : cteDependencyGraph.nodes()) {
            if (!visitedCteSet.contains(cteIndex)) {
                // Identify all nodes in the current connected component
                Set<Integer> componentNodes = new HashSet<>();
                Traverser.forGraph(undirectedDependencyGraph).breadthFirst(cteIndex).forEach(componentNode -> {
                    if (visitedCteSet.add(componentNode)) {
                        componentNodes.add(componentNode);
                    }
                });

                List<Integer> topSortedCteProducerList = new ArrayList<>();
                Traverser.forGraph(cteDependencyGraph).depthFirstPostOrder(componentNodes)
                        .forEach(topSortedCteProducerList::add);
                if (!topSortedCteProducerList.isEmpty()) {
                    independentCteProducerList.add(topSortedCteProducerList.stream().map(index -> cteProducers.get(index)).collect(Collectors.toList()));
                }
            }
        }
        return independentCteProducerList.build();
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitSequence(this, context);
    }
}
