package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.cost.CoefficientBasedCostCalculator;
import com.facebook.presto.cost.PlanNodeCost;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.joins.JoinGraph;
import com.facebook.presto.sql.planner.plan.*;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Created by hadoop on 17-6-8.
 */
public class JoinOrderRule implements Rule {
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session) {
       // if (!SystemSessionProperties.ISCBOEnabled(session)) {
      //      return Optional.empty();
       // }
        if (!(node instanceof JoinNode)) {
            return Optional.empty();
        }
        JoinGraph joinGraph = JoinGraph.buildShallowFrom(node, lookup);
        if (joinGraph.size() < 2) {
            return Optional.empty();
        }
        List<Integer> joinOrder = getCBOJoinOrder(joinGraph); //genjubiaohangshutizozheng order
        if (isOriginalOrder(joinOrder)) {
            return Optional.empty();
        }

        PlanNode replacement = rebuildJoinTree(node.getOutputSymbols(), joinGraph, joinOrder, idAllocator);
        return Optional.of(replacement);
    }

    public static boolean isOriginalOrder(List<Integer> joinOrder)
    {
        for (int i = 0; i < joinOrder.size(); i++) {
            if (joinOrder.get(i) != i) {
                return false;
            }
        }
        return true;
    }
    public static List<Integer> getCBOJoinOrder(JoinGraph graph) {

        ImmutableList.Builder<PlanNode> joinOrder = ImmutableList.builder();
        Map<PlanNodeId, Integer> priorities = new HashMap<>();
        Map<List<Symbol>,PlanNodeCost> map=CoefficientBasedCostCalculator.getScannerMap();

        Estimate max= new Estimate(0);
        int max_index=0;
        List<Symbol> list_sym=new ArrayList<Symbol>();
        for (int i = 0; i < graph.size(); i++) {
            priorities.put(graph.getNode(i).getId(), i);
             list_sym=graph.getNode(i).getOutputSymbols();
            if(map.get(list_sym).getOutputRowCount().getValue()>max.getValue()){
                max=map.get(list_sym).getOutputRowCount();
                max_index=i;
            }
        }

        PriorityQueue<PlanNode> nodesToVisit = new PriorityQueue<>(
                graph.size(),
                (Comparator<PlanNode>) (node1, node2) -> priorities.get(node1.getId()).compareTo(priorities.get(node2.getId())));
        Set<PlanNode> visited = new HashSet<>();

        nodesToVisit.add(graph.getNode(max_index));

        while (!nodesToVisit.isEmpty()) {
            PlanNode node = nodesToVisit.poll();
            if (!visited.contains(node)) {
                visited.add(node);
                joinOrder.add(node);
                PlanNode nextnode = null;
                Estimate temp = new Estimate(0);

                for (JoinGraph.Edge e : graph.getEdges(node)) {
                    list_sym=e.getTargetNode().getOutputSymbols();
                    if (map.get(list_sym).getOutputRowCount().getValue() > temp.getValue()) {
                        temp = map.get(list_sym).getOutputRowCount();
                        nextnode = e.getTargetNode();
                    }
                }
                if (nextnode != null)
                    nodesToVisit.add(nextnode);
            }
            if (nodesToVisit.isEmpty() && visited.size() < graph.size()) {
                // disconnected graph, find new starting point
                Optional<PlanNode> firstNotVisitedNode = graph.getNodes().stream()
                        .filter(graphNode -> !visited.contains(graphNode))
                        .findFirst();
                if (firstNotVisitedNode.isPresent()) {
                    nodesToVisit.add(firstNotVisitedNode.get());
                }
            }
        }
        checkState(visited.size() == graph.size());
        return joinOrder.build().stream()
                .map(node -> priorities.get(node.getId()))
                .collect(toImmutableList());
    }

    public static PlanNode rebuildJoinTree(List<Symbol> expectedOutputSymbols, JoinGraph graph, List<Integer> joinOrder, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(expectedOutputSymbols, "expectedOutputSymbols is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(graph, "graph is null");
        joinOrder = ImmutableList.copyOf(requireNonNull(joinOrder, "joinOrder is null"));
        checkArgument(joinOrder.size() >= 2);

        PlanNode result = graph.getNode(joinOrder.get(0));
        Set<PlanNodeId> alreadyJoinedNodes = new HashSet<>();
        alreadyJoinedNodes.add(result.getId());

        for (int i = 1; i < joinOrder.size(); i++) {
            PlanNode rightNode = graph.getNode(joinOrder.get(i));
            alreadyJoinedNodes.add(rightNode.getId());

            ImmutableList.Builder<JoinNode.EquiJoinClause> criteria = ImmutableList.builder();

            for (JoinGraph.Edge edge : graph.getEdges(rightNode)) {
                PlanNode targetNode = edge.getTargetNode();
                if (alreadyJoinedNodes.contains(targetNode.getId())) {
                    criteria.add(new JoinNode.EquiJoinClause(
                            edge.getTargetSymbol(),
                            edge.getSourceSymbol()));
                }
            }

            result = new JoinNode(
                    idAllocator.getNextId(),
                    JoinNode.Type.INNER,
                    result,
                    rightNode,
                    criteria.build(),
                    ImmutableList.<Symbol>builder()
                            .addAll(result.getOutputSymbols())
                            .addAll(rightNode.getOutputSymbols())
                            .build(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        List<Expression> filters = graph.getFilters();

        for (Expression filter : filters) {
            result = new FilterNode(
                    idAllocator.getNextId(),
                    result,
                    filter);
        }

        if (graph.getAssignments().isPresent()) {
            result = new ProjectNode(
                    idAllocator.getNextId(),
                    result,
                    Assignments.copyOf(graph.getAssignments().get()));
        }

        if (!result.getOutputSymbols().equals(expectedOutputSymbols)) {
            // Introduce a projection to constrain the outputs to what was originally expected
            // Some nodes are sensitive to what's produced (e.g., DistinctLimit node)
            result = new ProjectNode(
                    idAllocator.getNextId(),
                    result,
                    Assignments.identity(expectedOutputSymbols));
        }

        return result;
    }
}
