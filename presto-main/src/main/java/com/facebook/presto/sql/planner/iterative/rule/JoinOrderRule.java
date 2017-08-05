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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spi.ColHistogram;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Pattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.joins.JoinGraph;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
/**
 * Created by hadoop on 17-7-21.
 */
public class JoinOrderRule
        implements Rule
{
    private static final Pattern PATTERN = Pattern.node(JoinNode.class);

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session) {
        if (!(node instanceof JoinNode)) {
            return Optional.empty();
        }
        if (!SystemSessionProperties.isJoinReorderingEnabled(session)) {
            return Optional.empty();
        }
        JoinGraph joinGraph = JoinGraph.buildShallowFrom(node, lookup);

        if (joinGraph.size() < 2) {
            return Optional.empty();
        }


       /* Collections.reverse(joinOrder);
        PlanNode replacement = buildJoinTreeTest(node.getOutputSymbols(), joinGraph, joinOrder, idAllocator);*/

        PlanNode replacement = rewrite(node.getOutputSymbols(), joinGraph, session.getHistograms(), idAllocator);
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

    //根据一个node集合得到一个子图
   public static JoinGraph getJoinGraph(JoinGraph graph,List<PlanNode> nodes){
       if(nodes.size()==1)
           return new JoinGraph(nodes.get(nodes.size()-1));
       ImmutableMultimap.Builder<PlanNodeId, JoinGraph.Edge> edges = ImmutableMultimap.<PlanNodeId, JoinGraph.Edge>builder();
       for (PlanNode p : nodes) {
           for (JoinGraph.Edge edge : graph.getEdges(p)) {
               //if(nodes.subList(i,nodes.size()).contains(edge.getTargetNode()))
               if(nodes.contains(edge.getTargetNode()))
               edges.putAll(p.getId(),edge);
           }
       }
      return new JoinGraph(nodes,edges.build(),nodes.get(nodes.size()-1).getId(),null,Optional.empty());
    }

    //根据左部分表得到左右两部分表的连接关系
    public static List<JoinNode.EquiJoinClause> getEqui(JoinGraph graph,List<PlanNode> list) {
        List<JoinNode.EquiJoinClause> criteria = new ArrayList<JoinNode.EquiJoinClause>();
        for (PlanNode p : list) {
            for (JoinGraph.Edge edge : graph.getEdges(p)) {
                if(!list.contains(edge.getTargetNode()))
                    criteria.add(new JoinNode.EquiJoinClause(
                            edge.getSourceSymbol(),
                            edge.getTargetSymbol()));
            }
        }
        return criteria;
    }

    //完成partitions 函数划分所有可能的子集及其补集对
    public static List<GraphPartitions> partition(JoinGraph graph){
        List<GraphPartitions> partitions=new ArrayList<GraphPartitions>();
        ArrayList<PlanNode> nodes=(ArrayList<PlanNode>)(new ArrayList<PlanNode>(graph.getNodes())).clone();
        ArrayList<PlanNode> list1=new ArrayList<PlanNode>();
        for(PlanNode pn:nodes) {
          //  for(JoinGraph.Edge edge: graph.getEdges(pn)) {
            if(graph.getEdges(pn).size()==1){
                //if(map.containsKey(pn))
                   // map.put(pn,1);
                //if(map.containsKey(edge.getTargetNode())
                    //map.put(edge.getTargetNode(),1))
                //else
                list1.add(pn);
                break;
            }
            else
            continue;
        }
        ArrayList<ArrayList<PlanNode>> res=new ArrayList<ArrayList<PlanNode>>();
        while(list1.size()<nodes.size())
        {
            res.add(new ArrayList(list1));
            GraphPartitions g=new GraphPartitions();
            g.l=getJoinGraph(graph,new ArrayList(list1));
            g.joinClauseList=getEqui(graph,list1);
            nodes.removeAll(list1);
            g.r=getJoinGraph(graph,nodes);
            partitions.add(g);
            nodes=(ArrayList<PlanNode>)(new ArrayList<PlanNode>(graph.getNodes())).clone();
            int t= graph.getEdges(list1.get(list1.size()-1)).size();
            if(t>2) {
                for (JoinGraph.Edge edge : graph.getEdges(list1.get(list1.size() - 1))) {
                    if (!list1.contains(edge.getTargetNode())) {
                        list1.add(edge.getTargetNode());
                        res.add(new ArrayList(list1));
                        g=new GraphPartitions();
                        g.l=getJoinGraph(graph,new ArrayList(list1));
                        g.joinClauseList=getEqui(graph,list1);
                        nodes.removeAll(list1);
                        g.r=getJoinGraph(graph,nodes);
                        partitions.add(g);
                        nodes = (ArrayList<PlanNode>) (new ArrayList<PlanNode>(graph.getNodes())).clone();
                        list1.remove(edge.getTargetNode());
                    }
                }
                for (JoinGraph.Edge edge : graph.getEdges(list1.get(list1.size() - 1))) {
                    if (!list1.contains(edge.getTargetNode())) {
                        list1.add(edge.getTargetNode());
                    }
                }
            }
            else {
                for (JoinGraph.Edge edge : graph.getEdges(list1.get(list1.size() - 1))) {
                    if (!list1.contains(edge.getTargetNode()))
                        list1.add(edge.getTargetNode());
                }
            }
        }
        return partitions;
    }

    public static PlanNode rewrite(List<Symbol> expectedOutputSymbols, JoinGraph graph, Optional<HashMap<String[], ColHistogram>> histograms, PlanNodeIdAllocator idAllocator) {
        Estimate[] best=new Estimate[graph.size()];
        Map<JoinGraph,Estimate> map=new HashMap<JoinGraph,Estimate>();
        //初始化单个node
        PlanNode result = McChyp(graph,histograms,idAllocator);
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

    public static PlanNode McChyp(JoinGraph graph, Optional<HashMap<String[], ColHistogram>> histograms,PlanNodeIdAllocator idAllocator) {
        if(graph.size()<2)
            return  graph.getNodes().get(0);
        List<GraphPartitions> partitions=partition(graph);
        PlanNode result=null;
        GraphPartitions p=partitions.get(0);
        //for(GraphPartitions p:partitions){
            PlanNode  lefttree=McChyp(p.l,histograms,idAllocator);
            PlanNode  righttree=McChyp(p.r,histograms,idAllocator);
            result=buildTree(lefttree,righttree,p,histograms,idAllocator);
       // }
        return result;
    }

    public static PlanNode buildTree(PlanNode pl,PlanNode pr,GraphPartitions p,Optional<HashMap<String[], ColHistogram>> histograms,PlanNodeIdAllocator idAllocator){

        PlanNode result = new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.INNER,
                pl,
                pr,
                p.joinClauseList,
                ImmutableList.<Symbol>builder()
                        .addAll(pl.getOutputSymbols())
                        .addAll(pr.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return result;
    }

    public static List<Integer> getJoinOrder(JoinGraph graph){
        ImmutableList.Builder<PlanNode> joinOrder = ImmutableList.builder();

        Map<PlanNodeId, Integer> priorities = new HashMap<>();
        for (int i = 0; i < graph.size(); i++) {
            priorities.put(graph.getNode(i).getId(), i);
        }

        PriorityQueue<PlanNode> nodesToVisit = new PriorityQueue<>(
                graph.size(),
                (Comparator<PlanNode>) (node1, node2) -> priorities.get(node1.getId()).compareTo(priorities.get(node2.getId())));
        Set<PlanNode> visited = new HashSet<>();

        nodesToVisit.add(graph.getNode(0));

        while (!nodesToVisit.isEmpty()) {
            PlanNode node = nodesToVisit.poll();
            if (!visited.contains(node)) {
                visited.add(node);
                joinOrder.add(node);
                for (JoinGraph.Edge edge : graph.getEdges(node)) {
                    nodesToVisit.add(edge.getTargetNode());
                }
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

    public static PlanNode buildJoinTreeTest(List<Symbol> expectedOutputSymbols, JoinGraph graph, List<Integer> joinOrder, PlanNodeIdAllocator idAllocator){
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
