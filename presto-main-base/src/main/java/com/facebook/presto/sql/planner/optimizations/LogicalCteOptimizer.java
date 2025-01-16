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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.eventlistener.CTEInformation;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.CteConsumerNode;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.CteReferenceNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.Graph;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.Traverser;
import com.google.common.graph.ValueGraphBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.getCteHeuristicReplicationThreshold;
import static com.facebook.presto.SystemSessionProperties.getCteMaterializationStrategy;
import static com.facebook.presto.SystemSessionProperties.isCteMaterializationApplicable;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.CteMaterializationStrategy.ALL;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.CteMaterializationStrategy.HEURISTIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.CteMaterializationStrategy.HEURISTIC_COMPLEX_QUERIES_ONLY;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/*
 * Transformation of CTE Reference Nodes:
 * This process converts CTE reference nodes into corresponding CteProducers and Consumers.
 * Makes sure that execution deadlocks do not exist
 *
 * Example:
 * Before Transformation:
 *   JOIN
 *   |-- CTEReference(cte2)
 *   |   `-- TABLESCAN2
 *   `-- CTEReference(cte3)
 *       `-- TABLESCAN3
 *
 * After Transformation:
 *   SEQUENCE(cte1)
 *   |-- CTEProducer(cte2)
 *   |   `-- TABLESCAN2
 *   |-- CTEProducer(cte3)
 *   |   `-- TABLESCAN3
 *   `-- JOIN
 *       |-- CTEConsumer(cte2)
 *       `-- CTEConsumer(cte3)
 */
public class LogicalCteOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;

    public LogicalCteOptimizer(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(warningCollector, "warningCollector is null");
        if (!isCteMaterializationApplicable(session)) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }
        CteEnumerator cteEnumerator = new CteEnumerator(idAllocator, variableAllocator);
        PlanNode rewrittenPlan = cteEnumerator.transformPersistentCtes(session, plan);
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, cteEnumerator.isPlanRewritten());
    }

    public class CteEnumerator
    {
        private PlanNodeIdAllocator planNodeIdAllocator;
        private VariableAllocator variableAllocator;

        private boolean isPlanRewritten;

        public CteEnumerator(PlanNodeIdAllocator planNodeIdAllocator, VariableAllocator variableAllocator)
        {
            this.planNodeIdAllocator = requireNonNull(planNodeIdAllocator, "planNodeIdAllocator must not be null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator must not be null");
        }

        public PlanNode transformPersistentCtes(Session session, PlanNode root)
        {
            checkArgument(root.getSources().size() == 1, "expected newChildren to contain 1 node");
            LogicalCteOptimizerContext context = new LogicalCteOptimizerContext();
            determineMaterializationCandidatesAndUpdateContext(session, root, context);
            PlanNode transformedCte = SimplePlanRewriter.rewriteWith(new CteConsumerTransformer(session, planNodeIdAllocator, variableAllocator),
                    root, context);
            List<PlanNode> topologicalOrderedList = context.getTopologicalOrdering();
            if (topologicalOrderedList.isEmpty()) {
                isPlanRewritten = false;
                // Returning transformed Cte because cte reference nodes are cleared in the transformedCte regardless of materialization
                return transformedCte;
            }
            isPlanRewritten = true;
            SequenceNode sequenceNode = new SequenceNode(root.getSourceLocation(),
                    planNodeIdAllocator.getNextId(),
                    topologicalOrderedList,
                    transformedCte.getSources().get(0),
                    context.createIndexedGraphFromTopologicallySortedCteProducers(topologicalOrderedList));
            return root.replaceChildren(Arrays.asList(sequenceNode));
        }

        public boolean isPlanRewritten()
        {
            return isPlanRewritten;
        }

        private void determineMaterializationCandidatesAndUpdateContext(Session session, PlanNode root, LogicalCteOptimizerContext context)
        {
            if (shouldPerformHeuristicAnalysis(session)) {
                performHeuristicAnalysis(session, root, context);
            }
            else {
                markAllCtesForMaterialization(session, context);
            }
        }

        private boolean shouldPerformHeuristicAnalysis(Session session)
        {
            return !getCteMaterializationStrategy(session).equals(ALL);
        }

        private void performHeuristicAnalysis(Session session, PlanNode root, LogicalCteOptimizerContext context)
        {
            WeightedDependencyAnalyzer dependencyAnalyzer = new WeightedDependencyAnalyzer();
            ComplexCteAnalyzer complexCteAnalyzer = new ComplexCteAnalyzer(session);

            root.accept(dependencyAnalyzer, context);
            root.accept(complexCteAnalyzer, context);
            new HeuristicCteMaterializationDeterminer(session).determineHeuristicCandidates(context);
        }

        private void markAllCtesForMaterialization(Session session, LogicalCteOptimizerContext context)
        {
            session.getCteInformationCollector().getCTEInformationList().stream()
                    .map(CTEInformation::getCteId)
                    .forEach(context::addMaterializationCandidate);
        }
    }

    // Checks if the CTE has an underlying JoinNode.class, SemiJoinNode.class, AggregationNode.class
    // The presence of a complex node will mark the nearest parent CTE as complex
    public static class ComplexCteAnalyzer
            extends SimplePlanVisitor<LogicalCteOptimizerContext>
    {
        private final Session session;

        private static final List<Class<? extends PlanNode>> DATA_SOURCES_PLAN_NODES = ImmutableList.of(TableScanNode.class, RemoteSourceNode.class);

        public ComplexCteAnalyzer(Session session)
        {
            this.session = requireNonNull(session, "Session is null");
        }

        @Override
        public Void visitCteReference(CteReferenceNode node, LogicalCteOptimizerContext context)
        {
            context.pushActiveCte(node.getCteId());
            node.getSource().accept(this, context);
            context.popActiveCte();
            // Check for source nodes
            if (context.isComplexCte(node.getCteId()) &&
                    !PlanNodeSearcher.searchFrom(node)
                            .where(planNode -> DATA_SOURCES_PLAN_NODES.stream()
                                    .anyMatch(clazz -> clazz.isInstance(planNode)))
                            .matches()) {
                context.removeComplexCte(node.getCteId());
            }
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, LogicalCteOptimizerContext context)
        {
            Optional<String> parentCte = context.peekActiveCte();
            parentCte.ifPresent(context::addComplexCte);
            return super.visitJoin(node, context);
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, LogicalCteOptimizerContext context)
        {
            Optional<String> parentCte = context.peekActiveCte();
            parentCte.ifPresent(context::addComplexCte);
            return super.visitSemiJoin(node, context);
        }

        @Override
        public Void visitAggregation(AggregationNode node, LogicalCteOptimizerContext context)
        {
            Optional<String> parentCte = context.peekActiveCte();
            parentCte.ifPresent(context::addComplexCte);
            return super.visitAggregation(node, context);
        }
    }

    /**
     * Analyzes the query plan to build a weighted dependency graph for the CTEs
     * The weight on each edge signifies the number of times the child CTE is referenced by the parent
     **/
    public static class WeightedDependencyAnalyzer
            extends SimplePlanVisitor<LogicalCteOptimizerContext>
    {
        private final Set<String> visited;

        public WeightedDependencyAnalyzer()
        {
            visited = new HashSet<>();
        }

        @Override
        public Void visitCteReference(CteReferenceNode node, LogicalCteOptimizerContext context)
        {
            if (visited.contains(node.getCteId())) {
                // already visited so skip traversal but add dependency
                context.addCteReferenceDependency(node.getCteId());
                return null;
            }
            visited.add(node.getCteId());
            context.addCteReferenceDependency(node.getCteId());
            context.pushActiveCte(node.getCteId());
            node.getSource().accept(this, context);
            context.popActiveCte();
            return null;
        }

        @Override
        public Void visitApply(ApplyNode node, LogicalCteOptimizerContext context)
        {
            node.getInput().accept(this, context);
            node.getSubquery().accept(this, context);
            return null;
        }
    }

    /**
     * Selects CTEs for materialization following a greedy heuristic approach.
     * The algorithm greedily prioritizes the earliest parent CTE that meets the heuristic criteria for materialization and then reduces the reference counts for its child CTEs,
     * assuming they are now accessed via the materialized parent.
     * The CTEs selected for materialization by this class adhere to the heuristic conditions, yet they might not represent the most optimal choices due to the nature of the heuristic decision-making process.
     * Example:
     * <p>
     * CTE_A
     * /    \
     * CTE_B  CTE_C
     * |      |
     * CTE_D  CTE_E
     * <p>
     * In this graph, if CTE_B and CTE_C are heavily referenced, the algorithm might choose to materialize these first, reducing the reference count for CTE_D and CTE_E respectively.
     * This means that subsequent decisions will consider the reduced reference count for CTE_D and CTE_E, potentially affecting whether they are materialized.
     */
    public static class HeuristicCteMaterializationDeterminer
    {
        private final Session session;

        public HeuristicCteMaterializationDeterminer(Session session)
        {
            this.session = session;
        }

        private void decrementCteReferenceCount(String cteId, int referencesToRemove)
        {
            HashMap<String, CTEInformation> cteInformationMap = session.getCteInformationCollector().getCteInformationMap();
            CTEInformation cteInfo = cteInformationMap.get(cteId);
            int newReferenceCount = cteInfo.getNumberOfReferences() - referencesToRemove;

            checkArgument(newReferenceCount >= 0, "CTE Reference count for cteId %s should be >= 0", cteId);
            cteInformationMap.put(cteId, new CTEInformation(cteInfo.getCteName(), cteInfo.getCteId(), newReferenceCount, cteInfo.getIsView(), cteInfo.isMaterialized()));
        }

        void rebaseReferences(MutableValueGraph<String, Integer> graph, String cteId, int currentMultiplier, int baseRemovalMultiplier, LogicalCteOptimizerContext context)
        {
            for (String childCte : graph.successors(cteId)) {
                if (!context.shouldCteBeMaterialized(childCte)) {
                    int edgeValue = graph.edgeValueOrDefault(cteId, childCte, 1);
                    int referencesToRemove = baseRemovalMultiplier * currentMultiplier * edgeValue;
                    decrementCteReferenceCount(childCte, referencesToRemove);
                    rebaseReferences(graph, childCte, currentMultiplier * edgeValue, baseRemovalMultiplier, context);
                }
            }
        }

        /**
         * Recursively adjusts the reference counts in the dependency graph due to the materialization of a CTE.
         * This adjustment accounts for the reduced need to recompute the CTEs that are directly or indirectly
         * referenced by the materialized CTE.
         * <p>
         * Example:
         * Let's say A is referenced 3 times in a query and A references B 3 times, and B references C 2 times.
         * The graph would be: Query - (3) - A -(3)-> B -(2)-> C
         * If A was materialized, we would need to adjust B and C's references because their computations are
         * effectively encapsulated by A's materialization.
         * Initial reference counts are 9 for B and 18 for C.
         * The decrement needed would be as follows:
         * - For B, the adjustment would be 2 (A's references) * 3 (times A references B) = 6
         * - For C, following B's adjustment, the adjustment would be 2 (A's references) * 3 (times A references B) * 2 (times B references C) = 12
         * Therefore, the new reference counts would be 3 for B (9 - 6) and 6 for C (18 - 12).
         */
        private void adjustChildReferenceCounts(String parentCteId, int parentReferences, LogicalCteOptimizerContext context)
        {
            int adjustmentFactor = parentReferences - 1;
            checkArgument(adjustmentFactor >= 0, "adjustment count cannot be negative");
            rebaseReferences(context.cteReferenceDependencyGraph, parentCteId, 1, adjustmentFactor, context);
        }

        public void determineHeuristicCandidates(LogicalCteOptimizerContext context)
        {
            MutableValueGraph<String, Integer> cteReferenceDependencyGraph = context.copyOfCteReferenceDependencyGraph();
            HashMap<String, CTEInformation> cteInformationMap = session.getCteInformationCollector().getCteInformationMap();

            // populate vertexes with indegree 0
            List<String> nodesWithInDegreeZero = cteReferenceDependencyGraph.nodes().stream()
                    .filter(node -> cteReferenceDependencyGraph.inDegree(node) == 0)
                    .collect(Collectors.toList());

            while (!nodesWithInDegreeZero.isEmpty()) {
                // traverse these edges and update
                nodesWithInDegreeZero.forEach(cteId -> {
                    CTEInformation cteInfo = cteInformationMap.get(cteId);
                    boolean isAboveThreshold = cteInfo.getNumberOfReferences() >= getCteHeuristicReplicationThreshold(session);
                    boolean isHeuristic = getCteMaterializationStrategy(session).equals(HEURISTIC);
                    boolean isHeuristicComplexOnly = getCteMaterializationStrategy(session).equals(HEURISTIC_COMPLEX_QUERIES_ONLY);
                    boolean isComplexCte = context.isComplexCte(cteInfo.getCteId());

                    if (isAboveThreshold && (isHeuristic || (isHeuristicComplexOnly && isComplexCte))) {
                        // should be materialized
                        context.candidatesForMaterilization.add(cteId);
                        // update child references
                        adjustChildReferenceCounts(cteId, cteInfo.getNumberOfReferences(), context);
                    }
                });

                // Remove these nodes from the graphs
                nodesWithInDegreeZero.forEach(cteReferenceDependencyGraph::removeNode);

                // Refresh the list of nodes with in-degree of zero
                nodesWithInDegreeZero = cteReferenceDependencyGraph.nodes().stream()
                        .filter(node -> cteReferenceDependencyGraph.inDegree(node) == 0)
                        .collect(Collectors.toList());
            }
        }
    }

    public static class CteConsumerTransformer
            extends SimplePlanRewriter<LogicalCteOptimizerContext>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;
        private final Session session;

        public CteConsumerTransformer(Session session, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator must not be null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator must not be null");
            this.session = requireNonNull(session, "Session is null");
        }

        public boolean shouldCteBeMaterialized(String cteId, LogicalCteOptimizerContext context)
        {
            CTEInformation cteInfo = session.getCteInformationCollector().getCteInformationMap().get(cteId);
            boolean shouldBeMaterialized = context.shouldCteBeMaterialized(cteId);
            session.getCteInformationCollector().getCteInformationMap().put(cteId,
                    new CTEInformation(cteInfo.getCteName(), cteInfo.getCteId(), cteInfo.getNumberOfReferences(), cteInfo.getIsView(), shouldBeMaterialized));
            return shouldBeMaterialized;
        }

        @Override
        public PlanNode visitCteReference(CteReferenceNode node, RewriteContext<LogicalCteOptimizerContext> context)
        {
            if (!shouldCteBeMaterialized(node.getCteId(), context.get())) {
                return context.rewrite(node.getSource(), context.get());
            }
            context.get().addMaterializedCteDependency(node.getCteId());
            context.get().pushActiveCte(node.getCteId());
            // So that dependent CTEs are processed properly
            PlanNode actualSource = context.rewrite(node.getSource(), context.get());
            context.get().popActiveCte();
            CteProducerNode cteProducerSource = new CteProducerNode(node.getSourceLocation(),
                    idAllocator.getNextId(),
                    actualSource,
                    node.getCteId(),
                    variableAllocator.newVariable("rows", BIGINT), node.getOutputVariables());
            context.get().addProducer(node.getCteId(), cteProducerSource);
            return new CteConsumerNode(node.getSourceLocation(), idAllocator.getNextId(), Optional.of(actualSource), actualSource.getOutputVariables(), node.getCteId(), actualSource);
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<LogicalCteOptimizerContext> context)
        {
            return new ApplyNode(node.getSourceLocation(),
                    idAllocator.getNextId(),
                    context.rewrite(node.getInput(),
                            context.get()),
                    context.rewrite(node.getSubquery(),
                            context.get()),
                    node.getSubqueryAssignments(),
                    node.getCorrelation(),
                    node.getOriginSubqueryError(),
                    node.getMayParticipateInAntiJoin());
        }
    }

    public static class LogicalCteOptimizerContext
    {
        public Map<String, CteProducerNode> cteProducerMap;

        // a -> b indicates that b needs to be processed before a
        private MutableValueGraph<String, Integer> cteReferenceDependencyGraph;

        // a -> b indicates that a needs to be processed before b
        private MutableGraph<String> materializedCteDependencyGraph;

        private Stack<String> activeCteStack;

        private Set<String> complexCtes;

        private Set<String> candidatesForMaterilization;

        public LogicalCteOptimizerContext()
        {
            cteProducerMap = new HashMap<>();
            // The cte graph will never have cycles because sql won't allow it
            cteReferenceDependencyGraph = ValueGraphBuilder.directed().allowsSelfLoops(false).build();
            materializedCteDependencyGraph = GraphBuilder.directed().allowsSelfLoops(false).build();
            activeCteStack = new Stack<>();
            complexCtes = new HashSet<>();
            candidatesForMaterilization = new HashSet<>();
        }

        public Map<String, CteProducerNode> getCteProducerMap()
        {
            return ImmutableMap.copyOf(cteProducerMap);
        }

        public MutableValueGraph<String, Integer> copyOfCteReferenceDependencyGraph()
        {
            MutableValueGraph<String, Integer> graphCopy = ValueGraphBuilder.from(cteReferenceDependencyGraph).build();
            for (String node : cteReferenceDependencyGraph.nodes()) {
                graphCopy.addNode(node);
            }
            for (String node : cteReferenceDependencyGraph.nodes()) {
                cteReferenceDependencyGraph.successors(node).forEach(successor ->
                        graphCopy.putEdgeValue(node, successor, cteReferenceDependencyGraph.edgeValueOrDefault(node, successor, 0)));
            }
            return cteReferenceDependencyGraph;
        }

        public void addProducer(String cteId, CteProducerNode cteProducer)
        {
            cteProducerMap.putIfAbsent(cteId, cteProducer);
        }

        public void addMaterializationCandidate(String cteId)
        {
            this.candidatesForMaterilization.add(cteId);
        }

        public boolean shouldCteBeMaterialized(String cteId)
        {
            return this.candidatesForMaterilization.contains(cteId);
        }

        public void pushActiveCte(String cte)
        {
            this.activeCteStack.push(cte);
        }

        public String popActiveCte()
        {
            return this.activeCteStack.pop();
        }

        public Optional<String> peekActiveCte()
        {
            return (this.activeCteStack.isEmpty()) ? Optional.empty() : Optional.ofNullable(this.activeCteStack.peek());
        }

        public void addCteReferenceDependency(String currentCte)
        {
            cteReferenceDependencyGraph.addNode(currentCte);
            Optional<String> parentCte = peekActiveCte();
            parentCte.ifPresent(parent -> {
                if (cteReferenceDependencyGraph.hasEdgeConnecting(parent, currentCte)) {
                    // If the edge exists, increment its value
                    int existingWeight = cteReferenceDependencyGraph.edgeValueOrDefault(parent, currentCte, 0);
                    cteReferenceDependencyGraph.putEdgeValue(parent, currentCte, existingWeight + 1);
                }
                else {
                    // If the edge does not exist, create it with a value of 1
                    cteReferenceDependencyGraph.putEdgeValue(parent, currentCte, 1);
                }
            });
        }

        public void addMaterializedCteDependency(String currentCte)
        {
            materializedCteDependencyGraph.addNode(currentCte);
            Optional<String> parentCte = peekActiveCte();
            parentCte.ifPresent(s -> materializedCteDependencyGraph.putEdge(currentCte, s));
        }

        public void addComplexCte(String cteId)
        {
            complexCtes.add(cteId);
        }

        public void removeComplexCte(String cteId)
        {
            complexCtes.remove(cteId);
        }

        public boolean isComplexCte(String cteId)
        {
            return complexCtes.contains(cteId);
        }

        public List<PlanNode> getTopologicalOrdering()
        {
            ImmutableList.Builder<PlanNode> topSortedCteProducerListBuilder = ImmutableList.builder();
            Traverser.forGraph(materializedCteDependencyGraph).depthFirstPostOrder(materializedCteDependencyGraph.nodes())
                    .forEach(cteId -> topSortedCteProducerListBuilder.add(cteProducerMap.get(cteId)));
            return topSortedCteProducerListBuilder.build();
        }

        public Graph<Integer> createIndexedGraphFromTopologicallySortedCteProducers(List<PlanNode> topologicalSortedCteProducerList)
        {
            Map<String, Integer> cteIdToProducerIndexMap = new HashMap<>();
            MutableGraph<Integer> indexGraph = GraphBuilder
                    .directed()
                    .expectedNodeCount(topologicalSortedCteProducerList.size())
                    .build();
            for (int i = 0; i < topologicalSortedCteProducerList.size(); i++) {
                cteIdToProducerIndexMap.put(((CteProducerNode) topologicalSortedCteProducerList.get(i)).getCteId(), i);
                indexGraph.addNode(i);
            }

            // Populate the new graph with edges based on the index mapping
            for (String cteId : materializedCteDependencyGraph.nodes()) {
                materializedCteDependencyGraph.successors(cteId).forEach(successor ->
                        indexGraph.putEdge(cteIdToProducerIndexMap.get(cteId), cteIdToProducerIndexMap.get(successor)));
            }
            return indexGraph;
        }
    }
}
