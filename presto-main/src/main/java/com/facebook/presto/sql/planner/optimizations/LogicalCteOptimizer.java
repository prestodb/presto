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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.Graph;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.Traverser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;

import static com.facebook.presto.SystemSessionProperties.getCteHeuristicReplicationThreshold;
import static com.facebook.presto.SystemSessionProperties.getCteMaterializationStrategy;
import static com.facebook.presto.SystemSessionProperties.isCteMaterializationApplicable;
import static com.facebook.presto.common.type.BigintType.BIGINT;
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
            if (getCteMaterializationStrategy(session).equals(HEURISTIC_COMPLEX_QUERIES_ONLY)) {
                // Mark all Complex CTES and store info in the context
                root.accept(new ComplexCteAnalyzer(session), context);
            }
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
            HashMap<String, CTEInformation> cteInformationMap = session.getCteInformationCollector().getCteInformationMap();
            CTEInformation cteInfo = cteInformationMap.get(cteId);
            switch (getCteMaterializationStrategy(session)) {
                case HEURISTIC_COMPLEX_QUERIES_ONLY:
                    if (!context.isComplexCte(cteId)) {
                        break;
                    }
                case HEURISTIC:
                    if (cteInfo.getNumberOfReferences() < getCteHeuristicReplicationThreshold(session)) {
                        break;
                    }
                case ALL:
                    return true;
            }
            // do not materialize and update the cteInfo
            cteInformationMap.put(cteId,
                    new CTEInformation(cteInfo.getCteName(), cteInfo.getCteId(), cteInfo.getNumberOfReferences(), cteInfo.getIsView(), false));
            return false;
        }

        @Override
        public PlanNode visitCteReference(CteReferenceNode node, RewriteContext<LogicalCteOptimizerContext> context)
        {
            if (!shouldCteBeMaterialized(node.getCteId(), context.get())) {
                return context.rewrite(node.getSource(), context.get());
            }
            context.get().addDependency(node.getCteId());
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

        // a -> b indicates that a needs to be processed before b
        private MutableGraph<String> cteDependencyGraph;

        private Stack<String> activeCteStack;

        private Set<String> complexCtes;

        public LogicalCteOptimizerContext()
        {
            cteProducerMap = new HashMap<>();
            // The cte graph will never have cycles because sql won't allow it
            cteDependencyGraph = GraphBuilder.directed().build();
            activeCteStack = new Stack<>();
            complexCtes = new HashSet<>();
        }

        public Map<String, CteProducerNode> getCteProducerMap()
        {
            return ImmutableMap.copyOf(cteProducerMap);
        }

        public void addProducer(String cteName, CteProducerNode cteProducer)
        {
            cteProducerMap.putIfAbsent(cteName, cteProducer);
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

        public void addDependency(String currentCte)
        {
            cteDependencyGraph.addNode(currentCte);
            Optional<String> parentCte = peekActiveCte();
            // (current -> parentCte) this indicates that currentCte must be processed first
            parentCte.ifPresent(s -> cteDependencyGraph.putEdge(currentCte, s));
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
            Traverser.forGraph(cteDependencyGraph).depthFirstPostOrder(cteDependencyGraph.nodes())
                    .forEach(cteName -> topSortedCteProducerListBuilder.add(cteProducerMap.get(cteName)));
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
            for (String cteId : cteDependencyGraph.nodes()) {
                cteDependencyGraph.successors(cteId).forEach(successor ->
                        indexGraph.putEdge(cteIdToProducerIndexMap.get(cteId), cteIdToProducerIndexMap.get(successor)));
            }
            return indexGraph;
        }
    }
}
