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
import com.facebook.presto.spi.plan.CteConsumerNode;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.CteReferenceNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SequenceNode;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.Traverser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;

import static com.facebook.presto.SystemSessionProperties.getCteMaterializationStrategy;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.CteMaterializationStrategy.ALL;
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
        if (!getCteMaterializationStrategy(session).equals(ALL)
                || session.getCteInformationCollector().getCTEInformationList().stream().noneMatch(CTEInformation::isMaterialized)) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }
        CteEnumerator cteEnumerator = new CteEnumerator(idAllocator, variableAllocator);
        PlanNode rewrittenPlan = cteEnumerator.transformPersistentCtes(plan);
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

        public PlanNode transformPersistentCtes(PlanNode root)
        {
            checkArgument(root.getSources().size() == 1, "expected newChildren to contain 1 node");
            CteTransformerContext context = new CteTransformerContext();
            PlanNode transformedCte = SimplePlanRewriter.rewriteWith(new CteConsumerTransformer(planNodeIdAllocator, variableAllocator),
                    root, context);
            List<PlanNode> topologicalOrderedList = context.getTopologicalOrdering();
            if (topologicalOrderedList.isEmpty()) {
                isPlanRewritten = false;
                return root;
            }
            isPlanRewritten = true;
            SequenceNode sequenceNode = new SequenceNode(root.getSourceLocation(), planNodeIdAllocator.getNextId(), topologicalOrderedList,
                    transformedCte.getSources().get(0));
            return root.replaceChildren(Arrays.asList(sequenceNode));
        }

        public boolean isPlanRewritten()
        {
            return isPlanRewritten;
        }
    }

    public class CteConsumerTransformer
            extends SimplePlanRewriter<CteTransformerContext>
    {
        private final PlanNodeIdAllocator idAllocator;

        private final VariableAllocator variableAllocator;

        public CteConsumerTransformer(PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator must not be null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator must not be null");
        }

        @Override
        public PlanNode visitCteReference(CteReferenceNode node, RewriteContext<CteTransformerContext> context)
        {
            context.get().addDependency(node.getCteName());
            context.get().pushActiveCte(node.getCteName());
            // So that dependent CTEs are processed properly
            PlanNode actualSource = context.rewrite(node.getSource(), context.get());
            context.get().popActiveCte();
            CteProducerNode cteProducerSource = new CteProducerNode(node.getSourceLocation(),
                    idAllocator.getNextId(),
                    actualSource,
                    node.getCteName(),
                    variableAllocator.newVariable("rows", BIGINT), node.getOutputVariables());
            context.get().addProducer(node.getCteName(), cteProducerSource);
            return new CteConsumerNode(node.getSourceLocation(), idAllocator.getNextId(), actualSource.getOutputVariables(), node.getCteName());
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<CteTransformerContext> context)
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
        }}

    public class CteTransformerContext
    {
        public Map<String, CteProducerNode> cteProducerMap;

        // a -> b indicates that b needs to be processed before a
        MutableGraph<String> graph;
        public Stack<String> activeCteStack;

        public CteTransformerContext()
        {
            cteProducerMap = new HashMap<>();
            // The cte graph will never have cycles because sql won't allow it
            graph = GraphBuilder.directed().build();
            activeCteStack = new Stack<>();
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
            graph.addNode(currentCte);
            Optional<String> parentCte = peekActiveCte();
            parentCte.ifPresent(s -> graph.putEdge(currentCte, s));
        }

        public List<PlanNode> getTopologicalOrdering()
        {
            ImmutableList.Builder<PlanNode> topSortedCteProducerListBuilder = ImmutableList.builder();
            Traverser.forGraph(graph).depthFirstPostOrder(graph.nodes())
                    .forEach(cteName -> topSortedCteProducerListBuilder.add(cteProducerMap.get(cteName)));
            return topSortedCteProducerListBuilder.build();
        }
    }
}
