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
import com.facebook.presto.spi.plan.CteConsumerNode;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.rule.SimplifyRowExpressions;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.getCteFilterAndProjectionPushdownEnabled;
import static com.facebook.presto.SystemSessionProperties.isCteMaterializationApplicable;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.PlannerUtils.isConstant;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/*
 * CteProjectionAndPredicatePushDown Transformation:
 * This optimizer collects predicates and projections on top of CTE consumers and pushes them into the CTE producer.
 *
 * Example:
 * Before Transformation:
 *   CTEProducer(cteX)
 *   |-- SomeOp
 *   `--Filter (Pred1)
 *      -- Projection (C1,C2)
 *         -- CTEConsumer(cteX)
 *   |-- ...
 *   `--Filter (Pred2)
 *      -- Projection (C3,C4)
 *         -- CTEConsumer(cteX)
 *
 * After Transformation:
 *   CTEProducer(cteX)
 *   |-- Filter (Pred1 or Pred2)
 *       -- Projection (C1,C2,C3,C4)
 *          -- SomeOp
 *   `--Filter (Pred1)
 *      -- Projection (C1,C2)
 *         -- CTEConsumer(cteX)
 *   |-- ...
 *   `--Filter (Pred2)
 *      -- Projection (C3,C4)
 *         -- CTEConsumer(cteX)*/
public class CteProjectionAndPredicatePushDown
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final ExpressionOptimizerManager expressionOptimizerManager;

    public CteProjectionAndPredicatePushDown(Metadata metadata, ExpressionOptimizerManager expressionOptimizerManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.expressionOptimizerManager = requireNonNull(expressionOptimizerManager, "expressionOptimizerManager is null");
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(warningCollector, "warningCollector is null");
        if (!isCteMaterializationApplicable(session)
                || !getCteFilterAndProjectionPushdownEnabled(session)) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }
        CteContext cteContext = new CteContext();
        plan.accept(new CtePredicateAndProjectionExtractor(session, idAllocator, variableAllocator), cteContext);

        CteProducerRewriter cteProducerRewriter = new CteProducerRewriter(session, idAllocator, variableAllocator);
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(cteProducerRewriter, plan, cteContext);
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, cteProducerRewriter.isPlanRewritten());
    }

    public class CtePredicateAndProjectionExtractor
            extends SimplePlanVisitor<CteContext>
    {
        private final PlanNodeIdAllocator idAllocator;

        private final VariableAllocator variableAllocator;

        private final Session session;

        public CtePredicateAndProjectionExtractor(Session session, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator must not be null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator must not be null");
            this.session = requireNonNull(session, "session must not be null");
        }

        @Override
        public Void visitCteProducer(CteProducerNode node, CteContext context)
        {
            String cteName = node.getCteId();
            List<VariableReferenceExpression> columns = node.getOutputVariables();
            context.addCteProducerInfo(cteName, columns);

            return super.visitCteProducer(node, context);
        }

        public Void visitFilter(FilterNode node, CteContext context)
        {
            PlanNode childNode = node.getSource();

            if (!(childNode instanceof CteConsumerNode)) {
                return super.visitFilter(node, context);
            }

            String cteName = ((CteConsumerNode) childNode).getCteId();
            List<VariableReferenceExpression> producerColumns = context.getCteProducerColumns(cteName);
            RowExpression predicate = node.getPredicate();
            Map<VariableReferenceExpression, VariableReferenceExpression> varMap = constructConsumerToProducerVarMap((CteConsumerNode) childNode, context);
            RowExpression newPredicate = remapExpression(predicate, varMap);
            context.addCteConsumerInfo(cteName, producerColumns, ImmutableList.of(newPredicate));
            return null;
        }

        public Void visitProject(ProjectNode node, CteContext context)
        {
            if (!isCteConsumerFilterRestrict(node)) {
                return super.visitProject(node, context);
            }

            // get predicate and used columns
            CteConsumerNode cteConsumerNode = extractCteConsumer(node);
            Map<VariableReferenceExpression, VariableReferenceExpression> varMap = constructConsumerToProducerVarMap(cteConsumerNode, context);
            List<VariableReferenceExpression> usedColumns =
                    node.getAssignments().getExpressions().stream().map(expression -> (VariableReferenceExpression) remapExpression(expression, varMap)).collect(Collectors.toList());

            FilterNode filterNode = extractFilterNode(node);
            RowExpression predicate;
            if (filterNode != null) {
                predicate = remapExpression(filterNode.getPredicate(), varMap);
                // extract predicate columns and add to used columns
                usedColumns.addAll(VariablesExtractor.extractAll(predicate));
            }
            else {
                predicate = constant(true, BOOLEAN);
            }

            context.addCteConsumerInfo(cteConsumerNode.getCteId(), usedColumns, ImmutableList.of(predicate));
            return null;
        }

        @Override
        public Void visitCteConsumer(CteConsumerNode node, CteContext context)
        {
            // if we reach this point, it means that the CTE consumer had no filter or projection on top of it and we must take all columns and rows
            String cteName = node.getCteId();
            // TODO: support pushing of projections in the cte consumer (similar to table scan projection push down)
            // for now, take the original columns of the CTE producer
            List<VariableReferenceExpression> producerColumns = context.getCteProducerColumns(cteName);
            checkState(producerColumns != null, "No producer with name " + cteName + " found");
            // no filter encountered on top of this consumer: all rows must be read
            RowExpression predicate = constant(true, BOOLEAN);
            context.addCteConsumerInfo(cteName, producerColumns, ImmutableList.of(predicate));
            return null;
        }

        @Override
        public Void visitSequence(SequenceNode node, CteContext context)
        {
            List<PlanNode> cteProducers = node.getCteProducers();
            for (int i = cteProducers.size() - 1; i >= 0; i--) {
                PlanNode cteProducer = cteProducers.get(i);
                cteProducer.accept(this, context);
            }

            PlanNode primarySource = node.getPrimarySource();
            primarySource.accept(this, context);
            return null;
        }

        private boolean isCteConsumerFilterRestrict(PlanNode node)
        {
            if (!(node instanceof ProjectNode)) {
                return false;
            }
            ProjectNode projectNode = (ProjectNode) node;
            PlanNode childNode = projectNode.getSource();

            if (isCteConsumerFilter(childNode) || childNode instanceof CteConsumerNode) {
                // check if all project elements are restrict-only
                return projectNode.getAssignments().getExpressions().stream().allMatch(expression -> expression instanceof VariableReferenceExpression);
            }

            return false;
        }

        private boolean isCteConsumerFilter(PlanNode node)
        {
            return node instanceof FilterNode && ((FilterNode) node).getSource() instanceof CteConsumerNode;
        }

        private CteConsumerNode extractCteConsumer(PlanNode node)
        {
            checkState(isCteConsumerFilterRestrict(node));
            PlanNode childNode = ((ProjectNode) node).getSource();
            if (childNode instanceof CteConsumerNode) {
                return (CteConsumerNode) childNode;
            }

            return (CteConsumerNode) ((FilterNode) childNode).getSource();
        }

        private FilterNode extractFilterNode(PlanNode node)
        {
            checkState(isCteConsumerFilterRestrict(node));
            PlanNode childNode = ((ProjectNode) node).getSource();
            if (childNode instanceof FilterNode) {
                return (FilterNode) childNode;
            }

            // no filter node
            return null;
        }

        private Map<VariableReferenceExpression, VariableReferenceExpression> constructConsumerToProducerVarMap(CteConsumerNode cteConsumerNode, CteContext context)
        {
            List<VariableReferenceExpression> consumerColumns = cteConsumerNode.getOutputVariables();
            List<VariableReferenceExpression> producerColumns = context.getCteProducerColumns(cteConsumerNode.getCteId());
            Map<VariableReferenceExpression, VariableReferenceExpression> varMap = constructVarMap(consumerColumns, producerColumns);
            return varMap;
        }
    }

    private static Map<VariableReferenceExpression, VariableReferenceExpression> constructVarMap(List<VariableReferenceExpression> sourceColumns, List<VariableReferenceExpression> targetColumns)
    {
        Map<VariableReferenceExpression, VariableReferenceExpression> varMap = new HashMap<>();
        Streams.zip(
                sourceColumns.stream(),
                targetColumns.stream(),
                AbstractMap.SimpleImmutableEntry::new).forEach(pair -> varMap.put(pair.getKey(), pair.getValue()));
        return varMap;
    }

    private RowExpression remapExpression(RowExpression expression, Map<VariableReferenceExpression, VariableReferenceExpression> varMap)
    {
        return RowExpressionVariableInliner.inlineVariables(varMap, expression);
    }

    public class CteProducerRewriter
            extends SimplePlanRewriter<CteContext>
    {
        private final PlanNodeIdAllocator idAllocator;

        private final VariableAllocator variableAllocator;

        private final Session session;

        private boolean isPlanRewritten;

        public CteProducerRewriter(Session session, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator must not be null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator must not be null");
            this.session = requireNonNull(session, "session must not be null");
        }

        @Override
        public PlanNode visitCteProducer(CteProducerNode node, RewriteContext<CteContext> context)
        {
            String cteName = node.getCteId();
            List<VariableReferenceExpression> usedColumns = context.get().getCteRequiredColumns(cteName);
            List<RowExpression> predicates = context.get().getPredicates(cteName);

            // recursively process child node
            PlanNode newChild = node.getSource().accept(this, context);

            if (usedColumns == null || predicates == null) {
                PlanNode result = replaceChildren(node, ImmutableList.of(newChild));
                isPlanRewritten = isPlanRewritten || !node.equals(result);
                return result;
            }

            Set<VariableReferenceExpression> usedColumnsSet = new HashSet<VariableReferenceExpression>(usedColumns);
            PlanNode newChildWithFilterAndProject = addFilter(newChild, predicates);

            List<VariableReferenceExpression> producerColumns = node.getOutputVariables();
            List<VariableReferenceExpression> newProducerColumns = producerColumns.stream().filter(var -> usedColumnsSet.contains(var)).collect(Collectors.toList());
            if (!newProducerColumns.equals(newChildWithFilterAndProject.getOutputVariables())) {
                newChildWithFilterAndProject = PlannerUtils.restrictOutput(newChildWithFilterAndProject, idAllocator, newProducerColumns);
            }

            if (newChildWithFilterAndProject != node.getSource()) {
                isPlanRewritten = true;
                return new CteProducerNode(
                        node.getSourceLocation(),
                        node.getId(),
                        node.getStatsEquivalentPlanNode(),
                        newChildWithFilterAndProject,
                        cteName,
                        node.getRowCountVariable(),
                        newProducerColumns);
            }

            return node;
        }

        @Override
        public PlanNode visitCteConsumer(CteConsumerNode node, RewriteContext<CteContext> context)
        {
            // project out consumer columns
            List<VariableReferenceExpression> allProducerColumns = context.get().getCteProducerColumns(node.getCteId());
            List<VariableReferenceExpression> requiredProducerColumns = context.get().getCteRequiredColumns(node.getCteId());
            checkState(requiredProducerColumns != null, "Required columns for producer " + node.getCteId() + " not found");

            Set<VariableReferenceExpression> requiredProducerColumnsSet = new HashSet<>(requiredProducerColumns);
            List<VariableReferenceExpression> newConsumerColumns = new ArrayList<>();
            Streams.zip(
                    allProducerColumns.stream(),
                    node.getOutputVariables().stream(),
                    AbstractMap.SimpleImmutableEntry::new).forEach(pair -> {
                        if (requiredProducerColumnsSet.contains(pair.getKey())) {
                            newConsumerColumns.add(pair.getValue());
                        }
                    });
            return new CteConsumerNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), newConsumerColumns, node.getCteId(), node.getOriginalSource());
        }

        public boolean isPlanRewritten()
        {
            return isPlanRewritten;
        }

        private PlanNode addFilter(PlanNode node, List<RowExpression> predicates)
        {
            if (isConstTrue(predicates)) {
                return node;
            }

            RowExpression resultPredicate;
            if (predicates.size() == 1) {
                resultPredicate = predicates.get(0);
            }
            else {
                resultPredicate = predicates.get(0);
                for (int i = 1; i < predicates.size(); i++) {
                    resultPredicate = new SpecialFormExpression(
                            SpecialFormExpression.Form.OR,
                            BOOLEAN,
                            resultPredicate, predicates.get(i));
                }
            }
            resultPredicate = SimplifyRowExpressions.rewrite(resultPredicate, metadata, session, expressionOptimizerManager);
            return new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), node, resultPredicate);
        }

        private boolean isConstTrue(List<RowExpression> predicates)
        {
            return predicates.size() == 0 || predicates.stream().anyMatch(predicate -> isConstant(predicate, BOOLEAN, true));
        }
    }

    public static class CteContext
    {
        private Map<String, CteInfo> cteNameToTableInfo;
        private Map<String, List<VariableReferenceExpression>> cteProducerOutputColumnsMap;

        public CteContext()
        {
            cteNameToTableInfo = new HashMap<>();
            cteProducerOutputColumnsMap = new HashMap<>();
        }

        public void addCteProducerInfo(String cteName, List<VariableReferenceExpression> outputColumns)
        {
            requireNonNull(outputColumns, "CTE producer output columns cannot be null");
            checkState(!cteProducerOutputColumnsMap.containsKey(cteName), "CTE producer columns already recorded.");
            cteProducerOutputColumnsMap.put(cteName, outputColumns);
        }

        public void addCteConsumerInfo(String cteName, List<VariableReferenceExpression> columns, List<RowExpression> predicates)
        {
            CteInfo cteInfo = cteNameToTableInfo.getOrDefault(cteName, new CteInfo(new HashSet() {}, new ArrayList<>()));
            cteInfo.addColumns(columns);
            cteInfo.addPredicates(predicates);

            cteNameToTableInfo.put(cteName, cteInfo);
        }

        public List<VariableReferenceExpression> getCteProducerColumns(String cteName)
        {
            return cteProducerOutputColumnsMap.getOrDefault(cteName, null);
        }

        public List<VariableReferenceExpression> getCteRequiredColumns(String cteName)
        {
            if (cteNameToTableInfo.containsKey(cteName)) {
                return new ArrayList<>(cteNameToTableInfo.get(cteName).getColumns());
            }

            // if no CTE is found, this means no CTE consumers found during exploration: let the caller handle this case
            return null;
        }

        public List<RowExpression> getPredicates(String cteName)
        {
            if (cteNameToTableInfo.containsKey(cteName)) {
                return cteNameToTableInfo.get(cteName).getPredicates();
            }

            // if no CTE is found, this means no CTE consumers found during exploration: let the caller handle this case
            return null;
        }

        public static class CteInfo
        {
            private Set<VariableReferenceExpression> columns;
            private List<RowExpression> predicates;

            public CteInfo(Set<VariableReferenceExpression> columns, List<RowExpression> predicates)
            {
                this.columns = requireNonNull(columns, "columns must not be null");
                this.predicates = requireNonNull(predicates, "predicates must not be null");
            }

            public Set<VariableReferenceExpression> getColumns()
            {
                return columns;
            }

            public List<RowExpression> getPredicates()
            {
                return predicates;
            }

            public void addColumns(List<VariableReferenceExpression> columns)
            {
                this.columns.addAll(columns);
            }

            public void addPredicates(List<RowExpression> predicates)
            {
                this.predicates.addAll(predicates);
            }
        }
    }
}
