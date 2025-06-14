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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.OffsetNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.isRewriteExpressionWithConstantEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.sql.planner.PlannerUtils.addOverrideProjection;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Get constant from filter and project node. Rewrite expressions in parent nodes with the constant.
 * For example, for query "select orderkey, orderpriority, avg(totalprice) from orders where orderpriority='3-MEDIUM' group by 1, 2", the query plan changes from
 * <pre>
 *     - OutputNode
 *          orderkey, orderpriority, avg
 *          - Aggregate
 *              avg := avg(totalprice)
 *              Grouping Keys := [orderkey, orderpriority]
 *              - Filter
 *                  orderpriority = '3-MEDIUM'
 *                  - TableScan
 *                      orderkey := orderkey
 *                      orderpriority := orderpriority
 *                      totalprice := totalprice
 * </pre>
 * to
 * <pre>
 *      - OutputNode
 *          orderkey, expr_12, avg
 *          - project
 *              orderkey := orderkey
 *              expr_12 := '3-MEDIUM'
 *              avg := avg
 *              - Aggregate
 *                  avg := avg(totalprice)
 *                  Grouping Keys := [orderkey]
 *                  - Filter
 *                      orderpriority = '3-MEDIUM'
 *                      - TableScan
 *                          orderkey := orderkey
 *                          orderpriority := orderpriority
 *                          totalprice := totalprice
 * </pre>
 */
public class ReplaceConstantVariableReferencesWithConstants
        implements PlanOptimizer
{
    private static final List<Type> SUPPORTED_TYPES = ImmutableList.of(BIGINT, INTEGER, VARCHAR, DATE);
    private final FunctionAndTypeManager functionAndTypeManager;

    public ReplaceConstantVariableReferencesWithConstants(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    private static boolean isSupportedType(RowExpression rowExpression)
    {
        return SUPPORTED_TYPES.contains(rowExpression.getType()) || rowExpression.getType() instanceof VarcharType;
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isRewriteExpressionWithConstantEnabled(session)) {
            Rewriter rewriter = new Rewriter(idAllocator, functionAndTypeManager);
            PlanNode rewrittenPlan = plan.accept(rewriter, null).getPlanNode();
            return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
        }
        return PlanOptimizerResult.optimizerResult(plan, false);
    }

    private static class Rewriter
            extends InternalPlanVisitor<PlanNodeWithConstant, Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final FunctionResolution functionResolution;
        private boolean planChanged;

        public Rewriter(PlanNodeIdAllocator idAllocator, FunctionAndTypeManager functionAndTypeManager)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        private PlanNodeWithConstant accept(PlanNode node)
        {
            return node.accept(this, null);
        }

        private PlanNodeWithConstant planAndReplace(PlanNode node, boolean keepConstantConstraint)
        {
            List<PlanNodeWithConstant> children = node.getSources().stream().map(this::accept).collect(toImmutableList());
            ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();

            for (PlanNodeWithConstant planNodeWithConstant : children) {
                PlanNode child = planNodeWithConstant.getPlanNode();
                Map<VariableReferenceExpression, ConstantExpression> constantExpressionMap = planNodeWithConstant.getConstantExpressionMap();
                if (child.getOutputVariables().stream().noneMatch(x -> constantExpressionMap.containsKey(x))) {
                    newSources.add(child);
                    continue;
                }
                newSources.add(addOverrideProjection(child, idAllocator, constantExpressionMap));
            }
            PlanNode result = replaceChildren(node, newSources.build());
            if (!keepConstantConstraint) {
                return new PlanNodeWithConstant(result, ImmutableMap.of());
            }
            ImmutableMap.Builder<VariableReferenceExpression, ConstantExpression> properties = ImmutableMap.builder();
            children.stream().map(PlanNodeWithConstant::getConstantExpressionMap).forEach(properties::putAll);
            return new PlanNodeWithConstant(result, properties.build());
        }

        @Override
        public PlanNodeWithConstant visitPlan(PlanNode node, Void context)
        {
            return planAndReplace(node, false);
        }

        @Override
        public PlanNodeWithConstant visitFilter(FilterNode node, Void context)
        {
            PlanNodeWithConstant rewrittenChild = accept(node.getSource());

            RowExpression predicate = node.getPredicate();
            Map<VariableReferenceExpression, ConstantExpression> newConstantMap = new HashMap<>();
            newConstantMap.putAll(rewrittenChild.getConstantExpressionMap());
            for (RowExpression conjunct : extractConjuncts(predicate)) {
                if (conjunct instanceof CallExpression && functionResolution.isEqualsFunction(((CallExpression) conjunct).getFunctionHandle())) {
                    RowExpression argument0 = ((CallExpression) conjunct).getArguments().get(0);
                    RowExpression argument1 = ((CallExpression) conjunct).getArguments().get(1);
                    if (isSupportedType(argument0) && isSupportedType(argument1)) {
                        if ((argument0 instanceof VariableReferenceExpression && argument1 instanceof ConstantExpression)
                                || (argument1 instanceof VariableReferenceExpression && argument0 instanceof ConstantExpression)) {
                            VariableReferenceExpression variable = (VariableReferenceExpression) (argument0 instanceof VariableReferenceExpression ? argument0 : argument1);
                            ConstantExpression constant = (ConstantExpression) (argument0 instanceof ConstantExpression ? argument0 : argument1);
                            // Get conflicting filter expression
                            if (newConstantMap.containsKey(variable) && !newConstantMap.get(variable).equals(constant)) {
                                return new PlanNodeWithConstant(replaceChildren(node, ImmutableList.of(rewrittenChild.getPlanNode())), ImmutableMap.of());
                            }
                            if (!constant.isNull()) {
                                planChanged = true;
                                newConstantMap.put(variable, constant);
                            }
                        }
                    }
                }
            }

            FilterNode newFilterNode = node;
            if (!rewrittenChild.getConstantExpressionMap().isEmpty()) {
                predicate = predicate.accept(new ExpressionRewriter(rewrittenChild.getConstantExpressionMap()), null);
                newFilterNode = new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), node.getSource(), predicate);
            }

            return new PlanNodeWithConstant(replaceChildren(newFilterNode, ImmutableList.of(rewrittenChild.getPlanNode())), newConstantMap);
        }

        @Override
        public PlanNodeWithConstant visitProject(ProjectNode node, Void context)
        {
            PlanNodeWithConstant rewrittenChild = accept(node.getSource());
            ProjectNode newProjectNode = node;
            if (!rewrittenChild.getConstantExpressionMap().isEmpty()) {
                Map<VariableReferenceExpression, RowExpression> newAssignments = node.getAssignments().getMap().entrySet().stream()
                        .collect(toImmutableMap(x -> x.getKey(), x -> x.getValue().accept(new ExpressionRewriter(rewrittenChild.getConstantExpressionMap()), null)));
                newProjectNode = new ProjectNode(idAllocator.getNextId(), node.getSource(), Assignments.copyOf(newAssignments));
            }

            ImmutableMap.Builder<VariableReferenceExpression, ConstantExpression> newConstantMap = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : newProjectNode.getAssignments().getMap().entrySet()) {
                if (entry.getValue() instanceof ConstantExpression && isSupportedType(entry.getKey()) && isSupportedType(entry.getValue())) {
                    ConstantExpression constantExpression = (ConstantExpression) entry.getValue();
                    if (!constantExpression.isNull()) {
                        planChanged = true;
                        newConstantMap.put(entry.getKey(), constantExpression);
                    }
                }
            }

            return new PlanNodeWithConstant(replaceChildren(newProjectNode, ImmutableList.of(rewrittenChild.getPlanNode())), newConstantMap.build());
        }

        @Override
        public PlanNodeWithConstant visitJoin(JoinNode node, Void context)
        {
            PlanNodeWithConstant rewrittenLeft = accept(node.getLeft());
            PlanNodeWithConstant rewrittenRight = accept(node.getRight());
            ImmutableMap.Builder<VariableReferenceExpression, ConstantExpression> outputConstantMap = ImmutableMap.builder();

            // Output from inner side of outer joins can be NULL when no match, hence will not keep the constant constraint
            if (node.getType().equals(JoinType.LEFT) || node.getType().equals(JoinType.INNER)) {
                outputConstantMap.putAll(rewrittenLeft.getConstantExpressionMap());
            }

            if (node.getType().equals(JoinType.RIGHT) || node.getType().equals(JoinType.INNER)) {
                outputConstantMap.putAll(rewrittenRight.getConstantExpressionMap());
            }

            // Add a projection with constant assignment for input source nodes if exist
            List<PlanNode> sourceWithConstantProjection = ImmutableList.of(addOverrideProjection(rewrittenLeft.getPlanNode(), idAllocator, rewrittenLeft.getConstantExpressionMap()),
                    addOverrideProjection(rewrittenRight.getPlanNode(), idAllocator, rewrittenRight.getConstantExpressionMap()));

            return new PlanNodeWithConstant(replaceChildren(node, sourceWithConstantProjection), outputConstantMap.build());
        }

        @Override
        public PlanNodeWithConstant visitUnion(UnionNode node, Void context)
        {
            List<PlanNodeWithConstant> rewrittenSources = node.getSources().stream().map(this::accept).collect(toImmutableList());
            ImmutableMap.Builder<VariableReferenceExpression, ConstantExpression> outputConstantMap = ImmutableMap.builder();
            if (rewrittenSources.stream().allMatch(x -> !x.getConstantExpressionMap().isEmpty())) {
                for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> entry : node.getVariableMapping().entrySet()) {
                    VariableReferenceExpression outputVariable = entry.getKey();
                    List<VariableReferenceExpression> inputList = entry.getValue();
                    // Output variable is constant only when all corresponding variables in input are the same constant
                    if (IntStream.range(0, inputList.size()).boxed().allMatch(idx -> rewrittenSources.get(idx).getConstantExpressionMap().containsKey(inputList.get(idx)))
                            && IntStream.range(0, inputList.size()).boxed().map(idx -> rewrittenSources.get(idx).getConstantExpressionMap().get(inputList.get(idx))).distinct().count() == 1) {
                        outputConstantMap.put(outputVariable, rewrittenSources.get(0).getConstantExpressionMap().get(inputList.get(0)));
                    }
                }
            }
            List<PlanNode> sourceWithConstantProjection = rewrittenSources.stream().map(x -> addOverrideProjection(x.getPlanNode(), idAllocator, x.getConstantExpressionMap())).collect(toImmutableList());
            return new PlanNodeWithConstant(replaceChildren(node, sourceWithConstantProjection), outputConstantMap.build());
        }

        @Override
        public PlanNodeWithConstant visitAggregation(AggregationNode node, Void context)
        {
            PlanNodeWithConstant rewrittenChild = accept(node.getSource());
            List<PlanNode> sourceWithConstantProjection = ImmutableList.of(addOverrideProjection(rewrittenChild.getPlanNode(), idAllocator, rewrittenChild.getConstantExpressionMap()));
            if (node.getGroupingSetCount() != 1) {
                return new PlanNodeWithConstant(replaceChildren(node, sourceWithConstantProjection), ImmutableMap.of());
            }
            Map<VariableReferenceExpression, ConstantExpression> constantGroupByKeys = rewrittenChild.getConstantExpressionMap().entrySet().stream()
                    .filter(x -> node.getGroupingKeys().contains(x.getKey())).collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

            return new PlanNodeWithConstant(replaceChildren(node, sourceWithConstantProjection), constantGroupByKeys);
        }

        @Override
        public PlanNodeWithConstant visitTopN(TopNNode node, Void context)
        {
            return planAndReplace(node, true);
        }

        @Override
        public PlanNodeWithConstant visitSort(SortNode node, Void context)
        {
            return planAndReplace(node, true);
        }

        @Override
        public PlanNodeWithConstant visitLimit(LimitNode node, Void context)
        {
            return planAndReplace(node, true);
        }

        @Override
        public PlanNodeWithConstant visitSample(SampleNode node, Void context)
        {
            return planAndReplace(node, true);
        }

        @Override
        public PlanNodeWithConstant visitSemiJoin(SemiJoinNode node, Void context)
        {
            return planAndReplace(node, true);
        }

        @Override
        public PlanNodeWithConstant visitOffset(OffsetNode node, Void context)
        {
            return planAndReplace(node, true);
        }

        @Override
        public PlanNodeWithConstant visitUnnest(UnnestNode node, Void context)
        {
            return planAndReplace(node, true);
        }
    }

    private static class PlanNodeWithConstant
    {
        private final PlanNode planNode;
        private final Map<VariableReferenceExpression, ConstantExpression> constantExpressionMap;

        public PlanNodeWithConstant(PlanNode planNode, Map<VariableReferenceExpression, ConstantExpression> constantExpressionMap)
        {
            checkArgument(constantExpressionMap.entrySet().stream().allMatch(entry -> entry.getKey().getType().equals(entry.getValue().getType())),
                    "key and value in constantExpressionMap not of the same type");
            this.planNode = planNode;
            this.constantExpressionMap = constantExpressionMap.entrySet().stream().filter(entry -> planNode.getOutputVariables().contains(entry.getKey()))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public PlanNode getPlanNode()
        {
            return planNode;
        }

        public Map<VariableReferenceExpression, ConstantExpression> getConstantExpressionMap()
        {
            return constantExpressionMap;
        }
    }

    private static class ExpressionRewriter
            implements RowExpressionVisitor<RowExpression, Void>
    {
        private final Map<VariableReferenceExpression, ConstantExpression> expressionMap;

        public ExpressionRewriter(Map<VariableReferenceExpression, ConstantExpression> expressionMap)
        {
            this.expressionMap = ImmutableMap.copyOf(expressionMap);
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            return new CallExpression(
                    call.getSourceLocation(),
                    call.getDisplayName(),
                    call.getFunctionHandle(),
                    call.getType(),
                    call.getArguments().stream().map(argument -> argument.accept(this, null)).collect(toImmutableList()));
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return lambda;
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            if (expressionMap.containsKey(reference)) {
                return expressionMap.get(reference);
            }
            return reference;
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            return new SpecialFormExpression(
                    specialForm.getForm(),
                    specialForm.getType(),
                    specialForm.getArguments().stream().map(argument -> argument.accept(this, null)).collect(toImmutableList()));
        }
    }
}
