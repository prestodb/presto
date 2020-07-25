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
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.expressions.DynamicFilters.DynamicFilterExtractResult;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.facebook.presto.expressions.DynamicFilters.extractDynamicFilters;
import static com.facebook.presto.expressions.DynamicFilters.getPlaceholder;
import static com.facebook.presto.expressions.DynamicFilters.isDynamicFilter;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.RowExpressionTreeRewriter.rewriteWith;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Dynamic filters are supported only right after TableScan and only if the subtree is on the probe side of some downstream join node
 * Dynamic filters are removed from JoinNode if there is no consumer for it on probe side
 */
public class RemoveUnsupportedDynamicFilters
        implements PlanOptimizer
{
    private final LogicalRowExpressions logicalRowExpressions;

    public RemoveUnsupportedDynamicFilters(FunctionManager functionManager)
    {
        requireNonNull(functionManager, "functionManager is null");
        this.logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(functionManager),
                new FunctionResolution(functionManager),
                functionManager);
    }

    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            PlanVariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        PlanWithConsumedDynamicFilters result = plan.accept(new RemoveUnsupportedDynamicFilters.Rewriter(), ImmutableSet.of());
        return result.getNode();
    }

    private class Rewriter
            extends InternalPlanVisitor<PlanWithConsumedDynamicFilters, Set<String>>
    {
        @Override
        public PlanWithConsumedDynamicFilters visitPlan(PlanNode node, Set<String> allowedDynamicFilterIds)
        {
            List<PlanWithConsumedDynamicFilters> children = node.getSources().stream()
                    .map(source -> source.accept(this, allowedDynamicFilterIds))
                    .collect(toImmutableList());

            PlanNode result = replaceChildren(
                    node,
                    children.stream()
                            .map(PlanWithConsumedDynamicFilters::getNode)
                            .collect(toList()));

            Set<String> consumedDynamicFilterIds = children.stream()
                    .map(PlanWithConsumedDynamicFilters::getConsumedDynamicFilterIds)
                    .flatMap(Set::stream)
                    .collect(toImmutableSet());

            return new PlanWithConsumedDynamicFilters(result, consumedDynamicFilterIds);
        }

        @Override
        public PlanWithConsumedDynamicFilters visitJoin(JoinNode node, Set<String> allowedDynamicFilterIds)
        {
            ImmutableSet<String> allowedDynamicFilterIdsProbeSide = ImmutableSet.<String>builder()
                    .addAll(node.getDynamicFilters().keySet())
                    .addAll(allowedDynamicFilterIds)
                    .build();

            PlanWithConsumedDynamicFilters leftResult = node.getLeft().accept(this, allowedDynamicFilterIdsProbeSide);
            Set<String> consumedProbeSide = leftResult.getConsumedDynamicFilterIds();
            Map<String, VariableReferenceExpression> dynamicFilters = node.getDynamicFilters().entrySet().stream()
                    .filter(entry -> consumedProbeSide.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            PlanWithConsumedDynamicFilters rightResult = node.getRight().accept(this, allowedDynamicFilterIds);
            Set<String> consumed = new HashSet<>(rightResult.getConsumedDynamicFilterIds());
            consumed.addAll(consumedProbeSide);
            consumed.removeAll(dynamicFilters.keySet());

            PlanNode left = leftResult.getNode();
            PlanNode right = rightResult.getNode();
            if (!left.equals(node.getLeft()) || !right.equals(node.getRight()) || !dynamicFilters.equals(node.getDynamicFilters())) {
                return new PlanWithConsumedDynamicFilters(
                        new JoinNode(
                            node.getId(),
                            node.getType(),
                            left,
                            right,
                            node.getCriteria(),
                            node.getOutputVariables(),
                            node.getFilter(),
                            node.getLeftHashVariable(),
                            node.getRightHashVariable(),
                            node.getDistributionType(),
                            dynamicFilters),
                        ImmutableSet.copyOf(consumed));
            }
            return new PlanWithConsumedDynamicFilters(node, ImmutableSet.copyOf(consumed));
        }

        @Override
        public PlanWithConsumedDynamicFilters visitSemiJoin(SemiJoinNode node, Set<String> allowedDynamicFilterIds)
        {
            ImmutableSet<String> allowedDynamicFilterIdsProbeSide = ImmutableSet.<String>builder()
                    .addAll(node.getDynamicFilters().keySet())
                    .addAll(allowedDynamicFilterIds)
                    .build();

            PlanWithConsumedDynamicFilters leftResult = node.getSource().accept(this, allowedDynamicFilterIdsProbeSide);
            Set<String> consumedProbeSide = leftResult.getConsumedDynamicFilterIds();
            Map<String, VariableReferenceExpression> dynamicFilters = node.getDynamicFilters().entrySet().stream()
                    .filter(entry -> consumedProbeSide.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            PlanWithConsumedDynamicFilters rightResult = node.getFilteringSource().accept(this, allowedDynamicFilterIds);
            Set<String> consumed = new HashSet<>(rightResult.getConsumedDynamicFilterIds());
            consumed.addAll(consumedProbeSide);
            consumed.removeAll(dynamicFilters.keySet());

            PlanNode left = leftResult.getNode();
            PlanNode right = rightResult.getNode();
            if (!left.equals(node.getSource()) || !right.equals(node.getFilteringSource()) || !dynamicFilters.equals(node.getDynamicFilters())) {
                return new PlanWithConsumedDynamicFilters(
                        new SemiJoinNode(
                                node.getId(),
                                left,
                                right,
                                node.getSourceJoinVariable(),
                                node.getFilteringSourceJoinVariable(),
                                node.getSemiJoinOutput(),
                                node.getSourceHashVariable(),
                                node.getFilteringSourceHashVariable(),
                                node.getDistributionType(),
                                dynamicFilters),
                        ImmutableSet.copyOf(consumed));
            }
            return new PlanWithConsumedDynamicFilters(node, ImmutableSet.copyOf(consumed));
        }

        @Override
        public PlanWithConsumedDynamicFilters visitFilter(FilterNode node, Set<String> allowedDynamicFilterIds)
        {
            PlanWithConsumedDynamicFilters result = node.getSource().accept(this, allowedDynamicFilterIds);

            RowExpression original = node.getPredicate();
            ImmutableSet.Builder<String> consumedDynamicFilterIds = ImmutableSet.<String>builder()
                    .addAll(result.getConsumedDynamicFilterIds());

            PlanNode source = result.getNode();
            RowExpression modified;
            if (source instanceof TableScanNode) {
                // Keep only allowed dynamic filters
                modified = removeDynamicFilters(original, allowedDynamicFilterIds, consumedDynamicFilterIds);
            }
            else {
                modified = removeAllDynamicFilters(original);
            }

            if (TRUE_CONSTANT.equals(modified)) {
                return new PlanWithConsumedDynamicFilters(source, consumedDynamicFilterIds.build());
            }

            if (!original.equals(modified) || source != node.getSource()) {
                return new PlanWithConsumedDynamicFilters(new FilterNode(node.getId(), source, modified),
                        consumedDynamicFilterIds.build());
            }

            return new PlanWithConsumedDynamicFilters(node, consumedDynamicFilterIds.build());
        }

        @Override
        public PlanWithConsumedDynamicFilters visitTableScan(TableScanNode node, Set<String> allowedDynamicFilterIds)
        {
            ImmutableSet.Builder<String> consumedDynamicFilterIds = ImmutableSet.builder();

            RowExpression remainingPredicate = node.getTable().getLayout().map(ConnectorTableLayoutHandle::getRemainingPredicate).orElse(TRUE_CONSTANT);

            removeDynamicFilters(remainingPredicate, allowedDynamicFilterIds, consumedDynamicFilterIds);

            return new PlanWithConsumedDynamicFilters(node, consumedDynamicFilterIds.build());
        }

        private RowExpression removeDynamicFilters(RowExpression expression, Set<String> allowedDynamicFilterIds, ImmutableSet.Builder<String> consumedDynamicFilterIds)
        {
            return logicalRowExpressions.combineConjuncts(extractConjuncts(expression).stream()
                    .map(this::removeNestedDynamicFilters)
                    .filter(conjunct ->
                            getPlaceholder(conjunct)
                                    .map(placeholder -> {
                                        if (allowedDynamicFilterIds.contains(placeholder.getId())) {
                                            consumedDynamicFilterIds.add(placeholder.getId());
                                            return true;
                                        }
                                        return false;
                                    }).orElse(true))
                    .collect(toImmutableList()));
        }

        private RowExpression removeAllDynamicFilters(RowExpression expression)
        {
            RowExpression rewrittenExpression = removeNestedDynamicFilters(expression);
            DynamicFilterExtractResult extractResult = extractDynamicFilters(rewrittenExpression);
            if (extractResult.getDynamicConjuncts().isEmpty()) {
                return expression;
            }
            return logicalRowExpressions.combineConjuncts(extractResult.getStaticConjuncts());
        }

        private RowExpression removeNestedDynamicFilters(RowExpression expression)
        {
            return rewriteWith(new RowExpressionRewriter<AtomicBoolean>()
            {
                @Override
                public RowExpression rewriteRowExpression(RowExpression node, AtomicBoolean context, RowExpressionTreeRewriter<AtomicBoolean> treeRewriter)
                {
                    return node;
                }

                @Override
                public RowExpression rewriteSpecialForm(SpecialFormExpression node, AtomicBoolean modified, RowExpressionTreeRewriter<AtomicBoolean> treeRewriter)
                {
                    if (!isConjunctiveDisjunctive(node.getForm())) {
                        return node;
                    }

                    checkState(BooleanType.BOOLEAN.equals(node.getType()), "AND/OR must be boolean function");

                    ImmutableList.Builder<RowExpression> expressionBuilder = ImmutableList.builder();
                    for (RowExpression argument : node.getArguments()) {
                        expressionBuilder.add(rewriteWith(this, argument, modified));
                    }
                    List<RowExpression> arguments = expressionBuilder.build();

                    expressionBuilder = ImmutableList.builder();
                    if (isDynamicFilter(arguments.get(0))) {
                        expressionBuilder.add(TRUE_CONSTANT);
                        modified.set(true);
                    }
                    else {
                        expressionBuilder.add(arguments.get(0));
                    }

                    if (isDynamicFilter(arguments.get(1))) {
                        expressionBuilder.add(TRUE_CONSTANT);
                        modified.set(true);
                    }
                    else {
                        expressionBuilder.add(arguments.get(1));
                    }

                    if (!modified.get()) {
                        return node;
                    }

                    arguments = expressionBuilder.build();
                    if (node.getForm().equals(AND)) {
                        if (arguments.get(0).equals(TRUE_CONSTANT) && arguments.get(1).equals(TRUE_CONSTANT)) {
                            return TRUE_CONSTANT;
                        }

                        if (arguments.get(0).equals(TRUE_CONSTANT)) {
                            return arguments.get(1);
                        }

                        if (arguments.get(1).equals(TRUE_CONSTANT)) {
                            return arguments.get(0);
                        }
                    }

                    if (node.getForm().equals(OR) && (arguments.get(0).equals(TRUE_CONSTANT) || arguments.get(1).equals(TRUE_CONSTANT))) {
                        return TRUE_CONSTANT;
                    }

                    return new SpecialFormExpression(node.getForm(), node.getType(), arguments);
                }

                private boolean isConjunctiveDisjunctive(SpecialFormExpression.Form form)
                {
                    return form == AND || form == OR;
                }
            }, expression, new AtomicBoolean(false));
        }
    }

    private static class PlanWithConsumedDynamicFilters
    {
        private final PlanNode node;
        private final Set<String> consumedDynamicFilterIds;

        PlanWithConsumedDynamicFilters(PlanNode node, Set<String> consumedDynamicFilterIds)
        {
            this.node = node;
            this.consumedDynamicFilterIds = ImmutableSet.copyOf(consumedDynamicFilterIds);
        }

        PlanNode getNode()
        {
            return node;
        }

        Set<String> getConsumedDynamicFilterIds()
        {
            return consumedDynamicFilterIds;
        }
    }
}
