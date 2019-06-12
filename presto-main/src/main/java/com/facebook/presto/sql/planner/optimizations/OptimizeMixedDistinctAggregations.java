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
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isOptimizeDistinctAggregationEnabled;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.PlannerUtils.toVariableReference;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAsSymbolReference;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/*
 * This optimizer convert query of form:
 *
 *  SELECT a1, a2,..., an, F1(b1), F2(b2), F3(b3), ...., Fm(bm), F(distinct c) FROM Table GROUP BY a1, a2, ..., an
 *
 *  INTO
 *
 *  SELECT a1, a2,..., an, arbitrary(if(group = 0, f1)),...., arbitrary(if(group = 0, fm)), F(if(group = 1, c)) FROM
 *      SELECT a1, a2,..., an, F1(b1) as f1, F2(b2) as f2,...., Fm(bm) as fm, c, group FROM
 *        SELECT a1, a2,..., an, b1, b2, ... ,bn, c FROM Table GROUP BY GROUPING SETS ((a1, a2,..., an, b1, b2, ... ,bn), (a1, a2,..., an, c))
 *      GROUP BY a1, a2,..., an, c, group
 *  GROUP BY a1, a2,..., an
 */
public class OptimizeMixedDistinctAggregations
        implements PlanOptimizer
{
    private final Metadata metadata;

    public OptimizeMixedDistinctAggregations(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isOptimizeDistinctAggregationEnabled(session)) {
            return SimplePlanRewriter.rewriteWith(new Optimizer(idAllocator, variableAllocator, metadata), plan, Optional.empty());
        }

        return plan;
    }

    private static class Optimizer
            extends SimplePlanRewriter<Optional<AggregateInfo>>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final PlanVariableAllocator variableAllocator;
        private final Metadata metadata;

        private Optimizer(PlanNodeIdAllocator idAllocator, PlanVariableAllocator variableAllocator, Metadata metadata)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Optional<AggregateInfo>> context)
        {
            // optimize if and only if
            // some aggregation functions have a distinct mask symbol
            // and if not all aggregation functions on same distinct mask symbol (this case handled by SingleDistinctOptimizer)
            List<VariableReferenceExpression> masks = node.getAggregations().values().stream()
                    .map(Aggregation::getMask).filter(Optional::isPresent).map(Optional::get).collect(toImmutableList());
            Set<VariableReferenceExpression> uniqueMasks = ImmutableSet.copyOf(masks);
            if (uniqueMasks.size() != 1 || masks.size() == node.getAggregations().size()) {
                return context.defaultRewrite(node, Optional.empty());
            }

            if (node.getAggregations().values().stream().map(Aggregation::getFilter).anyMatch(Optional::isPresent)) {
                // Skip if any aggregation contains a filter
                return context.defaultRewrite(node, Optional.empty());
            }

            if (node.hasOrderings()) {
                // Skip if any aggregation contains a order by
                return context.defaultRewrite(node, Optional.empty());
            }

            AggregateInfo aggregateInfo = new AggregateInfo(
                    node.getGroupingKeys(),
                    Iterables.getOnlyElement(uniqueMasks),
                    node.getAggregations(),
                    variableAllocator.getTypes());

            if (!checkAllEquatableTypes(aggregateInfo)) {
                // This optimization relies on being able to GROUP BY arguments
                // of the original aggregation functions. If they their types are
                // not comparable, we have to skip it.
                return context.defaultRewrite(node, Optional.empty());
            }

            PlanNode source = context.rewrite(node.getSource(), Optional.of(aggregateInfo));

            // make sure there's a markdistinct associated with this aggregation
            if (!aggregateInfo.isFoundMarkDistinct()) {
                return context.defaultRewrite(node, Optional.empty());
            }

            // Change aggregate node to do second aggregation, handles this part of optimized plan mentioned above:
            //          SELECT a1, a2,..., an, arbitrary(if(group = 0, f1)),...., arbitrary(if(group = 0, fm)), F(if(group = 1, c))
            ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();
            // Add coalesce projection node to handle count(), count_if(), approx_distinct() functions return a
            // non-null result without any input
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> coalesceVariablesBuilder = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, Aggregation> entry : node.getAggregations().entrySet()) {
                if (entry.getValue().getMask().isPresent()) {
                    VariableReferenceExpression input = aggregateInfo.getNewDistinctAggregateVariable();
                    aggregations.put(entry.getKey(), new Aggregation(
                            new CallExpression(
                                    entry.getValue().getCall().getDisplayName(),
                                    entry.getValue().getCall().getFunctionHandle(),
                                    entry.getValue().getCall().getType(),
                                    ImmutableList.of(castToRowExpression(asSymbolReference(input)))),
                            Optional.empty(),
                            Optional.empty(),
                            false,
                            Optional.empty()));
                }
                else {
                    // Aggregations on non-distinct are already done by new node, just extract the non-null value
                    VariableReferenceExpression argument = aggregateInfo.getNewNonDistinctAggregateVariables().get(entry.getKey());
                    Aggregation aggregation = new Aggregation(
                            new CallExpression(
                                    "arbitrary",
                                    metadata.getFunctionManager().lookupFunction("arbitrary", fromTypes(ImmutableList.of(argument.getType()))),
                                    entry.getKey().getType(),
                                    ImmutableList.of(castToRowExpression(asSymbolReference(argument)))),
                            Optional.empty(),
                            Optional.empty(),
                            false,
                            Optional.empty());
                    String functionName = metadata.getFunctionManager().getFunctionMetadata(entry.getValue().getFunctionHandle()).getName();
                    if (functionName.equals("count") || functionName.equals("count_if") || functionName.equals("approx_distinct")) {
                        VariableReferenceExpression newVariable = variableAllocator.newVariable("expr", entry.getKey().getType());
                        aggregations.put(newVariable, aggregation);
                        coalesceVariablesBuilder.put(newVariable, entry.getKey());
                    }
                    else {
                        aggregations.put(entry.getKey(), aggregation);
                    }
                }
            }
            Map<VariableReferenceExpression, VariableReferenceExpression> coalesceVariables = coalesceVariablesBuilder.build();

            AggregationNode aggregationNode = new AggregationNode(
                    idAllocator.getNextId(),
                    source,
                    aggregations.build(),
                    node.getGroupingSets(),
                    ImmutableList.of(),
                    node.getStep(),
                    Optional.empty(),
                    node.getGroupIdVariable());

            if (coalesceVariables.isEmpty()) {
                return aggregationNode;
            }

            Assignments.Builder outputVariables = Assignments.builder();
            for (VariableReferenceExpression variable : aggregationNode.getOutputVariables()) {
                if (coalesceVariables.containsKey(variable)) {
                    Expression expression = new CoalesceExpression(new SymbolReference(variable.getName()), new Cast(new LongLiteral("0"), "bigint"));
                    outputVariables.put(coalesceVariables.get(variable), castToRowExpression(expression));
                }
                else {
                    outputVariables.put(identityAsSymbolReference(variable));
                }
            }

            return new ProjectNode(idAllocator.getNextId(), aggregationNode, outputVariables.build());
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Optional<AggregateInfo>> context)
        {
            Optional<AggregateInfo> aggregateInfo = context.get();

            // presence of aggregateInfo => mask also present
            if (!aggregateInfo.isPresent() || !aggregateInfo.get().getMask().equals(node.getMarkerVariable())) {
                return context.defaultRewrite(node, Optional.empty());
            }

            aggregateInfo.get().foundMarkDistinct();

            PlanNode source = context.rewrite(node.getSource(), Optional.empty());

            Set<VariableReferenceExpression> allVariables = new HashSet<>();
            List<VariableReferenceExpression> groupByVariables = aggregateInfo.get().getGroupByVariables(); // a
            List<VariableReferenceExpression> nonDistinctAggregateVariables = aggregateInfo.get().getOriginalNonDistinctAggregateArgs(); //b
            VariableReferenceExpression distinctVariable = Iterables.getOnlyElement(aggregateInfo.get().getOriginalDistinctAggregateArgs()); // c

            // If same symbol present in aggregations on distinct and non-distinct values, e.g. select sum(a), count(distinct a),
            // then we need to create a duplicate stream for this symbol
            VariableReferenceExpression duplicatedDistinctVariable = distinctVariable;

            if (nonDistinctAggregateVariables.contains(distinctVariable)) {
                VariableReferenceExpression newVariable = variableAllocator.newVariable(distinctVariable);
                nonDistinctAggregateVariables.set(nonDistinctAggregateVariables.indexOf(distinctVariable), newVariable);
                duplicatedDistinctVariable = newVariable;
            }

            allVariables.addAll(groupByVariables);
            allVariables.addAll(nonDistinctAggregateVariables);
            allVariables.add(distinctVariable);

            // 1. Add GroupIdNode
            VariableReferenceExpression groupVariable = variableAllocator.newVariable("group", BigintType.BIGINT); // g
            GroupIdNode groupIdNode = createGroupIdNode(
                    groupByVariables,
                    nonDistinctAggregateVariables,
                    distinctVariable,
                    duplicatedDistinctVariable,
                    groupVariable,
                    allVariables,
                    source);

            // 2. Add aggregation node
            Set<VariableReferenceExpression> groupByKeys = new HashSet<>(groupByVariables);
            groupByKeys.add(distinctVariable);
            groupByKeys.add(groupVariable);

            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> aggregationOutputVariablesMapBuilder = ImmutableMap.builder();
            AggregationNode aggregationNode = createNonDistinctAggregation(
                    aggregateInfo.get(),
                    distinctVariable,
                    duplicatedDistinctVariable,
                    groupByKeys,
                    groupIdNode,
                    node,
                    aggregationOutputVariablesMapBuilder);
            // This map has mapping only for aggregation on non-distinct symbols which the new AggregationNode handles
            Map<VariableReferenceExpression, VariableReferenceExpression> aggregationOutputVariablesMap = aggregationOutputVariablesMapBuilder.build();

            // 3. Add new project node that adds if expressions
            ProjectNode projectNode = createProjectNode(
                    aggregationNode,
                    aggregateInfo.get(),
                    distinctVariable,
                    groupVariable,
                    groupByVariables,
                    aggregationOutputVariablesMap);

            return projectNode;
        }

        // Returns false if either mask symbol or any of the symbols in aggregations is not comparable
        private boolean checkAllEquatableTypes(AggregateInfo aggregateInfo)
        {
            for (VariableReferenceExpression variable : aggregateInfo.getOriginalNonDistinctAggregateArgs()) {
                if (!variable.getType().isComparable()) {
                    return false;
                }
            }

            if (!aggregateInfo.getMask().getType().isComparable()) {
                return false;
            }

            return true;
        }

        /*
         * This Project is useful for cases when we aggregate on distinct and non-distinct values of same symbol, eg:
         *  select a, sum(b), count(c), sum(distinct c) group by a
         * Without this Project, we would count additional values for count(c)
         *
         * This method also populates maps of old to new symbols. For each key of outputNonDistinctAggregateSymbols,
         * Higher level aggregation node's aggregation <key, AggregateExpression> will now have to run AggregateExpression on value of outputNonDistinctAggregateSymbols
         * Same for outputDistinctAggregateSymbols map
         */
        private ProjectNode createProjectNode(
                AggregationNode source,
                AggregateInfo aggregateInfo,
                VariableReferenceExpression distinctVariable,
                VariableReferenceExpression groupVariable,
                List<VariableReferenceExpression> groupByVariables,
                Map<VariableReferenceExpression, VariableReferenceExpression> aggregationOutputVariablesMap)
        {
            Assignments.Builder outputVariables = Assignments.builder();
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> outputNonDistinctAggregateVariables = ImmutableMap.builder();
            for (VariableReferenceExpression variable : source.getOutputVariables()) {
                if (distinctVariable.equals(variable)) {
                    VariableReferenceExpression newVariable = variableAllocator.newVariable("expr", variable.getType());
                    aggregateInfo.setNewDistinctAggregateSymbol(newVariable);

                    Expression expression = createIfExpression(
                            new SymbolReference(groupVariable.getName()),
                            new Cast(new LongLiteral("1"), "bigint"), // TODO: this should use GROUPING() when that's available instead of relying on specific group numbering
                            ComparisonExpression.Operator.EQUAL,
                            new SymbolReference(variable.getName()),
                            variable.getType());
                    outputVariables.put(newVariable, castToRowExpression(expression));
                }
                else if (aggregationOutputVariablesMap.containsKey(variable)) {
                    VariableReferenceExpression newVariable = variableAllocator.newVariable("expr", variable.getType());
                    // key of outputNonDistinctAggregateSymbols is key of an aggregation in AggrNode above, it will now aggregate on this Map's value
                    outputNonDistinctAggregateVariables.put(aggregationOutputVariablesMap.get(variable), newVariable);
                    Expression expression = createIfExpression(
                            new SymbolReference(groupVariable.getName()),
                            new Cast(new LongLiteral("0"), "bigint"), // TODO: this should use GROUPING() when that's available instead of relying on specific group numbering
                            ComparisonExpression.Operator.EQUAL,
                            new SymbolReference(variable.getName()),
                            variable.getType());
                    outputVariables.put(newVariable, castToRowExpression(expression));
                }

                // A symbol can appear both in groupBy and distinct/non-distinct aggregation
                if (groupByVariables.contains(variable)) {
                    Expression expression = new SymbolReference(variable.getName());
                    outputVariables.put(variable, castToRowExpression(expression));
                }
            }

            // add null assignment for mask
            // unused mask will be removed by PruneUnreferencedOutputs
            outputVariables.put(aggregateInfo.getMask(), castToRowExpression(new NullLiteral()));

            aggregateInfo.setNewNonDistinctAggregateSymbols(outputNonDistinctAggregateVariables.build());

            return new ProjectNode(idAllocator.getNextId(), source, outputVariables.build());
        }

        private GroupIdNode createGroupIdNode(
                List<VariableReferenceExpression> groupByVariables,
                List<VariableReferenceExpression> nonDistinctAggregateVariables,
                VariableReferenceExpression distinctVariable,
                VariableReferenceExpression duplicatedDistinctVariable,
                VariableReferenceExpression groupVariable,
                Set<VariableReferenceExpression> allVariables,
                PlanNode source)
        {
            List<List<VariableReferenceExpression>> groups = new ArrayList<>();
            // g0 = {group-by symbols + allNonDistinctAggregateSymbols}
            // g1 = {group-by symbols + Distinct Symbol}
            // symbols present in Group_i will be set, rest will be Null

            //g0
            Set<VariableReferenceExpression> group0 = new HashSet<>();
            group0.addAll(groupByVariables);
            group0.addAll(nonDistinctAggregateVariables);
            groups.add(ImmutableList.copyOf(group0));

            // g1
            Set<VariableReferenceExpression> group1 = new HashSet<>(groupByVariables);
            group1.add(distinctVariable);
            groups.add(ImmutableList.copyOf(group1));

            return new GroupIdNode(
                    idAllocator.getNextId(),
                    source,
                    groups,
                    allVariables.stream().collect(Collectors.toMap(
                            identity(),
                            variable -> (variable.equals(duplicatedDistinctVariable) ? distinctVariable : variable))),
                    ImmutableList.of(),
                    groupVariable);
        }

        /*
         * This method returns a new Aggregation node which has aggregations on non-distinct symbols from original plan. Generates
         *      SELECT a1, a2,..., an, F1(b1) as f1, F2(b2) as f2,...., Fm(bm) as fm, c, group
         * part in the optimized plan mentioned above
         *
         * It also populates the mappings of new function's output symbol to corresponding old function's output symbol, e.g.
         *     { f1 -> F1, f2 -> F2, ... }
         * The new AggregateNode aggregates on the symbols that original AggregationNode aggregated on
         * Original one will now aggregate on the output symbols of this new node
         */
        private AggregationNode createNonDistinctAggregation(
                AggregateInfo aggregateInfo,
                VariableReferenceExpression distinctVariable,
                VariableReferenceExpression duplicatedDistinctVariable,
                Set<VariableReferenceExpression> groupByKeys,
                GroupIdNode groupIdNode,
                MarkDistinctNode originalNode,
                ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> aggregationOutputSymbolsMapBuilder)
        {
            ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregateInfo.getAggregations().entrySet()) {
                if (!entry.getValue().getMask().isPresent()) {
                    VariableReferenceExpression newVariable = variableAllocator.newVariable(entry.getKey());
                    Aggregation aggregation = entry.getValue();
                    aggregationOutputSymbolsMapBuilder.put(newVariable, entry.getKey());
                    // Handling for cases when mask symbol appears in non distinct aggregations too
                    // Now the aggregation should happen over the duplicate symbol added before
                    List<RowExpression> arguments;
                    if (!duplicatedDistinctVariable.equals(distinctVariable) &&
                            extractVariables(entry.getValue().getArguments(), variableAllocator.getTypes()).contains(distinctVariable)) {
                        ImmutableList.Builder<RowExpression> argumentsBuilder = ImmutableList.builder();
                        for (RowExpression argument : aggregation.getArguments()) {
                            if (castToExpression(argument) instanceof SymbolReference &&
                                    toVariableReference(castToExpression(argument), variableAllocator.getTypes()).equals(distinctVariable)) {
                                argumentsBuilder.add(castToRowExpression(asSymbolReference(duplicatedDistinctVariable)));
                            }
                            else {
                                argumentsBuilder.add(argument);
                            }
                        }
                        arguments = argumentsBuilder.build();
                    }
                    else {
                        arguments = aggregation.getArguments();
                    }

                    aggregations.put(newVariable, new Aggregation(
                            new CallExpression(
                                    aggregation.getCall().getDisplayName(),
                                    aggregation.getCall().getFunctionHandle(),
                                    aggregation.getCall().getType(),
                                    arguments),
                            Optional.empty(),
                            Optional.empty(),
                            false,
                            Optional.empty()));
                }
            }
            return new AggregationNode(
                    idAllocator.getNextId(),
                    groupIdNode,
                    aggregations.build(),
                    singleGroupingSet(ImmutableList.copyOf(groupByKeys)),
                    ImmutableList.of(),
                    SINGLE,
                    originalNode.getHashVariable(),
                    Optional.empty());
        }

        private static Set<VariableReferenceExpression> extractVariables(List<RowExpression> arguments, TypeProvider types)
        {
            ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
            for (RowExpression argument : arguments) {
                Expression expression = castToExpression(argument);
                if (expression instanceof SymbolReference) {
                    builder.add(variable(((SymbolReference) expression).getName(), types.get(expression)));
                }
            }
            return builder.build();
        }

        // creates if clause specific to use case here, default value always null
        private static IfExpression createIfExpression(Expression left, Expression right, ComparisonExpression.Operator operator, Expression result, Type trueValueType)
        {
            return new IfExpression(
                    new ComparisonExpression(operator, left, right),
                    result,
                    new Cast(new NullLiteral(), trueValueType.getTypeSignature().toString()));
        }
    }

    private static class AggregateInfo
    {
        private final List<VariableReferenceExpression> groupByVariables;
        private final VariableReferenceExpression mask;
        private final Map<VariableReferenceExpression, Aggregation> aggregations;
        private final TypeProvider types;

        // Filled on the way back, these are the variables corresponding to their distinct or non-distinct original variables
        private Map<VariableReferenceExpression, VariableReferenceExpression> newNonDistinctAggregateVariables;
        private VariableReferenceExpression newDistinctAggregateVariable;
        private boolean foundMarkDistinct;

        public AggregateInfo(List<VariableReferenceExpression> groupByVariables, VariableReferenceExpression mask, Map<VariableReferenceExpression, Aggregation> aggregations, TypeProvider types)
        {
            this.groupByVariables = ImmutableList.copyOf(groupByVariables);
            this.mask = mask;
            this.aggregations = ImmutableMap.copyOf(aggregations);
            this.types = types;
        }

        public List<VariableReferenceExpression> getOriginalNonDistinctAggregateArgs()
        {
            return aggregations.values().stream()
                    .filter(aggregation -> !aggregation.getMask().isPresent())
                    .flatMap(aggregation -> aggregation.getArguments().stream())
                    .distinct()
                    .map(OriginalExpressionUtils::castToExpression)
                    .map(expression -> toVariableReference(expression, types))
                    .collect(Collectors.toList());
        }

        public List<VariableReferenceExpression> getOriginalDistinctAggregateArgs()
        {
            return aggregations.values().stream()
                    .filter(aggregation -> aggregation.getMask().isPresent())
                    .flatMap(aggregation -> aggregation.getArguments().stream())
                    .distinct()
                    .map(expression -> toVariableReference(castToExpression(expression), types))
                    .collect(Collectors.toList());
        }

        public VariableReferenceExpression getNewDistinctAggregateVariable()
        {
            return newDistinctAggregateVariable;
        }

        public void setNewDistinctAggregateSymbol(VariableReferenceExpression newDistinctAggregateVariable)
        {
            this.newDistinctAggregateVariable = newDistinctAggregateVariable;
        }

        public Map<VariableReferenceExpression, VariableReferenceExpression> getNewNonDistinctAggregateVariables()
        {
            return newNonDistinctAggregateVariables;
        }

        public void setNewNonDistinctAggregateSymbols(Map<VariableReferenceExpression, VariableReferenceExpression> newNonDistinctAggregateVariables)
        {
            this.newNonDistinctAggregateVariables = newNonDistinctAggregateVariables;
        }

        public VariableReferenceExpression getMask()
        {
            return mask;
        }

        public List<VariableReferenceExpression> getGroupByVariables()
        {
            return groupByVariables;
        }

        public Map<VariableReferenceExpression, Aggregation> getAggregations()
        {
            return aggregations;
        }

        public void foundMarkDistinct()
        {
            foundMarkDistinct = true;
        }

        public boolean isFoundMarkDistinct()
        {
            return foundMarkDistinct;
        }
    }
}
