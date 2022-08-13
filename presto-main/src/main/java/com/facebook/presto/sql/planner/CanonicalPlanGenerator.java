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
package com.facebook.presto.sql.planner;

import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.AggregationNode.GroupingSetDescriptor;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.DEFAULT;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.REMOVE_SAFE_CONSTANTS;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.expressions.CanonicalRowExpressionRewriter.canonicalizeRowExpression;
import static com.facebook.presto.sql.planner.CanonicalPartitioningScheme.getCanonicalPartitioningScheme;
import static com.facebook.presto.sql.planner.CanonicalTableScanNode.CanonicalTableHandle.getCanonicalTableHandle;
import static com.facebook.presto.sql.planner.RowExpressionVariableInliner.inlineVariables;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public class CanonicalPlanGenerator
        extends InternalPlanVisitor<Optional<PlanNode>, CanonicalPlanGenerator.Context>
{
    private final PlanNodeIdAllocator planNodeidAllocator = new PlanNodeIdAllocator();
    private final PlanVariableAllocator variableAllocator = new PlanVariableAllocator();
    private final PlanCanonicalizationStrategy strategy;
    private final ObjectMapper objectMapper;

    public CanonicalPlanGenerator(PlanCanonicalizationStrategy strategy, ObjectMapper objectMapper)
    {
        this.strategy = requireNonNull(strategy, "strategy is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    public static Optional<CanonicalPlanFragment> generateCanonicalPlanFragment(PlanNode root, PartitioningScheme partitioningScheme, ObjectMapper objectMapper)
    {
        Context context = new Context();
        Optional<PlanNode> canonicalPlan = root.accept(new CanonicalPlanGenerator(PlanCanonicalizationStrategy.DEFAULT, objectMapper), context);
        if (!context.getExpressions().keySet().containsAll(partitioningScheme.getOutputLayout())) {
            return Optional.empty();
        }
        return canonicalPlan.map(planNode -> new CanonicalPlanFragment(new CanonicalPlan(planNode, DEFAULT), getCanonicalPartitioningScheme(partitioningScheme, context.getExpressions())));
    }

    public static Optional<CanonicalPlan> generateCanonicalPlan(PlanNode root, PlanCanonicalizationStrategy strategy, ObjectMapper objectMapper)
    {
        Optional<PlanNode> canonicalPlanNode = root.accept(new CanonicalPlanGenerator(strategy, objectMapper), new CanonicalPlanGenerator.Context());
        return canonicalPlanNode.map(planNode -> new CanonicalPlan(planNode, strategy));
    }

    @Override
    public Optional<PlanNode> visitPlan(PlanNode node, Context context)
    {
        return Optional.empty();
    }

    @Override
    public Optional<PlanNode> visitUnion(UnionNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        // We want to order sources in a consistent manner. Variable names and plan node ids can mess with that because of
        // our stateful canonicalization using `variableAllocator` and `planNodeIdAllocator`.
        // So, we first try to canonicalize each source independently. They may have conflicting variable names, but we only use it
        // to decide order, and then canonicalize them properly again.
        // This can lead to O(n * h) time complexity, where n is number of plan nodes, and h is height of plan tree. This
        // is at par with other pieces like hashing each plan node.
        Multimap<String, Integer> sourceToPosition = TreeMultimap.create();
        for (int i = 0; i < node.getSources().size(); ++i) {
            PlanNode source = node.getSources().get(i);
            Optional<CanonicalPlan> canonicalSource = generateCanonicalPlan(source, strategy, objectMapper);
            if (!canonicalSource.isPresent()) {
                return Optional.empty();
            }
            sourceToPosition.put(canonicalSource.get().toString(objectMapper), i);
        }

        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        ImmutableList.Builder<VariableReferenceExpression> outputVariables = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, List<VariableReferenceExpression>> outputsToInputs = ImmutableMap.builder();

        sourceToPosition.forEach((ignored, index) -> {
            PlanNode canonicalSource = node.getSources().get(index).accept(this, context).get();
            sources.add(canonicalSource);
        });

        node.getVariableMapping().forEach((outputVariable, sourceVariables) -> {
            ImmutableList.Builder<VariableReferenceExpression> newSourceVariablesBuilder = ImmutableList.builder();
            sourceToPosition.forEach((ignored, index) -> {
                newSourceVariablesBuilder.add(inlineAndCanonicalize(context.getExpressions(), sourceVariables.get(index)));
            });
            ImmutableList<VariableReferenceExpression> newSourceVariables = newSourceVariablesBuilder.build();
            VariableReferenceExpression newVariable = variableAllocator.newVariable(newSourceVariables.get(0));
            outputVariables.add(newVariable);
            context.mapExpression(outputVariable, newVariable);
            outputsToInputs.put(newVariable, newSourceVariables);
        });

        PlanNode result = new UnionNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                sources.build(),
                outputVariables.build().stream().sorted().collect(toImmutableList()),
                ImmutableSortedMap.copyOf(outputsToInputs.build()));

        context.addPlan(node, new CanonicalPlan(result, strategy));
        return Optional.of(result);
    }

    @Override
    public Optional<PlanNode> visitOutput(OutputNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        List<RowExpressionReference> rowExpressionReferences = node.getOutputVariables().stream()
                .map(variable -> new RowExpressionReference(inlineAndCanonicalize(context.getExpressions(), variable, strategy == REMOVE_SAFE_CONSTANTS), variable))
                .sorted(comparing(rowExpressionReference -> rowExpressionReference.getRowExpression().toString()))
                .collect(toImmutableList());

        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> assignments = ImmutableMap.builder();
        for (RowExpressionReference rowExpressionReference : rowExpressionReferences) {
            VariableReferenceExpression reference = variableAllocator.newVariable(rowExpressionReference.getRowExpression());
            context.mapExpression(rowExpressionReference.getVariableReferenceExpression(), reference);
            assignments.put(reference, rowExpressionReference.getRowExpression());
        }
        // Rewrite OutputNode as ProjectNode
        PlanNode canonicalPlan = new ProjectNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                new Assignments(assignments.build()),
                ProjectNode.Locality.LOCAL);
        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));

        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitAggregation(AggregationNode node, Context context)
    {
        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        // Steps to get canonical aggregations:
        //   1. Transform aggregation into canonical form
        //   2. Sort based on canonical aggregation expression
        //   3. Get new variable reference for aggregation expression
        //   4. Record mapping from original variable reference to the new one
        List<AggregationReference> aggregationReferences = node.getAggregations().entrySet().stream()
                .map(entry -> new AggregationReference(getCanonicalAggregation(entry.getValue(), context.getExpressions()), entry.getKey()))
                .sorted(comparing(aggregationReference -> aggregationReference.getAggregation().getCall().toString()))
                .collect(toImmutableList());
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();
        for (AggregationReference aggregationReference : aggregationReferences) {
            VariableReferenceExpression reference = variableAllocator.newVariable(aggregationReference.getAggregation().getCall());
            context.mapExpression(aggregationReference.getVariableReferenceExpression(), reference);
            aggregations.put(reference, aggregationReference.getAggregation());
        }

        PlanNode canonicalPlan = new AggregationNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                aggregations.build(),
                getCanonicalGroupingSetDescriptor(node.getGroupingSets(), context.getExpressions()),
                node.getPreGroupedVariables().stream()
                        .map(variable -> context.getExpressions().get(variable))
                        .collect(toImmutableList()),
                node.getStep(),
                node.getHashVariable().map(ignored -> variableAllocator.newHashVariable()),
                node.getGroupIdVariable().map(variable -> context.getExpressions().get(variable)));

        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    private Aggregation getCanonicalAggregation(Aggregation aggregation, Map<VariableReferenceExpression, VariableReferenceExpression> context)
    {
        return new Aggregation(
                (CallExpression) inlineAndCanonicalize(context, aggregation.getCall()),
                aggregation.getFilter().map(filter -> inlineAndCanonicalize(context, filter)),
                aggregation.getOrderBy().map(orderBy -> getCanonicalOrderingScheme(orderBy, context)),
                aggregation.isDistinct(),
                aggregation.getMask().map(context::get));
    }

    private static OrderingScheme getCanonicalOrderingScheme(OrderingScheme orderingScheme, Map<VariableReferenceExpression, VariableReferenceExpression> context)
    {
        return new OrderingScheme(
                orderingScheme.getOrderBy().stream()
                        .map(orderBy -> new Ordering(context.get(orderBy.getVariable()), orderBy.getSortOrder()))
                        .collect(toImmutableList()));
    }

    private static GroupingSetDescriptor getCanonicalGroupingSetDescriptor(GroupingSetDescriptor groupingSetDescriptor, Map<VariableReferenceExpression, VariableReferenceExpression> context)
    {
        return new GroupingSetDescriptor(
                groupingSetDescriptor.getGroupingKeys().stream()
                        .map(context::get)
                        .collect(toImmutableList()),
                groupingSetDescriptor.getGroupingSetCount(),
                groupingSetDescriptor.getGlobalGroupingSets());
    }

    private static class AggregationReference
    {
        private final Aggregation aggregation;
        private final VariableReferenceExpression variableReferenceExpression;

        public AggregationReference(Aggregation aggregation, VariableReferenceExpression variableReferenceExpression)
        {
            this.aggregation = requireNonNull(aggregation, "aggregation is null");
            this.variableReferenceExpression = requireNonNull(variableReferenceExpression, "variableReferenceExpression is null");
        }

        public Aggregation getAggregation()
        {
            return aggregation;
        }

        public VariableReferenceExpression getVariableReferenceExpression()
        {
            return variableReferenceExpression;
        }
    }

    @Override
    public Optional<PlanNode> visitGroupId(GroupIdNode node, Context context)
    {
        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> groupingColumns = ImmutableMap.builder();
        for (Entry<VariableReferenceExpression, VariableReferenceExpression> entry : node.getGroupingColumns().entrySet()) {
            VariableReferenceExpression column = context.getExpressions().get(entry.getValue());
            VariableReferenceExpression reference = variableAllocator.newVariable(column, "gid");
            context.mapExpression(entry.getKey(), reference);
            groupingColumns.put(reference, column);
        }

        ImmutableList.Builder<List<VariableReferenceExpression>> groupingSets = ImmutableList.builder();
        for (List<VariableReferenceExpression> groupingSet : node.getGroupingSets()) {
            groupingSets.add(groupingSet.stream()
                    .map(variable -> context.getExpressions().get(variable))
                    .collect(toImmutableList()));
        }

        VariableReferenceExpression groupId = variableAllocator.newVariable("groupid", INTEGER);
        context.mapExpression(node.getGroupIdVariable(), groupId);

        PlanNode canonicalPlan = new GroupIdNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                groupingSets.build(),
                groupingColumns.build(),
                node.getAggregationArguments().stream()
                        .map(variable -> context.getExpressions().get(variable))
                        .collect(toImmutableList()),
                groupId);

        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitUnnest(UnnestNode node, Context context)
    {
        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        // Generate canonical unnestVariables.
        ImmutableMap.Builder<VariableReferenceExpression, List<VariableReferenceExpression>> newUnnestVariables = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> unnestVariable : node.getUnnestVariables().entrySet()) {
            VariableReferenceExpression input = (VariableReferenceExpression) inlineAndCanonicalize(context.getExpressions(), unnestVariable.getKey());
            ImmutableList.Builder<VariableReferenceExpression> newVariables = ImmutableList.builder();
            for (VariableReferenceExpression variable : unnestVariable.getValue()) {
                VariableReferenceExpression newVariable = variableAllocator.newVariable(Optional.empty(), "unnest_field", variable.getType());
                context.mapExpression(variable, newVariable);
                newVariables.add(newVariable);
            }
            newUnnestVariables.put(input, newVariables.build());
        }

        // Generate canonical ordinality variable
        Optional<VariableReferenceExpression> ordinalityVariable = node.getOrdinalityVariable()
                .map(variable -> {
                    VariableReferenceExpression newVariable = variableAllocator.newVariable(Optional.empty(), "unnest_ordinality", variable.getType());
                    context.mapExpression(variable, newVariable);
                    return newVariable;
                });

        PlanNode canonicalPlan = new UnnestNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                node.getReplicateVariables().stream()
                        .map(variable -> (VariableReferenceExpression) inlineAndCanonicalize(context.getExpressions(), variable))
                        .collect(toImmutableList()),
                newUnnestVariables.build(),
                ordinalityVariable);

        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitProject(ProjectNode node, Context context)
    {
        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        List<RowExpressionReference> rowExpressionReferences = node.getAssignments().entrySet().stream()
                .map(entry -> new RowExpressionReference(inlineAndCanonicalize(context.getExpressions(), entry.getValue(), strategy == REMOVE_SAFE_CONSTANTS), entry.getKey()))
                .sorted(comparing(rowExpressionReference -> rowExpressionReference.getRowExpression().toString()))
                .collect(toImmutableList());
        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> assignments = ImmutableMap.builder();
        for (RowExpressionReference rowExpressionReference : rowExpressionReferences) {
            VariableReferenceExpression reference = variableAllocator.newVariable(rowExpressionReference.getRowExpression());
            context.mapExpression(rowExpressionReference.getVariableReferenceExpression(), reference);
            assignments.put(reference, rowExpressionReference.getRowExpression());
        }

        PlanNode canonicalPlan = new ProjectNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                new Assignments(assignments.build()),
                node.getLocality());

        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    private static class RowExpressionReference
    {
        private final RowExpression rowExpression;
        private final VariableReferenceExpression variableReferenceExpression;

        public RowExpressionReference(RowExpression rowExpression, VariableReferenceExpression variableReferenceExpression)
        {
            this.rowExpression = requireNonNull(rowExpression, "rowExpression is null");
            this.variableReferenceExpression = requireNonNull(variableReferenceExpression, "variableReferenceExpression is null");
        }

        public RowExpression getRowExpression()
        {
            return rowExpression;
        }

        public VariableReferenceExpression getVariableReferenceExpression()
        {
            return variableReferenceExpression;
        }
    }

    @Override
    public Optional<PlanNode> visitFilter(FilterNode node, Context context)
    {
        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        PlanNode canonicalPlan = new FilterNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                inlineAndCanonicalize(context.getExpressions(), node.getPredicate()));

        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitTableScan(TableScanNode node, Context context)
    {
        List<ColumnReference> columnReferences = node.getAssignments().entrySet().stream()
                .map(entry -> new ColumnReference(entry.getValue(), entry.getKey()))
                .sorted(comparing(columnReference -> columnReference.getColumnHandle().toString()))
                .collect(toImmutableList());
        ImmutableList.Builder<VariableReferenceExpression> outputVariables = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.builder();
        for (ColumnReference columnReference : columnReferences) {
            VariableReferenceExpression reference = variableAllocator.newVariable(Optional.empty(), columnReference.getColumnHandle().toString(), columnReference.getVariableReferenceExpression().getType());
            context.mapExpression(columnReference.getVariableReferenceExpression(), reference);
            outputVariables.add(reference);
            assignments.put(reference, columnReference.getColumnHandle());
        }

        PlanNode canonicalPlan = new CanonicalTableScanNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                getCanonicalTableHandle(node.getTable(), strategy),
                outputVariables.build(),
                assignments.build());

        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    private static <T extends RowExpression> T inlineAndCanonicalize(
            Map<VariableReferenceExpression, VariableReferenceExpression> context,
            T expression)
    {
        return (T) canonicalizeRowExpression(inlineVariables(context::get, expression), false);
    }

    private static <T extends RowExpression> T inlineAndCanonicalize(
            Map<VariableReferenceExpression, VariableReferenceExpression> context,
            T expression,
            boolean removeConstants)
    {
        return (T) canonicalizeRowExpression(inlineVariables(context::get, expression), removeConstants);
    }

    private static class ColumnReference
    {
        private final ColumnHandle columnHandle;
        private final VariableReferenceExpression variableReferenceExpression;

        public ColumnReference(ColumnHandle columnHandle, VariableReferenceExpression variableReferenceExpression)
        {
            this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
            this.variableReferenceExpression = requireNonNull(variableReferenceExpression, "variableReferenceExpression is null");
        }

        public ColumnHandle getColumnHandle()
        {
            return columnHandle;
        }

        public VariableReferenceExpression getVariableReferenceExpression()
        {
            return variableReferenceExpression;
        }
    }

    public static class Context
    {
        private final Map<VariableReferenceExpression, VariableReferenceExpression> expressions = new HashMap<>();
        private final Map<PlanNode, CanonicalPlan> canonicalPlans = new IdentityHashMap<>();

        public Map<VariableReferenceExpression, VariableReferenceExpression> getExpressions()
        {
            return expressions;
        }

        public Map<PlanNode, CanonicalPlan> getCanonicalPlans()
        {
            return canonicalPlans;
        }

        public void mapExpression(VariableReferenceExpression from, VariableReferenceExpression to)
        {
            expressions.put(from, to);
        }

        public void addPlan(PlanNode plan, CanonicalPlan canonicalPlan)
        {
            canonicalPlans.put(plan, canonicalPlan);
        }
    }
}
