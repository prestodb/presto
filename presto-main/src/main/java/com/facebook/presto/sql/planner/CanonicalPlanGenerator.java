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
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.planner.CanonicalPartitioningScheme.getCanonicalPartitioningScheme;
import static com.facebook.presto.sql.planner.CanonicalTableScanNode.CanonicalTableHandle.getCanonicalTableHandle;
import static com.facebook.presto.sql.planner.RowExpressionVariableInliner.inlineVariables;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public class CanonicalPlanGenerator
        extends InternalPlanVisitor<Optional<PlanNode>, Map<VariableReferenceExpression, VariableReferenceExpression>>
{
    private final PlanNodeIdAllocator planNodeidAllocator = new PlanNodeIdAllocator();
    private final PlanVariableAllocator variableAllocator = new PlanVariableAllocator();

    public static Optional<CanonicalPlanFragment> generateCanonicalPlan(PlanNode root, PartitioningScheme partitioningScheme)
    {
        Map<VariableReferenceExpression, VariableReferenceExpression> originalToNewVariableNames = new HashMap<>();
        Optional<PlanNode> canonicalPlan = root.accept(new CanonicalPlanGenerator(), originalToNewVariableNames);
        if (!originalToNewVariableNames.keySet().containsAll(partitioningScheme.getOutputLayout())) {
            return Optional.empty();
        }
        return canonicalPlan.map(planNode -> new CanonicalPlanFragment(planNode, getCanonicalPartitioningScheme(partitioningScheme, originalToNewVariableNames)));
    }

    @Override
    public Optional<PlanNode> visitPlan(PlanNode node, Map<VariableReferenceExpression, VariableReferenceExpression> context)
    {
        return Optional.empty();
    }

    @Override
    public Optional<PlanNode> visitAggregation(AggregationNode node, Map<VariableReferenceExpression, VariableReferenceExpression> context)
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
                .map(entry -> new AggregationReference(getCanonicalAggregation(entry.getValue(), context), entry.getKey()))
                .sorted(comparing(aggregationReference -> aggregationReference.getAggregation().getCall().toString()))
                .collect(toImmutableList());
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();
        for (AggregationReference aggregationReference : aggregationReferences) {
            VariableReferenceExpression reference = variableAllocator.newVariable(aggregationReference.getAggregation().getCall());
            context.put(aggregationReference.getVariableReferenceExpression(), reference);
            aggregations.put(reference, aggregationReference.getAggregation());
        }

        return Optional.of(new AggregationNode(
                planNodeidAllocator.getNextId(),
                source.get(),
                aggregations.build(),
                getCanonicalGroupingSetDescriptor(node.getGroupingSets(), context),
                node.getPreGroupedVariables().stream()
                        .map(context::get)
                        .collect(toImmutableList()),
                node.getStep(),
                node.getHashVariable().map(ignored -> variableAllocator.newHashVariable()),
                node.getGroupIdVariable().map(context::get)));
    }

    private static Aggregation getCanonicalAggregation(Aggregation aggregation, Map<VariableReferenceExpression, VariableReferenceExpression> context)
    {
        return new Aggregation(
                (CallExpression) inlineVariables(context, aggregation.getCall()),
                aggregation.getFilter().map(filter -> inlineVariables(context, filter)),
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
    public Optional<PlanNode> visitGroupId(GroupIdNode node, Map<VariableReferenceExpression, VariableReferenceExpression> context)
    {
        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> groupingColumns = ImmutableMap.builder();
        for (Entry<VariableReferenceExpression, VariableReferenceExpression> entry : node.getGroupingColumns().entrySet()) {
            VariableReferenceExpression column = context.get(entry.getValue());
            VariableReferenceExpression reference = variableAllocator.newVariable(column, "gid");
            context.put(entry.getKey(), reference);
            groupingColumns.put(reference, column);
        }

        ImmutableList.Builder<List<VariableReferenceExpression>> groupingSets = ImmutableList.builder();
        for (List<VariableReferenceExpression> groupingSet : node.getGroupingSets()) {
            groupingSets.add(groupingSet.stream()
                    .map(context::get)
                    .collect(toImmutableList()));
        }

        VariableReferenceExpression groupId = variableAllocator.newVariable("groupid", INTEGER);
        context.put(node.getGroupIdVariable(), groupId);

        return Optional.of(new GroupIdNode(
                planNodeidAllocator.getNextId(),
                source.get(),
                groupingSets.build(),
                groupingColumns.build(),
                node.getAggregationArguments().stream()
                        .map(context::get)
                        .collect(toImmutableList()),
                groupId));
    }

    @Override
    public Optional<PlanNode> visitUnnest(UnnestNode node, Map<VariableReferenceExpression, VariableReferenceExpression> context)
    {
        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        // Generate canonical unnestVariables.
        ImmutableMap.Builder<VariableReferenceExpression, List<VariableReferenceExpression>> newUnnestVariables = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> unnestVariable : node.getUnnestVariables().entrySet()) {
            VariableReferenceExpression input = (VariableReferenceExpression) inlineVariables(context, unnestVariable.getKey());
            ImmutableList.Builder<VariableReferenceExpression> newVariables = ImmutableList.builder();
            for (VariableReferenceExpression variable : unnestVariable.getValue()) {
                VariableReferenceExpression newVariable = variableAllocator.newVariable("unnest_field", variable.getType());
                context.put(variable, newVariable);
                newVariables.add(newVariable);
            }
            newUnnestVariables.put(input, newVariables.build());
        }

        // Generate canonical ordinality variable
        Optional<VariableReferenceExpression> ordinalityVariable = node.getOrdinalityVariable()
                .map(variable -> {
                    VariableReferenceExpression newVariable = variableAllocator.newVariable("unnest_ordinality", variable.getType());
                    context.put(variable, newVariable);
                    return newVariable;
                });

        return Optional.of(new UnnestNode(
                planNodeidAllocator.getNextId(),
                source.get(),
                node.getReplicateVariables().stream()
                        .map(variable -> (VariableReferenceExpression) inlineVariables(context, variable))
                        .collect(toImmutableList()),
                newUnnestVariables.build(),
                ordinalityVariable));
    }

    @Override
    public Optional<PlanNode> visitProject(ProjectNode node, Map<VariableReferenceExpression, VariableReferenceExpression> context)
    {
        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        List<RowExpressionReference> rowExpressionReferences = node.getAssignments().entrySet().stream()
                .map(entry -> new RowExpressionReference(inlineVariables(context, entry.getValue()), entry.getKey()))
                .sorted(comparing(rowExpressionReference -> rowExpressionReference.getRowExpression().toString()))
                .collect(toImmutableList());
        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> assignments = ImmutableMap.builder();
        for (RowExpressionReference rowExpressionReference : rowExpressionReferences) {
            VariableReferenceExpression reference = variableAllocator.newVariable(rowExpressionReference.getRowExpression());
            context.put(rowExpressionReference.getVariableReferenceExpression(), reference);
            assignments.put(reference, rowExpressionReference.getRowExpression());
        }

        return Optional.of(new ProjectNode(
                planNodeidAllocator.getNextId(),
                source.get(),
                new Assignments(assignments.build()),
                node.getLocality()));
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
    public Optional<PlanNode> visitFilter(FilterNode node, Map<VariableReferenceExpression, VariableReferenceExpression> context)
    {
        Optional<PlanNode> source = node.getSource().accept(this, context);
        return source.map(planNode -> new FilterNode(
                planNodeidAllocator.getNextId(),
                planNode,
                inlineVariables(context, node.getPredicate())));
    }

    @Override
    public Optional<PlanNode> visitTableScan(TableScanNode node, Map<VariableReferenceExpression, VariableReferenceExpression> context)
    {
        List<ColumnReference> columnReferences = node.getAssignments().entrySet().stream()
                .map(entry -> new ColumnReference(entry.getValue(), entry.getKey()))
                .sorted(comparing(columnReference -> columnReference.getColumnHandle().toString()))
                .collect(toImmutableList());
        ImmutableList.Builder<VariableReferenceExpression> outputVariables = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.builder();
        for (ColumnReference columnReference : columnReferences) {
            VariableReferenceExpression reference = variableAllocator.newVariable(columnReference.getColumnHandle().toString(), columnReference.getVariableReferenceExpression().getType());
            context.put(columnReference.getVariableReferenceExpression(), reference);
            outputVariables.add(reference);
            assignments.put(reference, columnReference.getColumnHandle());
        }

        return Optional.of(new CanonicalTableScanNode(
                planNodeidAllocator.getNextId(),
                getCanonicalTableHandle(node.getTable()),
                outputVariables.build(),
                assignments.build()));
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
}
