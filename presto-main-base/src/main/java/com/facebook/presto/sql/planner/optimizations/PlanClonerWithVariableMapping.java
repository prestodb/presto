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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Deep-clones a plan tree with fresh variable instances.
 */
public class PlanClonerWithVariableMapping
        extends SimplePlanRewriter<Void>
{
    private final PlanNodeIdAllocator idAllocator;
    private final VariableAllocator variableAllocator;
    private final Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping;
    private final Lookup lookup;

    private PlanClonerWithVariableMapping(PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, Lookup lookup)
    {
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.variableMapping = new HashMap<>();
        this.lookup = lookup;
    }

    public static ClonedPlan clonePlan(PlanNode plan, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return clonePlan(plan, variableAllocator, idAllocator, null);
    }

    public static ClonedPlan clonePlan(PlanNode plan, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        PlanClonerWithVariableMapping cloner = new PlanClonerWithVariableMapping(idAllocator, variableAllocator, lookup);
        PlanNode clonedPlan = SimplePlanRewriter.rewriteWith(cloner, plan);
        return new ClonedPlan(clonedPlan, ImmutableMap.copyOf(cloner.variableMapping));
    }

    private VariableReferenceExpression getClonedVariable(VariableReferenceExpression original)
    {
        return variableMapping.computeIfAbsent(original, var ->
            variableAllocator.newVariable(var.getName(), var.getType()));
    }

    private RowExpression rewriteExpression(RowExpression expression)
    {
        return RowExpressionVariableInliner.inlineVariables(this::getClonedVariable, expression);
    }

    private Assignments rewriteAssignments(Assignments assignments)
    {
        Assignments.Builder builder = Assignments.builder();
        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : assignments.entrySet()) {
            VariableReferenceExpression clonedVar = getClonedVariable(entry.getKey());
            RowExpression clonedExpr = rewriteExpression(entry.getValue());
            builder.put(clonedVar, clonedExpr);
        }
        return builder.build();
    }

    private List<VariableReferenceExpression> rewriteVariableList(List<VariableReferenceExpression> variables)
    {
        return variables.stream()
                .map(this::getClonedVariable)
                .collect(toImmutableList());
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
    {
        Map<VariableReferenceExpression, ColumnHandle> newAssignments = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : node.getAssignments().entrySet()) {
            newAssignments.put(getClonedVariable(entry.getKey()), entry.getValue());
        }

        return new TableScanNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                node.getTable(),
                rewriteVariableList(node.getOutputVariables()),
                newAssignments,
                node.getTableConstraints(),
                node.getCurrentConstraint(),
                node.getEnforcedConstraint(),
                node.getCteMaterializationInfo());
    }

    @Override
    public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
    {
        PlanNode source = context.rewrite(node.getSource());

        return new ProjectNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                source,
                rewriteAssignments(node.getAssignments()),
                node.getLocality());
    }

    @Override
    public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
    {
        PlanNode source = context.rewrite(node.getSource());

        return new FilterNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                source,
                rewriteExpression(node.getPredicate()));
    }

    @Override
    public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
    {
        PlanNode left = context.rewrite(node.getLeft());
        PlanNode right = context.rewrite(node.getRight());

        List<EquiJoinClause> clonedCriteria = node.getCriteria().stream()
                .map(clause -> new EquiJoinClause(
                        getClonedVariable(clause.getLeft()),
                        getClonedVariable(clause.getRight())))
                .collect(toImmutableList());

        Optional<RowExpression> clonedFilter = node.getFilter()
                .map(this::rewriteExpression);

        List<VariableReferenceExpression> clonedOutputVariables = node.getOutputVariables().stream()
                .map(this::getClonedVariable)
                .collect(toImmutableList());

        return new JoinNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                node.getType(),
                left,
                right,
                clonedCriteria,
                clonedOutputVariables,
                clonedFilter,
                node.getLeftHashVariable().map(this::getClonedVariable),
                node.getRightHashVariable().map(this::getClonedVariable),
                node.getDistributionType(),
                node.getDynamicFilters());
    }

    @Override
    public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
    {
        PlanNode source = context.rewrite(node.getSource());

        Map<VariableReferenceExpression, AggregationNode.Aggregation> clonedAggregations = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
            VariableReferenceExpression clonedVar = getClonedVariable(entry.getKey());
            AggregationNode.Aggregation clonedAgg = rewriteAggregation(entry.getValue());
            clonedAggregations.put(clonedVar, clonedAgg);
        }

        AggregationNode.GroupingSetDescriptor clonedGroupingSets = rewriteGroupingSetDescriptor(node.getGroupingSets());

        return new AggregationNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                node.getStatsEquivalentPlanNode().map(n -> n.accept(this, context)),
                source,
                clonedAggregations,
                clonedGroupingSets,
                rewriteVariableList(node.getPreGroupedVariables()),
                node.getStep(),
                node.getHashVariable().map(this::getClonedVariable),
                node.getGroupIdVariable().map(this::getClonedVariable),
                node.getAggregationId());
    }

    @Override
    public PlanNode visitUnion(UnionNode node, RewriteContext<Void> context)
    {
        List<PlanNode> clonedSources = node.getSources().stream()
                .map(source -> source.accept(this, context))
                .collect(toImmutableList());

        List<VariableReferenceExpression> clonedOutputVariables = rewriteVariableList(node.getOutputVariables());

        Map<VariableReferenceExpression, List<VariableReferenceExpression>> clonedMapping = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> entry : node.getVariableMapping().entrySet()) {
            VariableReferenceExpression clonedOutput = getClonedVariable(entry.getKey());
            List<VariableReferenceExpression> clonedInputs = rewriteVariableList(entry.getValue());
            clonedMapping.put(clonedOutput, clonedInputs);
        }

        return new UnionNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                node.getStatsEquivalentPlanNode().map(n -> n.accept(this, context)),
                clonedSources,
                clonedOutputVariables,
                clonedMapping);
    }

    @Override
    public PlanNode visitIntersect(IntersectNode node, RewriteContext<Void> context)
    {
        List<PlanNode> clonedSources = node.getSources().stream()
                .map(source -> source.accept(this, context))
                .collect(toImmutableList());

        List<VariableReferenceExpression> clonedOutputVariables = rewriteVariableList(node.getOutputVariables());

        Map<VariableReferenceExpression, List<VariableReferenceExpression>> clonedMapping = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> entry : node.getVariableMapping().entrySet()) {
            VariableReferenceExpression clonedOutput = getClonedVariable(entry.getKey());
            List<VariableReferenceExpression> clonedInputs = rewriteVariableList(entry.getValue());
            clonedMapping.put(clonedOutput, clonedInputs);
        }

        return new IntersectNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                node.getStatsEquivalentPlanNode().map(n -> n.accept(this, context)),
                clonedSources,
                clonedOutputVariables,
                clonedMapping);
    }

    @Override
    public PlanNode visitExcept(ExceptNode node, RewriteContext<Void> context)
    {
        List<PlanNode> clonedSources = node.getSources().stream()
                .map(source -> source.accept(this, context))
                .collect(toImmutableList());

        List<VariableReferenceExpression> clonedOutputVariables = rewriteVariableList(node.getOutputVariables());

        Map<VariableReferenceExpression, List<VariableReferenceExpression>> clonedMapping = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> entry : node.getVariableMapping().entrySet()) {
            VariableReferenceExpression clonedOutput = getClonedVariable(entry.getKey());
            List<VariableReferenceExpression> clonedInputs = rewriteVariableList(entry.getValue());
            clonedMapping.put(clonedOutput, clonedInputs);
        }

        return new ExceptNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                node.getStatsEquivalentPlanNode().map(n -> n.accept(this, context)),
                clonedSources,
                clonedOutputVariables,
                clonedMapping);
    }

    @Override
    public PlanNode visitSort(SortNode node, RewriteContext<Void> context)
    {
        PlanNode source = context.rewrite(node.getSource());

        return new SortNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                source,
                rewriteOrderingScheme(node.getOrderingScheme()),
                node.isPartial(),
                rewriteVariableList(node.getPartitionBy()));
    }

    @Override
    public PlanNode visitLimit(LimitNode node, RewriteContext<Void> context)
    {
        PlanNode source = context.rewrite(node.getSource());

        return new LimitNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                node.getStatsEquivalentPlanNode().map(n -> n.accept(this, context)),
                source,
                node.getCount(),
                node.getStep());
    }

    @Override
    public PlanNode visitValues(ValuesNode node, RewriteContext<Void> context)
    {
        List<VariableReferenceExpression> clonedOutputVariables = rewriteVariableList(node.getOutputVariables());

        List<List<RowExpression>> clonedRows = node.getRows().stream()
                .map(row -> row.stream()
                        .map(this::rewriteExpression)
                        .collect(toImmutableList()))
                .collect(toImmutableList());

        return new ValuesNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                node.getStatsEquivalentPlanNode().map(n -> n.accept(this, context)),
                clonedOutputVariables,
                clonedRows,
                node.getValuesNodeLabel());
    }

    @Override
    public PlanNode visitWindow(WindowNode node, RewriteContext<Void> context)
    {
        PlanNode source = context.rewrite(node.getSource());

        Map<VariableReferenceExpression, WindowNode.Function> clonedWindowFunctions = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
            VariableReferenceExpression clonedVar = getClonedVariable(entry.getKey());
            WindowNode.Function clonedFunction = rewriteWindowFunction(entry.getValue());
            clonedWindowFunctions.put(clonedVar, clonedFunction);
        }

        return new WindowNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                node.getStatsEquivalentPlanNode().map(n -> n.accept(this, context)),
                source,
                rewriteDataOrganizationSpecification(node.getSpecification()),
                clonedWindowFunctions,
                node.getHashVariable().map(this::getClonedVariable),
                node.getPrePartitionedInputs().stream()
                        .map(this::getClonedVariable)
                        .collect(ImmutableSet.toImmutableSet()),
                node.getPreSortedOrderPrefix());
    }

    @Override
    public PlanNode visitGroupReference(com.facebook.presto.sql.planner.iterative.GroupReference node, RewriteContext<Void> context)
    {
        // GroupReferences can appear when cloning plans in the iterative optimizer
        if (lookup != null) {
            // Resolve the GroupReference and clone the resolved plan
            PlanNode resolved = lookup.resolve(node);
            return resolved.accept(this, context);
        }
        // If no lookup available, fall back to default behavior (will likely fail)
        return context.defaultRewrite(node);
    }

    /**
     * Helper method to rewrite an Aggregation with cloned variables.
     */
    private AggregationNode.Aggregation rewriteAggregation(AggregationNode.Aggregation aggregation)
    {
        return new AggregationNode.Aggregation(
                (com.facebook.presto.spi.relation.CallExpression) rewriteExpression(aggregation.getCall()),
                aggregation.getFilter().map(this::rewriteExpression),
                aggregation.getOrderBy().map(this::rewriteOrderingScheme),
                aggregation.isDistinct(),
                aggregation.getMask().map(this::getClonedVariable));
    }

    /**
     * Helper method to rewrite GroupingSetDescriptor with cloned variables.
     */
    private AggregationNode.GroupingSetDescriptor rewriteGroupingSetDescriptor(AggregationNode.GroupingSetDescriptor descriptor)
    {
        return new AggregationNode.GroupingSetDescriptor(
                rewriteVariableList(descriptor.getGroupingKeys()),
                descriptor.getGroupingSetCount(),
                descriptor.getGlobalGroupingSets());
    }

    /**
     * Helper method to rewrite OrderingScheme with cloned variables.
     */
    private OrderingScheme rewriteOrderingScheme(OrderingScheme scheme)
    {
        List<Ordering> clonedOrderings = scheme.getOrderBy().stream()
                .map(ordering -> new Ordering(
                        getClonedVariable(ordering.getVariable()),
                        ordering.getSortOrder()))
                .collect(toImmutableList());
        return new OrderingScheme(clonedOrderings);
    }

    /**
     * Helper method to rewrite DataOrganizationSpecification with cloned variables.
     */
    private DataOrganizationSpecification rewriteDataOrganizationSpecification(DataOrganizationSpecification spec)
    {
        return new DataOrganizationSpecification(
                rewriteVariableList(spec.getPartitionBy()),
                spec.getOrderingScheme().map(this::rewriteOrderingScheme));
    }

    /**
     * Helper method to rewrite a window function with cloned variables.
     */
    private WindowNode.Function rewriteWindowFunction(WindowNode.Function function)
    {
        return new WindowNode.Function(
                (com.facebook.presto.spi.relation.CallExpression) rewriteExpression(function.getFunctionCall()),
                rewriteFrame(function.getFrame()),
                function.isIgnoreNulls());
    }

    /**
     * Helper method to rewrite a window frame with cloned variables.
     */
    private WindowNode.Frame rewriteFrame(WindowNode.Frame frame)
    {
        return new WindowNode.Frame(
                frame.getType(),
                frame.getStartType(),
                frame.getStartValue().map(this::getClonedVariable),
                frame.getSortKeyCoercedForFrameStartComparison().map(this::getClonedVariable),
                frame.getEndType(),
                frame.getEndValue().map(this::getClonedVariable),
                frame.getSortKeyCoercedForFrameEndComparison().map(this::getClonedVariable),
                frame.getOriginalStartValue(),
                frame.getOriginalEndValue());
    }

    @Override
    public PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
    {
        throw new UnsupportedOperationException(String.format(
                "Plan node type %s is not supported by PlanClonerWithVariableMapping. " +
                "Please add an explicit visitor method for this node type to handle variable remapping correctly.",
                node.getClass().getSimpleName()));
    }

    public static class ClonedPlan
    {
        private final PlanNode plan;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping;

        public ClonedPlan(PlanNode plan, Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping)
        {
            this.plan = requireNonNull(plan, "plan is null");
            this.variableMapping = requireNonNull(variableMapping, "variableMapping is null");
        }

        public PlanNode getPlan()
        {
            return plan;
        }

        public Map<VariableReferenceExpression, VariableReferenceExpression> getVariableMapping()
        {
            return variableMapping;
        }
    }
}
