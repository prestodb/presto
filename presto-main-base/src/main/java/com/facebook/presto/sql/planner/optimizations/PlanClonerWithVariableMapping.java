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
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

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

    /**
     * Clone a plan node with all variables renamed to avoid conflicts.
     * The VariableAllocator will automatically ensure variable uniqueness.
     *
     * @param plan The plan to clone
     * @param variableAllocator Allocator for creating new variables (registers them in TypeProvider)
     * @param idAllocator Allocator for new plan node IDs
     * @return ClonedPlan containing the cloned plan and variable mapping
     */
    public static ClonedPlan clonePlan(PlanNode plan, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return clonePlan(plan, variableAllocator, idAllocator, null);
    }

    /**
     * Clone a plan node with all variables renamed to avoid conflicts.
     * The VariableAllocator will automatically ensure variable uniqueness.
     * This version accepts a Lookup to handle GroupReferences in the iterative optimizer.
     *
     * @param plan The plan to clone
     * @param variableAllocator Allocator for creating new variables (registers them in TypeProvider)
     * @param idAllocator Allocator for new plan node IDs
     * @param lookup Lookup for resolving GroupReferences (can be null if not in iterative optimizer)
     * @return ClonedPlan containing the cloned plan and variable mapping
     */
    public static ClonedPlan clonePlan(PlanNode plan, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        PlanClonerWithVariableMapping cloner = new PlanClonerWithVariableMapping(idAllocator, variableAllocator, lookup);
        PlanNode clonedPlan = SimplePlanRewriter.rewriteWith(cloner, plan);
        return new ClonedPlan(clonedPlan, ImmutableMap.copyOf(cloner.variableMapping));
    }

    /**
     * Get or create a cloned variable for the given original variable.
     * Uses VariableAllocator to register the new variable in the TypeProvider
     * and automatically ensure uniqueness.
     */
    private VariableReferenceExpression getClonedVariable(VariableReferenceExpression original)
    {
        return variableMapping.computeIfAbsent(original, var ->
            variableAllocator.newVariable(var.getName(), var.getType()));
    }

    /**
     * Rewrite RowExpression to use cloned variables.
     */
    private RowExpression rewriteExpression(RowExpression expression)
    {
        return RowExpressionVariableInliner.inlineVariables(this::getClonedVariable, expression);
    }

    /**
     * Rewrite Assignments to use cloned variables.
     */
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

    /**
     * Rewrite list of variables to use cloned variables.
     */
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
    public PlanNode visitGroupReference(com.facebook.presto.sql.planner.iterative.GroupReference node, RewriteContext<Void> context)
    {
        if (lookup != null) {
            PlanNode resolved = lookup.resolve(node);
            return resolved.accept(this, context);
        }
        return context.defaultRewrite(node);
    }

    @Override
    public PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
    {
        return context.defaultRewrite(node);
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
