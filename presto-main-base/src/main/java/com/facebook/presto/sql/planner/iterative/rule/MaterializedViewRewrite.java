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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.MaterializedViewScanNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.PlanClonerWithVariableMapping;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.optimizations.SetOperationNodeUtils;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.relational.Expressions;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.JoinType.FULL;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.planner.plan.Patterns.materializedViewScan;
import static com.facebook.presto.sql.relational.Expressions.not;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public class MaterializedViewRewrite
        implements Rule<MaterializedViewScanNode>
{
    private static final Logger log = Logger.get(MaterializedViewRewrite.class);
    private final Metadata metadata;

    public MaterializedViewRewrite(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<MaterializedViewScanNode> getPattern()
    {
        return materializedViewScan();
    }

    @Override
    public Result apply(MaterializedViewScanNode node, Captures captures, Context context)
    {
        Session session = context.getSession();
        VariableAllocator variableAllocator = context.getVariableAllocator();
        PlanNodeIdAllocator idAllocator = context.getIdAllocator();

        MetadataResolver metadataResolver = metadata.getMetadataResolver(session);

        MaterializedViewStatus status = metadataResolver.getMaterializedViewStatus(
                node.getMaterializedViewName(),
                TupleDomain.all());

        boolean useFreshData;
        if (!status.isFullyMaterialized() &&
                status.getStaleDataConstraints().isPresent() &&
                !status.getStaleDataConstraints().get().isEmpty()) {
            MaterializedViewStatus.StaleDataConstraints constraints = status.getStaleDataConstraints().get();

            if (canPerformUnionStitching(node, constraints, context.getLookup())) {
                PlanNode unionPlan = buildUnionStitchedPlan(node, constraints, variableAllocator, idAllocator, session, context.getLookup());

                if (unionPlan != null) {
                    return Result.ofPlanNode(unionPlan);
                }
            }
        }
        useFreshData = false;

        PlanNode chosenPlan = useFreshData ? node.getDataTablePlan() : node.getViewQueryPlan();
        Map<VariableReferenceExpression, VariableReferenceExpression> chosenMappings =
                useFreshData ? node.getDataTableMappings() : node.getViewQueryMappings();

        requireNonNull(chosenMappings, "chosenMappings is null (useFreshData=" + useFreshData +
                ", dataTableMappings=" + node.getDataTableMappings() +
                ", viewQueryMappings=" + node.getViewQueryMappings() + ")");

        Assignments.Builder assignments = Assignments.builder();
        for (VariableReferenceExpression outputVariable : node.getOutputVariables()) {
            VariableReferenceExpression sourceVariable = chosenMappings.get(outputVariable);
            if (sourceVariable != null) {
                assignments.put(outputVariable, sourceVariable);
            }
            else {
                assignments.put(outputVariable, outputVariable);
            }
        }

        return Result.ofPlanNode(new ProjectNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                chosenPlan,
                assignments.build(),
                LOCAL));
    }

    private boolean canPerformUnionStitching(
            MaterializedViewScanNode node,
            MaterializedViewStatus.StaleDataConstraints constraints,
            Lookup lookup)
    {
        return !constraints.getDataDisjuncts().isEmpty() &&
                constraints.getConjunctiveConstraints().isEmpty() &&
                !containsOuterJoin(node.getViewQueryPlan(), lookup);
    }

    private boolean containsOuterJoin(PlanNode plan, Lookup lookup)
    {
        return PlanNodeSearcher.searchFrom(plan, lookup)
                .where(node -> node instanceof JoinNode &&
                        (((JoinNode) node).getType() == LEFT ||
                         ((JoinNode) node).getType() == RIGHT ||
                         ((JoinNode) node).getType() == FULL))
                .matches();
    }

    private PlanNode buildUnionStitchedPlan(
            MaterializedViewScanNode node,
            MaterializedViewStatus.StaleDataConstraints constraints,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Session session,
            Lookup lookup)
    {
        return buildMultiTableUnionPlan(node, constraints, variableAllocator, idAllocator, session, lookup);
    }

    private PlanNode buildMultiTableUnionPlan(
            MaterializedViewScanNode node,
            MaterializedViewStatus.StaleDataConstraints constraints,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Session session,
            Lookup lookup)
    {
        ImmutableList.Builder<PlanNode> branchPlans = ImmutableList.builder();
        ImmutableList.Builder<Map<VariableReferenceExpression, VariableReferenceExpression>> branchMappings =
                ImmutableList.builder();

        PlanNode freshPlan = buildFreshBranchForMultiTable(node, constraints, idAllocator, session, lookup);
        branchPlans.add(freshPlan);
        branchMappings.add(node.getDataTableMappings());

        List<PlanClonerWithVariableMapping.ClonedPlan> staleBranches = buildMultiTableStaleBranches(
                node, constraints, variableAllocator, idAllocator, session, lookup);
        for (PlanClonerWithVariableMapping.ClonedPlan staleBranch : staleBranches) {
            branchPlans.add(staleBranch.getPlan());

            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : node.getViewQueryMappings().entrySet()) {
                VariableReferenceExpression outputVar = entry.getKey();
                VariableReferenceExpression originalVar = entry.getValue();
                VariableReferenceExpression clonedVar = staleBranch.getVariableMapping().get(originalVar);
                if (clonedVar != null) {
                    mapping.put(outputVar, clonedVar);
                }
            }
            branchMappings.add(mapping.build());
        }

        List<PlanNode> allBranchPlans = branchPlans.build();
        List<Map<VariableReferenceExpression, VariableReferenceExpression>> allBranchMappings = branchMappings.build();

        if (allBranchPlans.isEmpty()) {
            return null;
        }
        else if (allBranchPlans.size() == 1) {
            return allBranchPlans.get(0);
        }
        else {
            return buildMultiWayUnionNode(node, allBranchPlans, allBranchMappings, idAllocator);
        }
    }

    private PlanNode buildFreshBranchForMultiTable(
            MaterializedViewScanNode node,
            MaterializedViewStatus.StaleDataConstraints constraints,
            PlanNodeIdAllocator idAllocator,
            Session session,
            Lookup lookup)
    {
        ImmutableList.Builder<TupleDomain<String>> allStalePartitions = ImmutableList.builder();
        for (List<TupleDomain<String>> tablePartitions : constraints.getDataDisjuncts().values()) {
            allStalePartitions.addAll(tablePartitions);
        }

        return applyFreshConstraints(
                node.getDataTablePlan(),
                allStalePartitions.build(),
                node.getDataTableMappings(),
                idAllocator,
                session,
                lookup);
    }

    private List<PlanClonerWithVariableMapping.ClonedPlan> buildMultiTableStaleBranches(
            MaterializedViewScanNode node,
            MaterializedViewStatus.StaleDataConstraints constraints,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Session session,
            Lookup lookup)
    {
        ImmutableList.Builder<PlanClonerWithVariableMapping.ClonedPlan> staleBranches = ImmutableList.builder();

        List<Map.Entry<SchemaTableName, List<TupleDomain<String>>>> sortedEntries = constraints.getDataDisjuncts().entrySet().stream()
                .sorted(comparing((Map.Entry<SchemaTableName, List<TupleDomain<String>>> o) -> o.getKey().toString()))
                .collect(ImmutableList.toImmutableList());

        for (Map.Entry<SchemaTableName, List<TupleDomain<String>>> entry : sortedEntries) {
            SchemaTableName staleTable = entry.getKey();
            List<TupleDomain<String>> stalePartitions = entry.getValue();

            log.info("Building stale branch for table %s with %d partitions", staleTable, stalePartitions.size());
            for (int i = 0; i < stalePartitions.size(); i++) {
                log.info("  Partition %d: %s", i, stalePartitions.get(i));
            }

            PlanClonerWithVariableMapping.ClonedPlan clonedPlan =
                    PlanClonerWithVariableMapping.clonePlan(node.getViewQueryPlan(), variableAllocator, idAllocator, lookup);

            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> clonedMappings =
                    ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> mapping : node.getViewQueryMappings().entrySet()) {
                VariableReferenceExpression outputVar = mapping.getKey();
                VariableReferenceExpression originalVar = mapping.getValue();
                VariableReferenceExpression clonedVar = clonedPlan.getVariableMapping().get(originalVar);
                if (clonedVar != null) {
                    clonedMappings.put(outputVar, clonedVar);
                }
            }

            PlanNode filteredBranch = applyInclusiveConstraints(
                    clonedPlan.getPlan(),
                    stalePartitions,
                    clonedMappings.build(),
                    idAllocator,
                    session,
                    lookup);

            PlanClonerWithVariableMapping.ClonedPlan filteredClonedPlan =
                    new PlanClonerWithVariableMapping.ClonedPlan(filteredBranch, clonedPlan.getVariableMapping());

            staleBranches.add(filteredClonedPlan);
        }

        return staleBranches.build();
    }

    private PlanNode buildMultiWayUnionNode(
            MaterializedViewScanNode node,
            List<PlanNode> branches,
            List<Map<VariableReferenceExpression, VariableReferenceExpression>> branchMappings,
            PlanNodeIdAllocator idAllocator)
    {
        ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> outputsToInputs =
                ImmutableListMultimap.builder();

        for (VariableReferenceExpression outputVar : node.getOutputVariables()) {
            for (int i = 0; i < branches.size(); i++) {
                Map<VariableReferenceExpression, VariableReferenceExpression> branchMapping = branchMappings.get(i);
                VariableReferenceExpression branchVar = branchMapping.get(outputVar);
                if (branchVar != null) {
                    outputsToInputs.put(outputVar, branchVar);
                }
            }
        }

        ListMultimap<VariableReferenceExpression, VariableReferenceExpression> mapping = outputsToInputs.build();

        PlanNode unionNode = new UnionNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                branches,
                ImmutableList.copyOf(mapping.keySet()),
                SetOperationNodeUtils.fromListMultimap(mapping));

        if (branchMappings.size() > 2) {
            return new AggregationNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    unionNode,
                    ImmutableMap.of(),
                    singleGroupingSet(node.getOutputVariables()),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        return unionNode;
    }

    private TupleDomain<VariableReferenceExpression> mapTupleDomainToVariables(
            PlanNode plan,
            TupleDomain<String> columnConstraint,
            Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings,
            Session session,
            Lookup lookup)
    {
        if (columnConstraint.isAll()) {
            return TupleDomain.all();
        }

        if (columnConstraint.isNone()) {
            return TupleDomain.none();
        }

        Map<String, VariableReferenceExpression> columnToVariable = buildColumnToVariableMapping(plan, variableMappings, session, lookup);

        Optional<Map<String, Domain>> columnsOptional = columnConstraint.getDomains();
        if (!columnsOptional.isPresent()) {
            return TupleDomain.all();
        }

        ImmutableMap.Builder<VariableReferenceExpression, Domain> variableDomainsBuilder = ImmutableMap.builder();
        for (Map.Entry<String, Domain> entry : columnsOptional.get().entrySet()) {
            String columnName = entry.getKey();
            Domain domain = entry.getValue();

            VariableReferenceExpression planVariable = columnToVariable.get(columnName);
            if (planVariable == null) {
                continue;
            }

            variableDomainsBuilder.put(planVariable, domain);
        }

        return TupleDomain.withColumnDomains(variableDomainsBuilder.build());
    }

    private Map<String, VariableReferenceExpression> buildColumnToVariableMapping(
            PlanNode plan,
            Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings,
            Session session,
            Lookup lookup)
    {
        Map<String, VariableReferenceExpression> columnToVariable = new HashMap<>();

        if (plan instanceof TableScanNode) {
            TableScanNode tableScan = (TableScanNode) plan;

            for (Map.Entry<VariableReferenceExpression, ColumnHandle> assignment : tableScan.getAssignments().entrySet()) {
                VariableReferenceExpression variable = assignment.getKey();
                ColumnHandle columnHandle = assignment.getValue();

                ColumnMetadata columnMetadata = metadata.getColumnMetadata(
                        session,
                        tableScan.getTable(),
                        columnHandle);
                String columnName = columnMetadata.getName();

                columnToVariable.put(columnName, variable);
            }
        }
        else {
            SourceVariableResolver resolver = new SourceVariableResolver(lookup);
            plan.accept(resolver, null);
            Map<VariableReferenceExpression, SourceColumnInfo> variableToSource = resolver.getVariableToSourceMap();

            for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : variableMappings.entrySet()) {
                VariableReferenceExpression outputVar = entry.getKey();
                VariableReferenceExpression planVar = entry.getValue();

                SourceColumnInfo sourceInfo = variableToSource.get(planVar);
                if (sourceInfo != null) {
                    ColumnMetadata columnMetadata = metadata.getColumnMetadata(
                            session,
                            sourceInfo.getTableScanNode().getTable(),
                            sourceInfo.getColumnHandle());
                    String columnName = columnMetadata.getName();
                    columnToVariable.put(columnName, planVar);
                }
                else {
                    throw new IllegalStateException("Could not resolve source column for variable " + planVar);
                }
            }
        }

        return columnToVariable;
    }

    private PlanNode applyFreshConstraints(
            PlanNode plan,
            List<TupleDomain<String>> stalePartitions,
            Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings,
            PlanNodeIdAllocator idAllocator,
            Session session,
            Lookup lookup)
    {
        RowExpression stalePredicate = buildCombinedPartitionPredicate(plan, stalePartitions, variableMappings, session, lookup);
        RowExpression freshPredicate = not(metadata.getFunctionAndTypeManager(), stalePredicate);

        return new FilterNode(
                plan.getSourceLocation(),
                idAllocator.getNextId(),
                plan,
                freshPredicate);
    }

    private PlanNode applyInclusiveConstraints(
            PlanNode plan,
            List<TupleDomain<String>> stalePartitions,
            Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings,
            PlanNodeIdAllocator idAllocator,
            Session session,
            Lookup lookup)
    {
        RowExpression stalePredicate = buildCombinedPartitionPredicate(plan, stalePartitions, variableMappings, session, lookup);

        log.info("Created stale predicate with %d partitions: %s", stalePartitions.size(), stalePredicate);

        return new FilterNode(
                plan.getSourceLocation(),
                idAllocator.getNextId(),
                plan,
                stalePredicate);
    }

    private RowExpression buildCombinedPartitionPredicate(
            PlanNode plan,
            List<TupleDomain<String>> stalePartitions,
            Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings,
            Session session,
            Lookup lookup)
    {
        RowExpressionDomainTranslator translator = new RowExpressionDomainTranslator(metadata);
        ImmutableList.Builder<RowExpression> stalePredicates = ImmutableList.builder();

        for (TupleDomain<String> stalePartition : stalePartitions) {
            TupleDomain<VariableReferenceExpression> variableTerm =
                    mapTupleDomainToVariables(plan, stalePartition, variableMappings, session, lookup);

            RowExpression stalePredicate = translator.toPredicate(variableTerm);
            stalePredicates.add(stalePredicate);
        }

        return or(stalePredicates.build());
    }

    private static class SourceColumnInfo
    {
        private final TableScanNode tableScanNode;
        private final ColumnHandle columnHandle;

        public SourceColumnInfo(TableScanNode tableScanNode, ColumnHandle columnHandle)
        {
            this.tableScanNode = requireNonNull(tableScanNode, "tableScanNode is null");
            this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
        }

        public TableScanNode getTableScanNode()
        {
            return tableScanNode;
        }

        public ColumnHandle getColumnHandle()
        {
            return columnHandle;
        }
    }

    private static class SourceVariableResolver
            extends InternalPlanVisitor<Void, Void>
    {
        private final Map<VariableReferenceExpression, SourceColumnInfo> variableToSource = new HashMap<>();
        private final Lookup lookup;

        public SourceVariableResolver(Lookup lookup)
        {
            this.lookup = requireNonNull(lookup, "lookup is null");
        }

        public Map<VariableReferenceExpression, SourceColumnInfo> getVariableToSourceMap()
        {
            return variableToSource;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : node.getAssignments().entrySet()) {
                variableToSource.put(entry.getKey(), new SourceColumnInfo(node, entry.getValue()));
            }
            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            node.getSource().accept(this, context);

            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().entrySet()) {
                VariableReferenceExpression outputVar = entry.getKey();
                RowExpression assignment = entry.getValue();

                if (assignment instanceof VariableReferenceExpression) {
                    VariableReferenceExpression sourceVar = (VariableReferenceExpression) assignment;
                    SourceColumnInfo sourceInfo = variableToSource.get(sourceVar);
                    if (sourceInfo != null) {
                        variableToSource.put(outputVar, sourceInfo);
                    }
                }
            }

            return null;
        }

        @Override
        public Void visitGroupReference(com.facebook.presto.sql.planner.iterative.GroupReference node, Void context)
        {
            PlanNode resolved = lookup.resolve(node);
            resolved.accept(this, context);
            return null;
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, context);
            }
            return null;
        }
    }
}
