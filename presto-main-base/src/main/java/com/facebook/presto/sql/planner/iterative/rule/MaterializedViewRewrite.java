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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewDefinition.TableColumn;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.MaterializedViewScanNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.ViewExpression;
import com.facebook.presto.spi.security.ViewSecurity;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.MaterializedViewStitchingUtils.ColumnEquivalences;
import com.facebook.presto.sql.planner.optimizations.PlanClonerWithVariableMapping;
import com.facebook.presto.sql.planner.optimizations.SetOperationNodeUtils;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.MaterializedViewDataConsistencyMode;
import static com.facebook.presto.SystemSessionProperties.MaterializedViewDataConsistencyMode.USE_DATA_TABLE;
import static com.facebook.presto.SystemSessionProperties.MaterializedViewDataConsistencyMode.USE_STITCHING;
import static com.facebook.presto.SystemSessionProperties.getMaterializedViewDataConsistencyMode;
import static com.facebook.presto.SystemSessionProperties.isLegacyMaterializedViews;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import static com.facebook.presto.spi.StandardWarningCode.MATERIALIZED_VIEW_ACCESS_CONTROL_FALLBACK;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.security.ViewSecurity.DEFINER;
import static com.facebook.presto.spi.security.ViewSecurity.INVOKER;
import static com.facebook.presto.sql.planner.optimizations.MaterializedViewStitchingUtils.applyStalePredicates;
import static com.facebook.presto.sql.planner.optimizations.MaterializedViewStitchingUtils.buildColumnToVariableMapping;
import static com.facebook.presto.sql.planner.optimizations.MaterializedViewStitchingUtils.canPerformPartitionLevelRefresh;
import static com.facebook.presto.sql.planner.optimizations.MaterializedViewStitchingUtils.filterPredicatesToMappedColumns;
import static com.facebook.presto.sql.planner.plan.Patterns.materializedViewScan;
import static com.facebook.presto.sql.relational.Expressions.not;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.toMap;
import static java.util.Objects.requireNonNull;

/**
 * Rewrites {@link MaterializedViewScanNode} to use pre-computed data when possible.
 *
 * <h3>Rewrite Modes</h3>
 * <p>Controlled by the {@code materialized_view_data_consistency} session property:
 * <ul>
 *   <li>{@code USE_DATA_TABLE}: Always read from MV storage table, even if stale.</li>
 *   <li>{@code USE_STITCHING}: If fully fresh, use data table; if partially stale, build
 *       UNION(fresh data, recomputed stale data); otherwise fall back to full recompute.</li>
 *   <li>{@code USE_VIEW_QUERY}: Always execute the original view query (full recompute).
 *       Used for testing.</li>
 * </ul>
 *
 * <h3>Access Control</h3>
 * <p>For INVOKER security mode views, the data table cannot be used if row filters or column
 * masks exist on base tables, since these depend on the invoking user's identity. DEFINER mode
 * views are unaffected as access control was applied at materialization time.
 *
 * <h3>Union Stitching Details</h3>
 * <p>When stitching is enabled and feasible, the rewrite produces:
 * <pre>
 *   UNION ALL(
 *     FilterNode(NOT stale_predicate) -> DataTableScan,   // fresh rows from MV storage
 *     FilterNode(stale_predicate) -> ViewQuery)            // recomputed stale rows
 * </pre>
 *
 * <p>Stitching is disabled when:
 * <ul>
 *   <li>The view query contains outer joins (LEFT/RIGHT/FULL)</li>
 *   <li>The view query contains non-deterministic functions</li>
 *   <li>Stale predicates cannot be mapped through passthrough columns (see below)</li>
 * </ul>
 *
 * <h3>Passthrough Column Mappings</h3>
 * <p>Stale predicates from the connector (e.g., partition constraints) reference base table columns.
 * These must be mapped to MV output columns via "passthrough" mappings - columns that flow directly
 * from base tables to MV output without transformation. Join equalities can create multiple base
 * columns mapping to the same MV column (e.g., {@code t1.dt = t2.dt} both map to {@code mv.dt}).
 *
 * <h3>Multi-Table Staleness and Exclusion Predicates</h3>
 * <p>When multiple base tables are stale, each gets its own recompute branch in the UNION.
 * To prevent duplicate rows (e.g., when table A's stale partition joins with table B's stale
 * partition), exclusion predicates are applied: each branch excludes rows matching previously
 * processed stale tables' predicates. For example, with stale tables A and B:
 * <pre>
 *   Branch for A: WHERE stale_predicate_A
 *   Branch for B: WHERE stale_predicate_B AND NOT(stale_predicate_A)
 * </pre>
 */
public class MaterializedViewRewrite
        implements Rule<MaterializedViewScanNode>
{
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final RowExpressionDomainTranslator translator;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;

    public MaterializedViewRewrite(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.translator = new RowExpressionDomainTranslator(metadata);
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager());
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
        checkState(!isLegacyMaterializedViews(session), "Materialized view rewrite rule should not fire when legacy materialized views are enabled");

        VariableAllocator variableAllocator = context.getVariableAllocator();
        PlanNodeIdAllocator idAllocator = context.getIdAllocator();
        MetadataResolver metadataResolver = metadata.getMetadataResolver(session);

        MaterializedViewStatus status = metadataResolver.getMaterializedViewStatus(node.getMaterializedViewName(), TupleDomain.all());

        boolean canUseDataTable = canUseDataTable(session, context, node, metadataResolver);
        if (!status.isFullyMaterialized() && !status.getPartitionsFromBaseTables().isEmpty()) {
            Map<SchemaTableName, MaterializedDataPredicates> constraints = status.getPartitionsFromBaseTables();

            if (canUseDataTable && canPerformUnionStitching(node, constraints, context.getLookup())) {
                Optional<MaterializedViewDefinition> materializedViewDefinition = metadataResolver.getMaterializedView(node.getMaterializedViewName());
                checkState(materializedViewDefinition.isPresent(), "MV definition not found for: %s", node.getMaterializedViewName());

                SchemaTableName dataTable = new SchemaTableName(materializedViewDefinition.get().getSchema(), materializedViewDefinition.get().getTable());
                ColumnEquivalences columnEquivalences = new ColumnEquivalences(materializedViewDefinition.get(), dataTable);
                Optional<PlanNode> unionPlan = buildStitchedQueryPlan(session, node, constraints, columnEquivalences, dataTable, variableAllocator, idAllocator, context.getLookup());

                if (unionPlan.isPresent()) {
                    return Result.ofPlanNode(unionPlan.get());
                }
            }
        }

        PlanNode plan;
        Map<VariableReferenceExpression, VariableReferenceExpression> mappings;
        if ((status.isFullyMaterialized() && canUseDataTable)
                || getMaterializedViewDataConsistencyMode(session) == USE_DATA_TABLE) {
            plan = node.getDataTablePlan();
            mappings = node.getDataTableMappings();
        }
        else {
            plan = node.getViewQueryPlan();
            mappings = node.getViewQueryMappings();
        }

        Assignments.Builder assignments = Assignments.builder();
        for (VariableReferenceExpression outputVariable : node.getOutputVariables()) {
            VariableReferenceExpression sourceVariable = mappings.get(outputVariable);
            requireNonNull(sourceVariable, "No mapping found for output variable: " + outputVariable);
            assignments.put(outputVariable, sourceVariable);
        }

        return Result.ofPlanNode(new ProjectNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                plan,
                assignments.build(),
                LOCAL));
    }

    private boolean canUseDataTable(
            Session session,
            Context context,
            MaterializedViewScanNode node,
            MetadataResolver metadataResolver)
    {
        MaterializedViewDataConsistencyMode consistencyMode = getMaterializedViewDataConsistencyMode(session);
        if (consistencyMode != USE_STITCHING) {
            return consistencyMode == USE_DATA_TABLE;
        }

        Optional<MaterializedViewDefinition> materializedViewDefinition = metadataResolver.getMaterializedView(node.getMaterializedViewName());
        checkState(materializedViewDefinition.isPresent(), "Materialized view definition not found for: %s", node.getMaterializedViewName());
        ViewSecurity securityMode = materializedViewDefinition.get().getSecurityMode().orElse(INVOKER);

        // Definer rights: row filters/column masks don't depend on invoker
        if (securityMode == DEFINER) {
            return true;
        }

        String catalogName = node.getMaterializedViewName().getCatalogName();
        for (SchemaTableName schemaTableName : materializedViewDefinition.get().getBaseTables()) {
            QualifiedObjectName baseTable = new QualifiedObjectName(catalogName, schemaTableName.getSchemaName(), schemaTableName.getTableName());

            List<ViewExpression> rowFilters = accessControl.getRowFilters(
                    session.getTransactionId().get(),
                    session.getIdentity(),
                    session.getAccessControlContext(),
                    baseTable);

            if (!rowFilters.isEmpty()) {
                context.getWarningCollector().add(new PrestoWarning(
                        MATERIALIZED_VIEW_ACCESS_CONTROL_FALLBACK,
                        "Cannot use materialized view data table for " + node.getMaterializedViewName() +
                                ": row filters exist on base table " + baseTable + " with INVOKER security mode"));
                return false;
            }

            Optional<TableHandle> tableHandle = metadataResolver.getTableHandle(baseTable);
            if (!tableHandle.isPresent()) {
                return false;
            }

            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.get());
            List<ColumnMetadata> columnsMetadata = columnHandles.values().stream()
                    .map(handle -> metadata.getColumnMetadata(session, tableHandle.get(), handle))
                    .collect(toImmutableList());

            Map<ColumnMetadata, ViewExpression> columnMasks = accessControl.getColumnMasks(
                    session.getTransactionId().get(),
                    session.getIdentity(),
                    session.getAccessControlContext(),
                    baseTable,
                    columnsMetadata);

            if (!columnMasks.isEmpty()) {
                context.getWarningCollector().add(new PrestoWarning(
                        MATERIALIZED_VIEW_ACCESS_CONTROL_FALLBACK,
                        "Cannot use materialized view data table for " + node.getMaterializedViewName() +
                                ": column masks exist on base table " + baseTable + " with INVOKER security mode"));
                return false;
            }
        }

        return true;
    }

    private boolean canPerformUnionStitching(
            MaterializedViewScanNode node,
            Map<SchemaTableName, MaterializedDataPredicates> constraints,
            Lookup lookup)
    {
        return canPerformPartitionLevelRefresh(node.getViewQueryPlan(), constraints, determinismEvaluator, lookup);
    }

    private Optional<PlanNode> buildStitchedQueryPlan(
            Session session,
            MaterializedViewScanNode node,
            Map<SchemaTableName, MaterializedDataPredicates> constraints,
            ColumnEquivalences columnEquivalences,
            SchemaTableName dataTable,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Lookup lookup)
    {
        Map<SchemaTableName, List<TupleDomain<String>>> filteredConstraints = filterPredicatesToMappedColumns(constraints, columnEquivalences);
        if (filteredConstraints.values().stream().anyMatch(List::isEmpty)) {
            return Optional.empty();
        }

        PlanNode freshPlan = buildDataTableBranch(node, filteredConstraints, columnEquivalences, dataTable, idAllocator, session, lookup);

        Optional<PlanNode> staleQuery = buildViewQueryBranch(
                node, filteredConstraints, columnEquivalences, variableAllocator, idAllocator, session, lookup);

        if (!staleQuery.isPresent()) {
            return Optional.empty();
        }

        Map<VariableReferenceExpression, VariableReferenceExpression> identityMapping =
                toMap(node.getOutputVariables(), v -> v);

        List<PlanNode> branches = ImmutableList.of(freshPlan, staleQuery.get());
        List<Map<VariableReferenceExpression, VariableReferenceExpression>> branchMappings =
                ImmutableList.of(node.getDataTableMappings(), identityMapping);

        return Optional.of(buildUnionNode(node, branches, branchMappings, idAllocator));
    }

    private PlanNode buildDataTableBranch(
            MaterializedViewScanNode node,
            Map<SchemaTableName, List<TupleDomain<String>>> constraints,
            ColumnEquivalences columnEquivalences,
            SchemaTableName dataTable,
            PlanNodeIdAllocator idAllocator,
            Session session,
            Lookup lookup)
    {
        PlanNode plan = node.getDataTablePlan();
        Map<TableColumn, VariableReferenceExpression> columnToVariable = buildColumnToVariableMapping(plan, metadata, session, lookup);

        ImmutableList.Builder<RowExpression> stalePredicatesPerTable = ImmutableList.builder();
        for (Map.Entry<SchemaTableName, List<TupleDomain<String>>> entry : constraints.entrySet()) {
            SchemaTableName sourceTable = entry.getKey();
            List<TupleDomain<String>> stalePredicates = entry.getValue();

            List<RowExpression> predicateExpressions = stalePredicates.stream()
                    .map(disjunct -> columnEquivalences.getEquivalentPredicates(sourceTable, disjunct).get(dataTable))
                    .filter(Objects::nonNull)
                    .map(dataTablePredicate -> dataTablePredicate.transform(col -> columnToVariable.get(new TableColumn(dataTable, col))))
                    .map(translator::toPredicate)
                    .collect(toImmutableList());
            stalePredicatesPerTable.add(or(predicateExpressions));
        }

        RowExpression freshPredicate = not(metadata.getFunctionAndTypeManager(), or(stalePredicatesPerTable.build()));
        return new FilterNode(
                plan.getSourceLocation(),
                idAllocator.getNextId(),
                plan,
                freshPredicate);
    }

    private Optional<PlanNode> buildViewQueryBranch(
            MaterializedViewScanNode node,
            Map<SchemaTableName, List<TupleDomain<String>>> filteredConstraints,
            ColumnEquivalences columnEquivalences,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Session session,
            Lookup lookup)
    {
        ImmutableList.Builder<PlanNode> branchPlans = ImmutableList.builder();
        ImmutableList.Builder<Map<VariableReferenceExpression, VariableReferenceExpression>> branchMappings =
                ImmutableList.builder();

        // Track processed tables to build exclusion predicates for subsequent branches
        Map<SchemaTableName, List<TupleDomain<String>>> processedTables = new HashMap<>();

        for (Map.Entry<SchemaTableName, List<TupleDomain<String>>> entry : filteredConstraints.entrySet()) {
            SchemaTableName staleTable = entry.getKey();
            List<TupleDomain<String>> stalePredicates = entry.getValue();

            // Exclusions = snapshot of all previously processed stale tables
            Map<SchemaTableName, List<TupleDomain<String>>> exclusions = ImmutableMap.copyOf(processedTables);

            PlanClonerWithVariableMapping.ClonedPlan clonedPlan =
                    PlanClonerWithVariableMapping.clonePlan(node.getViewQueryPlan(), variableAllocator, idAllocator, lookup);

            PlanNode filteredBranch = applyStalePredicates(
                    clonedPlan.getPlan(),
                    staleTable,
                    stalePredicates,
                    exclusions,
                    columnEquivalences,
                    translator,
                    idAllocator,
                    metadata,
                    session,
                    lookup);
            branchPlans.add(filteredBranch);

            // Build mapping from MV output variables to cloned branch variables for the union
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> mapping = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> viewMapping : node.getViewQueryMappings().entrySet()) {
                VariableReferenceExpression clonedVar = clonedPlan.getVariableMapping().get(viewMapping.getValue());
                if (clonedVar != null) {
                    mapping.put(viewMapping.getKey(), clonedVar);
                }
            }
            branchMappings.add(mapping.build());
            processedTables.put(staleTable, stalePredicates);
        }

        List<PlanNode> plans = branchPlans.build();
        List<Map<VariableReferenceExpression, VariableReferenceExpression>> mappings = branchMappings.build();

        if (plans.isEmpty()) {
            return Optional.empty();
        }

        PlanNode sourcePlan;
        Map<VariableReferenceExpression, VariableReferenceExpression> outputMapping;
        if (plans.size() == 1) {
            sourcePlan = plans.get(0);
            outputMapping = mappings.get(0);
        }
        else {
            sourcePlan = buildUnionNode(node, plans, mappings, idAllocator);
            outputMapping = toMap(node.getOutputVariables(), v -> v);
        }

        Assignments.Builder assignments = Assignments.builder();
        outputMapping.forEach(assignments::put);
        return Optional.of(new ProjectNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                sourcePlan,
                assignments.build(),
                LOCAL));
    }

    private PlanNode buildUnionNode(
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
        return new UnionNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                branches,
                ImmutableList.copyOf(mapping.keySet()),
                SetOperationNodeUtils.fromListMultimap(mapping));
    }
}
