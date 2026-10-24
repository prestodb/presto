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
package com.facebook.presto.sql.planner.iterative.rule.materializedview;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ChangedRowsPredicate;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewRefreshType;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.MVRewriteCandidatesNode;
import com.facebook.presto.spi.plan.MVRewriteCandidatesNode.MVRewriteCandidate;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.RefreshMaterializedViewNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getMaterializedViewDefaultRefreshType;
import static com.facebook.presto.SystemSessionProperties.getMaterializedViewIncrementalRefreshStrategy;
import static com.facebook.presto.SystemSessionProperties.isLegacyMaterializedViews;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardWarningCode.MATERIALIZED_VIEW_STITCHING_FALLBACK;
import static com.facebook.presto.sql.planner.iterative.rule.materializedview.DifferentialPlanRewriter.buildDeltaPlanForRefresh;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.refreshMaterializedViewNode;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Optimizer rule that enables incremental refresh of materialized views.
 *
 * <p>When a {@code REFRESH MATERIALIZED VIEW} command is executed, this rule transforms
 * the {@link RefreshMaterializedViewNode} marker into one of:
 * <ul>
 *   <li><b>No-op</b>: Returns an empty ValuesNode when fully materialized (nothing to refresh)</li>
 *   <li><b>Full refresh</b>: Returns the source plan unchanged (scans all base table data)</li>
 *   <li><b>Incremental refresh</b>: Returns a delta plan (scans only stale partition data)</li>
 * </ul>
 *
 * <p>The planner creates: {@code TableFinishNode -> TableWriterNode -> RefreshMaterializedViewNode(source=fullQueryPlan)}
 * <p>This rule replaces the RefreshMaterializedViewNode with the appropriate plan, leaving the write structure intact.
 *
 * @see DifferentialPlanRewriter
 * @see MaterializedViewRewrite
 */
public class IncrementalRefreshRule
        implements Rule<RefreshMaterializedViewNode>
{
    private static final Pattern<RefreshMaterializedViewNode> PATTERN = refreshMaterializedViewNode();

    private final Metadata metadata;

    public IncrementalRefreshRule(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<RefreshMaterializedViewNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return !isLegacyMaterializedViews(session);
    }

    @Override
    public Result apply(RefreshMaterializedViewNode node, Captures captures, Context context)
    {
        Session session = context.getSession();
        PlanNodeIdAllocator idAllocator = context.getIdAllocator();
        VariableAllocator variableAllocator = context.getVariableAllocator();

        SchemaTableName materializedViewName = node.getMaterializedViewName();
        TableHandle storageTableHandle = node.getStorageTableHandle();
        String catalogName = storageTableHandle.getConnectorId().getCatalogName();

        QualifiedObjectName qualifiedViewName = new QualifiedObjectName(
                catalogName,
                materializedViewName.getSchemaName(),
                materializedViewName.getTableName());

        MetadataResolver metadataResolver = metadata.getMetadataResolver(session);

        Optional<MaterializedViewDefinition> materializedViewDefinition = metadataResolver.getMaterializedView(qualifiedViewName);
        if (!materializedViewDefinition.isPresent()) {
            throw new PrestoException(NOT_FOUND, "Materialized view not found: " + qualifiedViewName);
        }

        MaterializedViewRefreshType refreshType = materializedViewDefinition.get().getRefreshType()
                .orElseGet(() -> getMaterializedViewDefaultRefreshType(session));
        if (refreshType != MaterializedViewRefreshType.INCREMENTAL) {
            return Result.ofPlanNode(node.getSource());
        }

        MaterializedViewStatus status = metadataResolver.getMaterializedViewStatus(qualifiedViewName, TupleDomain.all());

        // Row-level eligibility is decided below, against the storage table's partitioning, rather
        // than by a blanket capability check here. Bailing out at this point used to send a base
        // table with row lineage all the way to a full recompute -- strictly worse than the
        // partition-level refresh the same table would have received without it.

        // If fully materialized, nothing to refresh - return empty result
        if (status.isFullyMaterialized()) {
            return Result.ofPlanNode(new ValuesNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    node.getOutputVariables(),
                    ImmutableList.of(),
                    Optional.empty()));
        }

        MaterializedViewRewriteStrategy strategy = getMaterializedViewIncrementalRefreshStrategy(session);
        if (strategy == MaterializedViewRewriteStrategy.NEVER) {
            context.getWarningCollector().add(new PrestoWarning(
                    MATERIALIZED_VIEW_STITCHING_FALLBACK,
                    "Incremental refresh disabled for materialized view " + qualifiedViewName +
                            " (materialized_view_incremental_refresh_strategy=NEVER); falling back to full refresh."));
            return Result.ofPlanNode(node.getSource());
        }

        SchemaTableName dataTable = new SchemaTableName(materializedViewDefinition.get().getSchema(), materializedViewDefinition.get().getTable());
        PassthroughColumnEquivalences columnEquivalences = new PassthroughColumnEquivalences(materializedViewDefinition.get(), dataTable);

        // A mergeable view can be refreshed from its own stored aggregates plus the appended rows,
        // which reads one row per group instead of rereading every row of each affected group. Tried
        // first; it declines quietly when the view or the change set does not qualify.
        //
        // It needs neither partition-level staleness nor a storage table partitioned by the grouping
        // columns, because it emits the complete view: replacing the written partitions and
        // overwriting the whole table then produce the same table, so either commit mode is correct.
        //
        // Recomputing affected groups from the base is not offered. The affected keys are runtime
        // values, so nothing prunes that scan -- 200,049 rows and 251-489ms CPU against the
        // partition-level refresh's 4,010 and 28ms, for identical output -- and that path was
        // reachable only where partition-level already was.
        Map<SchemaTableName, ChangedRowsPredicate> rowLevelPredicates = ImmutableMap.of();

        if (status.hasRowLevelChanges()
                && RowLevelRefreshEnablement.isEnabled(session, materializedViewDefinition.get().getRowLevelIncrementalRefresh())) {
            Map<SchemaTableName, List<TupleDomain<String>>> staleBases = status.getPartitionsFromBaseTables().entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getPredicateDisjuncts()));
            Optional<PlanNode> mergedRefresh = DifferentialPlanRewriter.buildMergedRefreshPlan(
                    node,
                    storageTableHandle,
                    metadata,
                    session,
                    idAllocator,
                    variableAllocator,
                    staleBases,
                    status.getChangedRowsPredicates(),
                    columnEquivalences,
                    context.getLookup(),
                    context.getWarningCollector());
            if (mergedRefresh.isPresent()) {
                // No incrementalRefreshPredicates: those bound a recomputation of the base, and this
                // plan does not read the base beyond the delta, whose own bound the rewriter applied.
                return Result.ofPlanNode(mergedRefresh.get());
            }
        }

        // Bail out only when the connector reported no stale base at all. A base that is stale but
        // carries no partition disjuncts -- an unpartitioned or bounded V3 base -- must still reach
        // the predicate rewriter below: its incrementalRefreshPredicate is the whole point of a
        // bounded refresh, and the full storage overwrite the connector then performs is exactly
        // the right commit for a recompute that is complete up to that bound. Requiring
        // hasPartitionRefreshData() here, as the row-level guard above legitimately does, silently
        // dropped that bound and refreshed unbounded.
        if (status.getPartitionsFromBaseTables().isEmpty()) {
            context.getWarningCollector().add(new PrestoWarning(
                    MATERIALIZED_VIEW_STITCHING_FALLBACK,
                    "Cannot perform incremental refresh for materialized view " + qualifiedViewName +
                            ": no partition-level staleness available (unpartitioned base, untracked partitions, or " +
                            "non-append base changes), and the refresh commit replaces the whole storage table without " +
                            "it. Falling back to full refresh."));
            return Result.ofPlanNode(node.getSource());
        }

        Map<SchemaTableName, MaterializedDataPredicates> constraints = status.getPartitionsFromBaseTables();

        Map<SchemaTableName, TupleDomain<String>> incrementalRefreshPredicates = constraints.entrySet().stream()
                .filter(entry -> !entry.getValue().getIncrementalRefreshPredicate().isAll())
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getIncrementalRefreshPredicate()));

        // The row-level analogue of incrementalRefreshPredicate: a refresh-only cap on the change
        // sequence number, so a bounded refresh advances the watermark only as far as it recomputed.
        // Like its partition-level counterpart this narrows the refresh and must never be applied to
        // a stale read, where dropping rows beyond the cap would lose data the query has to return.
        //
        // Only when row-level is actually enabled. The cap belongs to the row-level change set, and
        // the partition-level path already has incrementalRefreshPredicate for the same purpose.
        // Applying both put two caps on one scan -- on a version 3 base they are different columns
        // bounding the same snapshot, so the extra one was redundant rather than wrong, but it
        // changed the plan of a bounded refresh that had not asked for row-level at all.
        Map<SchemaTableName, TupleDomain<ColumnHandle>> refreshBounds =
                RowLevelRefreshEnablement.isEnabled(session, materializedViewDefinition.get().getRowLevelIncrementalRefresh())
                        ? status.getChangedRowsPredicates().entrySet().stream()
                                .filter(entry -> !entry.getValue().getRefreshBound().isAll())
                                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getRefreshBound()))
                        : ImmutableMap.of();

        Map<SchemaTableName, List<TupleDomain<String>>> filteredConstraints =
                filterToValidRefreshColumns(materializedViewDefinition.get(), constraints, columnEquivalences, dataTable);

        if (filteredConstraints.isEmpty()) {
            context.getWarningCollector().add(new PrestoWarning(
                    MATERIALIZED_VIEW_STITCHING_FALLBACK,
                    "Cannot perform incremental refresh for materialized view " + qualifiedViewName +
                            ": stale columns are not valid refresh columns. Falling back to full refresh."));
            return Result.ofPlanNode(applyIncrementalRefreshPredicates(
                    node.getSource(), incrementalRefreshPredicates, refreshBounds, session, idAllocator, variableAllocator, metadata, context.getLookup()));
        }

        Optional<PlanNode> deltaPlan = buildDeltaPlanForRefresh(
                node,
                metadata,
                session,
                idAllocator,
                variableAllocator,
                filteredConstraints,
                rowLevelPredicates,
                columnEquivalences,
                context.getLookup(),
                context.getWarningCollector());

        if (!deltaPlan.isPresent()) {
            context.getWarningCollector().add(new PrestoWarning(
                    MATERIALIZED_VIEW_STITCHING_FALLBACK,
                    "Cannot perform incremental refresh for materialized view " + qualifiedViewName +
                            ": unsupported operation in view query. Falling back to full refresh."));
            return Result.ofPlanNode(applyIncrementalRefreshPredicates(
                    node.getSource(), incrementalRefreshPredicates, refreshBounds, session, idAllocator, variableAllocator, metadata, context.getLookup()));
        }

        PlanNode deltaWithPredicates = applyIncrementalRefreshPredicates(
                deltaPlan.get(), incrementalRefreshPredicates, refreshBounds, session, idAllocator, variableAllocator, metadata, context.getLookup());

        if (strategy == MaterializedViewRewriteStrategy.AUTOMATIC) {
            PlanNode fullWithPredicates = applyIncrementalRefreshPredicates(
                    node.getSource(), incrementalRefreshPredicates, refreshBounds, session, idAllocator, variableAllocator, metadata, context.getLookup());
            return Result.ofPlanNode(new MVRewriteCandidatesNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    fullWithPredicates,
                    ImmutableList.of(new MVRewriteCandidate(
                            deltaWithPredicates,
                            catalogName,
                            materializedViewName.getSchemaName(),
                            materializedViewName.getTableName(),
                            // The delta recomputes only changed groups exactly when the connector
                            // supplied changed-rows predicates and they survived the safety checks.
                            !rowLevelPredicates.isEmpty())),
                    node.getOutputVariables()));
        }

        return Result.ofPlanNode(deltaWithPredicates);
    }

    /**
     * Filters stale predicates to only include columns that are valid refresh columns.
     * Returns empty if any stale column cannot be mapped to a valid refresh column.
     *
     * @param materializedViewDefinition The materialized view definition containing validRefreshColumns
     * @param constraints Raw stale predicates by base table
     * @param columnEquivalences Column equivalences for mapping base to storage columns
     * @param dataTable The MV storage table name
     * @return Filtered constraints with valid refresh columns only, empty map if incremental refresh is not possible
     */
    private static Map<SchemaTableName, List<TupleDomain<String>>> filterToValidRefreshColumns(
            MaterializedViewDefinition materializedViewDefinition,
            Map<SchemaTableName, MaterializedDataPredicates> constraints,
            PassthroughColumnEquivalences columnEquivalences,
            SchemaTableName dataTable)
    {
        Optional<List<String>> validRefreshColumns = materializedViewDefinition.getValidRefreshColumns();

        if (!validRefreshColumns.isPresent() || validRefreshColumns.get().isEmpty()) {
            return ImmutableMap.of();
        }

        ImmutableSet<String> validRefreshColumnSet = ImmutableSet.copyOf(validRefreshColumns.get());

        // Filter each table's predicates to only include valid refresh columns
        Map<SchemaTableName, List<TupleDomain<String>>> filtered = constraints.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> filterPredicatesForTable(
                                entry.getValue().getPredicateDisjuncts(),
                                entry.getKey(),
                                columnEquivalences,
                                validRefreshColumnSet,
                                dataTable)));

        // If any table has no valid predicates after filtering, incremental refresh is not possible
        if (filtered.values().stream().anyMatch(List::isEmpty)) {
            return ImmutableMap.of();
        }

        return filtered;
    }

    private static List<TupleDomain<String>> filterPredicatesForTable(
            List<TupleDomain<String>> stalePredicates,
            SchemaTableName baseTable,
            PassthroughColumnEquivalences columnEquivalences,
            ImmutableSet<String> validRefreshColumnSet,
            SchemaTableName dataTable)
    {
        return stalePredicates.stream()
                .filter(predicate -> predicate.getDomains().isPresent())
                .map(predicate -> projectToValidRefreshColumns(
                        predicate, baseTable, columnEquivalences, validRefreshColumnSet, dataTable))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .distinct()
                .collect(toImmutableList());
    }

    private static Optional<TupleDomain<String>> projectToValidRefreshColumns(
            TupleDomain<String> predicate,
            SchemaTableName baseTable,
            PassthroughColumnEquivalences columnEquivalences,
            ImmutableSet<String> validRefreshColumnSet,
            SchemaTableName dataTable)
    {
        if (!predicate.getDomains().isPresent()) {
            return Optional.empty();
        }

        Map<String, Domain> projected = predicate.getDomains().get().entrySet().stream()
                .filter(entry -> columnEquivalences.getStorageColumnName(dataTable, baseTable, entry.getKey())
                        .filter(validRefreshColumnSet::contains)
                        .isPresent())
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        if (projected.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(TupleDomain.withColumnDomains(projected));
    }

    /**
     * Wraps each base {@link TableScanNode} with a {@link FilterNode}, rebuilding the scan to
     * expose any predicate column it doesn't already output and re-projecting back if so.
     */
    private static PlanNode applyIncrementalRefreshPredicates(
            PlanNode plan,
            Map<SchemaTableName, TupleDomain<String>> perBasePredicates,
            Map<SchemaTableName, TupleDomain<ColumnHandle>> perBaseRefreshBounds,
            Session session,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            Metadata metadata,
            Lookup lookup)
    {
        if (perBasePredicates.isEmpty() && perBaseRefreshBounds.isEmpty()) {
            return plan;
        }
        return SimplePlanRewriter.rewriteWith(
                new IncrementalRefreshPredicateRewriter(
                        perBasePredicates, perBaseRefreshBounds, session, idAllocator, variableAllocator, metadata, lookup),
                plan);
    }

    private static final class IncrementalRefreshPredicateRewriter
            extends SimplePlanRewriter<Void>
    {
        private final Map<SchemaTableName, TupleDomain<String>> perBasePredicates;
        private final Map<SchemaTableName, TupleDomain<ColumnHandle>> perBaseRefreshBounds;
        private final Session session;
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;
        private final Metadata metadata;
        private final Lookup lookup;
        private final RowExpressionDomainTranslator translator;

        IncrementalRefreshPredicateRewriter(
                Map<SchemaTableName, TupleDomain<String>> perBasePredicates,
                Map<SchemaTableName, TupleDomain<ColumnHandle>> perBaseRefreshBounds,
                Session session,
                PlanNodeIdAllocator idAllocator,
                VariableAllocator variableAllocator,
                Metadata metadata,
                Lookup lookup)
        {
            this.perBasePredicates = requireNonNull(perBasePredicates, "perBasePredicates is null");
            this.perBaseRefreshBounds = requireNonNull(perBaseRefreshBounds, "perBaseRefreshBounds is null");
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.translator = new RowExpressionDomainTranslator(metadata);
        }

        @Override
        public PlanNode visitGroupReference(GroupReference node, RewriteContext<Void> context)
        {
            // SimplePlanRewriter's default descent throws on GroupReference.getSources.
            return lookup.resolveGroup(node).findFirst().orElse(node).accept(this, context);
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            SchemaTableName tableName = metadata.getTableMetadata(session, node.getTable()).getTable();
            TupleDomain<String> byName = perBasePredicates.getOrDefault(tableName, TupleDomain.all());
            TupleDomain<ColumnHandle> byHandle = perBaseRefreshBounds.getOrDefault(tableName, TupleDomain.all());
            if (isUnconstrained(byName) && isUnconstrained(byHandle)) {
                return node;
            }

            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, node.getTable());

            // A column can be read into more than one variable; either binding evaluates the same,
            // so keep the first rather than letting an inverted map reject the duplicate.
            Map<ColumnHandle, VariableReferenceExpression> variableByColumnHandle = new HashMap<>();
            node.getAssignments().forEach((variable, handle) -> variableByColumnHandle.putIfAbsent(handle, variable));

            List<VariableReferenceExpression> outputs = new ArrayList<>(node.getOutputVariables());
            Map<VariableReferenceExpression, ColumnHandle> assignments = new HashMap<>(node.getAssignments());

            // Resolve every column both predicates constrain, reading it from the table when the
            // scan does not already produce it. The refresh bound in particular is expressed over a
            // change-tracking column the view query has no reason to project.
            Map<ColumnHandle, VariableReferenceExpression> resolved = new HashMap<>();
            List<ColumnHandle> required = new ArrayList<>();
            for (String columnName : byName.getDomains().orElse(ImmutableMap.of()).keySet()) {
                ColumnHandle columnHandle = columnHandles.get(columnName);
                if (columnHandle == null) {
                    // Connector doesn't expose this column; skip the bound and run unbounded.
                    return node;
                }
                required.add(columnHandle);
            }
            required.addAll(byHandle.getDomains().orElse(ImmutableMap.of()).keySet());

            for (ColumnHandle columnHandle : required) {
                if (resolved.containsKey(columnHandle)) {
                    continue;
                }
                VariableReferenceExpression existing = variableByColumnHandle.get(columnHandle);
                if (existing != null) {
                    resolved.put(columnHandle, existing);
                    continue;
                }
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, node.getTable(), columnHandle);
                VariableReferenceExpression newVar = variableAllocator.newVariable(columnMetadata.getName(), columnMetadata.getType());
                outputs.add(newVar);
                assignments.put(newVar, columnHandle);
                resolved.put(columnHandle, newVar);
            }

            ImmutableList.Builder<RowExpression> predicates = ImmutableList.builder();
            if (!isUnconstrained(byName)) {
                predicates.add(translator.toPredicate(byName.transform(column -> resolved.get(columnHandles.get(column)))));
            }
            if (!isUnconstrained(byHandle)) {
                predicates.add(translator.toPredicate(byHandle.transform(resolved::get)));
            }

            boolean addedColumns = outputs.size() != node.getOutputVariables().size();
            TableScanNode rewrittenScan = addedColumns
                    ? new TableScanNode(
                            node.getSourceLocation(),
                            idAllocator.getNextId(),
                            node.getTable(),
                            outputs,
                            assignments,
                            node.getTableConstraints(),
                            node.getCurrentConstraint(),
                            node.getEnforcedConstraint(),
                            node.getCteMaterializationInfo())
                    : node;

            FilterNode filterNode = new FilterNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    rewrittenScan,
                    and(predicates.build()));

            if (!addedColumns) {
                return filterNode;
            }
            return new ProjectNode(idAllocator.getNextId(), filterNode, identityAssignments(node.getOutputVariables()));
        }

        private static <T> boolean isUnconstrained(TupleDomain<T> domain)
        {
            return domain.isAll() || !domain.getDomains().isPresent();
        }
    }
}
