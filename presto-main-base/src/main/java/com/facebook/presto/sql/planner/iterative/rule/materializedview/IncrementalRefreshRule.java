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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewRefreshType;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.RefreshMaterializedViewNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getMaterializedViewDefaultRefreshType;
import static com.facebook.presto.SystemSessionProperties.isLegacyMaterializedViews;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardWarningCode.MATERIALIZED_VIEW_STITCHING_FALLBACK;
import static com.facebook.presto.sql.planner.iterative.rule.materializedview.DifferentialPlanRewriter.buildDeltaPlanForRefresh;
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

        // If fully materialized, nothing to refresh - return empty result
        if (status.isFullyMaterialized()) {
            return Result.ofPlanNode(new ValuesNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    node.getOutputVariables(),
                    ImmutableList.of(),
                    Optional.empty()));
        }

        // If no partition info available (unpartitioned tables or connector doesn't track partitions),
        // fall back to full refresh since we can't determine which partitions are stale
        if (status.getPartitionsFromBaseTables().isEmpty()) {
            return Result.ofPlanNode(node.getSource());
        }

        Map<SchemaTableName, MaterializedDataPredicates> constraints = status.getPartitionsFromBaseTables();

        SchemaTableName dataTable = new SchemaTableName(materializedViewDefinition.get().getSchema(), materializedViewDefinition.get().getTable());
        PassthroughColumnEquivalences columnEquivalences = new PassthroughColumnEquivalences(materializedViewDefinition.get(), dataTable);

        Optional<Map<SchemaTableName, List<TupleDomain<String>>>> filteredConstraints =
                filterToValidRefreshColumns(materializedViewDefinition.get(), constraints, columnEquivalences, dataTable);

        if (!filteredConstraints.isPresent()) {
            context.getWarningCollector().add(new PrestoWarning(
                    MATERIALIZED_VIEW_STITCHING_FALLBACK,
                    "Cannot perform incremental refresh for materialized view " + qualifiedViewName +
                            ": stale columns are not valid refresh columns. Falling back to full refresh."));
            return Result.ofPlanNode(node.getSource());
        }

        Optional<PlanNode> deltaPlan = buildDeltaPlanForRefresh(
                node,
                metadata,
                session,
                idAllocator,
                variableAllocator,
                filteredConstraints.get(),
                columnEquivalences,
                context.getLookup(),
                context.getWarningCollector());

        if (!deltaPlan.isPresent()) {
            context.getWarningCollector().add(new PrestoWarning(
                    MATERIALIZED_VIEW_STITCHING_FALLBACK,
                    "Cannot perform incremental refresh for materialized view " + qualifiedViewName +
                            ": unsupported operation in view query. Falling back to full refresh."));
            return Result.ofPlanNode(node.getSource());
        }

        return Result.ofPlanNode(deltaPlan.get());
    }

    /**
     * Filters stale predicates to only include columns that are valid refresh columns.
     * Returns empty if any stale column cannot be mapped to a valid refresh column.
     *
     * @param materializedViewDefinition The materialized view definition containing validRefreshColumns
     * @param constraints Raw stale predicates by base table
     * @param columnEquivalences Column equivalences for mapping base to storage columns
     * @param dataTable The MV storage table name
     * @return Filtered constraints if all stale columns are valid refresh columns, empty otherwise
     */
    private static Optional<Map<SchemaTableName, List<TupleDomain<String>>>> filterToValidRefreshColumns(
            MaterializedViewDefinition materializedViewDefinition,
            Map<SchemaTableName, MaterializedDataPredicates> constraints,
            PassthroughColumnEquivalences columnEquivalences,
            SchemaTableName dataTable)
    {
        Optional<List<String>> validRefreshColumns = materializedViewDefinition.getValidRefreshColumns();

        if (!validRefreshColumns.isPresent() || validRefreshColumns.get().isEmpty()) {
            return Optional.empty();
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
            return Optional.empty();
        }

        return Optional.of(filtered);
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
                .filter(predicate -> allColumnsAreValidRefreshColumns(
                        predicate, baseTable, columnEquivalences, validRefreshColumnSet, dataTable))
                .collect(toImmutableList());
    }

    private static boolean allColumnsAreValidRefreshColumns(
            TupleDomain<String> predicate,
            SchemaTableName baseTable,
            PassthroughColumnEquivalences columnEquivalences,
            ImmutableSet<String> validRefreshColumnSet,
            SchemaTableName dataTable)
    {
        return predicate.getDomains()
                .map(domains -> domains.keySet().stream()
                        .allMatch(baseColumn -> columnEquivalences.getStorageColumnName(dataTable, baseTable, baseColumn)
                                .filter(validRefreshColumnSet::contains)
                                .isPresent()))
                .orElse(false);
    }
}
