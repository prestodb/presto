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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewStaleReadBehavior;
import com.facebook.presto.spi.MaterializedViewStalenessConfig;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.MaterializedViewScanNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.ViewExpression;
import com.facebook.presto.spi.security.ViewSecurity;
import com.facebook.presto.sql.planner.iterative.Rule;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getMaterializedViewStaleReadBehavior;
import static com.facebook.presto.SystemSessionProperties.getMaterializedViewStalenessWindow;
import static com.facebook.presto.SystemSessionProperties.isLegacyMaterializedViews;
import static com.facebook.presto.SystemSessionProperties.isMaterializedViewForceStale;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import static com.facebook.presto.spi.StandardErrorCode.MATERIALIZED_VIEW_STALE;
import static com.facebook.presto.spi.StandardWarningCode.MATERIALIZED_VIEW_ACCESS_CONTROL_FALLBACK;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.security.ViewSecurity.DEFINER;
import static com.facebook.presto.spi.security.ViewSecurity.INVOKER;
import static com.facebook.presto.sql.planner.iterative.rule.materializedview.DifferentialPlanRewriter.buildStitchedPlan;
import static com.facebook.presto.sql.planner.plan.Patterns.materializedViewScan;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;

/**
 * Rewrites {@link MaterializedViewScanNode} to use pre-computed data when possible.
 *
 * <p>Controlled by the {@code materialized_view_data_consistency} session property:
 * <ul>
 *   <li>{@code FAIL}: Fail the query if stale.</li>
 *   <li>{@code USE_STITCHING}: If fully fresh, use data table; if partially stale, build
 *       a stitched plan combining fresh data with recomputed stale data; otherwise fall back
 *       to full recompute.</li>
 *   <li>{@code USE_VIEW_QUERY}: Always execute the original view query (full recompute).</li>
 * </ul>
 *
 * <p>For INVOKER security mode views, the data table cannot be used if row filters or column
 * masks exist on base tables, since these depend on the invoking user's identity.
 *
 * @see DifferentialPlanRewriter
 */
public class MaterializedViewRewrite
        implements Rule<MaterializedViewScanNode>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    public MaterializedViewRewrite(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
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

        Optional<MaterializedViewDefinition> materializedViewDefinition = metadataResolver.getMaterializedView(node.getMaterializedViewName());
        checkState(materializedViewDefinition.isPresent(), "Materialized view definition not found for: %s", node.getMaterializedViewName());
        MaterializedViewDefinition definition = materializedViewDefinition.get();

        MaterializedViewStatus status = metadataResolver.getMaterializedViewStatus(node.getMaterializedViewName(), TupleDomain.all());

        MaterializedViewStaleReadBehavior staleReadBehavior = definition.getStalenessConfig()
                .map(MaterializedViewStalenessConfig::getStaleReadBehavior)
                .orElseGet(() -> getMaterializedViewStaleReadBehavior(session));

        Duration stalenessWindow = definition.getStalenessConfig()
                .map(MaterializedViewStalenessConfig::getStalenessWindow)
                .orElseGet(() -> getMaterializedViewStalenessWindow(session).orElse(Duration.valueOf("0s")));

        boolean canUseDataTable = canUseDataTable(session, context, node, metadataResolver, definition, status, staleReadBehavior, stalenessWindow);
        boolean shouldStitch = shouldPerformStitching(status, staleReadBehavior, stalenessWindow);
        if (!status.isFullyMaterialized() && !status.getPartitionsFromBaseTables().isEmpty()) {
            Map<SchemaTableName, MaterializedDataPredicates> constraints = status.getPartitionsFromBaseTables();

            if (shouldStitch) {
                Optional<PlanNode> unionPlan = buildStitchedPlan(
                        metadata,
                        session,
                        node,
                        constraints,
                        definition,
                        variableAllocator,
                        idAllocator,
                        context.getLookup(),
                        context.getWarningCollector());

                if (unionPlan.isPresent()) {
                    return Result.ofPlanNode(unionPlan.get());
                }
            }
        }

        PlanNode plan;
        Map<VariableReferenceExpression, VariableReferenceExpression> mappings;
        if (canUseDataTable && (status.isFullyMaterialized() || !shouldStitch)) {
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
            MetadataResolver metadataResolver,
            MaterializedViewDefinition definition,
            MaterializedViewStatus status,
            MaterializedViewStaleReadBehavior staleReadBehavior,
            Duration stalenessWindow)
    {
        if (isMaterializedViewForceStale(session)) {
            return shouldUseDataTableWhenStale(staleReadBehavior, node.getMaterializedViewName());
        }

        if (status.isFullyMaterialized() || isWithinStalenessWindow(status, stalenessWindow)) {
            return canUseDataTableWithSecurityChecks(node, metadataResolver, session, definition, context);
        }
        return shouldUseDataTableWhenStale(staleReadBehavior, node.getMaterializedViewName());
    }

    private boolean isWithinStalenessWindow(MaterializedViewStatus status, Duration stalenessWindow)
    {
        return status.getLastFreshTime()
                .map(time -> (currentTimeMillis() - time) <= stalenessWindow.toMillis())
                .orElse(false);
    }

    private boolean shouldPerformStitching(
            MaterializedViewStatus status,
            MaterializedViewStaleReadBehavior staleReadBehavior,
            Duration stalenessWindow)
    {
        if (status.isFullyMaterialized()) {
            return false;
        }

        // If within staleness window, just return stale data - don't do stitching
        if (isWithinStalenessWindow(status, stalenessWindow)) {
            return false;
        }

        return staleReadBehavior == MaterializedViewStaleReadBehavior.USE_STITCHING;
    }

    private boolean shouldUseDataTableWhenStale(MaterializedViewStaleReadBehavior behavior, QualifiedObjectName viewName)
    {
        switch (behavior) {
            case FAIL:
                throw new PrestoException(
                        MATERIALIZED_VIEW_STALE,
                        String.format("Materialized view '%s' is stale", viewName));
            case USE_VIEW_QUERY:
                return false;
            case USE_STITCHING:
                return true;
            default:
                throw new IllegalStateException("Unexpected stale read behavior: " + behavior);
        }
    }

    private boolean canUseDataTableWithSecurityChecks(
            MaterializedViewScanNode node,
            MetadataResolver metadataResolver,
            Session session,
            MaterializedViewDefinition definition,
            Context context)
    {
        // Security mode defaults to INVOKER for legacy materialized views created without explicitly specifying it
        ViewSecurity securityMode = definition.getSecurityMode().orElse(INVOKER);

        // In definer rights, there's only one user permissions (the definer), so row filters and column masks
        // do not depend on the invoker and can be safely ignored when deciding whether to use the data table
        if (securityMode == DEFINER) {
            return true;
        }

        String catalogName = node.getMaterializedViewName().getCatalogName();
        for (SchemaTableName schemaTableName : definition.getBaseTables()) {
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
}
