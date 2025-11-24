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
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.MaterializedViewScanNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.ViewExpression;
import com.facebook.presto.spi.security.ViewSecurity;
import com.facebook.presto.sql.planner.iterative.Rule;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isLegacyMaterializedViews;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.security.ViewSecurity.DEFINER;
import static com.facebook.presto.spi.security.ViewSecurity.INVOKER;
import static com.facebook.presto.sql.planner.plan.Patterns.materializedViewScan;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

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

        MetadataResolver metadataResolver = metadata.getMetadataResolver(session);

        boolean useDataTable = isUseDataTable(node, metadataResolver, session);
        PlanNode chosenPlan = useDataTable ? node.getDataTablePlan() : node.getViewQueryPlan();
        Map<VariableReferenceExpression, VariableReferenceExpression> chosenMappings =
                useDataTable ? node.getDataTableMappings() : node.getViewQueryMappings();

        Assignments.Builder assignments = Assignments.builder();
        for (VariableReferenceExpression outputVariable : node.getOutputVariables()) {
            VariableReferenceExpression sourceVariable = chosenMappings.get(outputVariable);
            requireNonNull(sourceVariable, "No mapping found for output variable: " + outputVariable);
            assignments.put(outputVariable, sourceVariable);
        }

        return Result.ofPlanNode(new ProjectNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                chosenPlan,
                assignments.build(),
                LOCAL));
    }

    private boolean isUseDataTable(MaterializedViewScanNode node, MetadataResolver metadataResolver, Session session)
    {
        Optional<MaterializedViewDefinition> materializedViewDefinition = metadataResolver.getMaterializedView(node.getMaterializedViewName());
        checkState(materializedViewDefinition.isPresent(), "Materialized view definition not found for: %s", node.getMaterializedViewName());
        // Security mode defaults to INVOKER for legacy materialized views created without explicitly specifying it
        ViewSecurity securityMode = materializedViewDefinition.get().getSecurityMode().orElse(INVOKER);
        MaterializedViewStatus status = metadataResolver.getMaterializedViewStatus(node.getMaterializedViewName(), TupleDomain.all());

        if (!status.isFullyMaterialized()) {
            return false;
        }

        // In definer rights, there's only one user permissions (the definer), so row filters and column masks
        // do not depend on the invoker and can be safely ignored when deciding whether to use the data table
        if (securityMode == DEFINER) {
            return true;
        }

        // Invoker rights: need to check for row filters and column masks on base tables because they may alter
        // the data returned by the materialized view depending on the invoker's permissions.
        String catalogName = node.getMaterializedViewName().getCatalogName();
        for (SchemaTableName schemaTableName : materializedViewDefinition.get().getBaseTables()) {
            QualifiedObjectName baseTable = new QualifiedObjectName(catalogName, schemaTableName.getSchemaName(), schemaTableName.getTableName());

            // Check for row filters on this base table
            List<ViewExpression> rowFilters = accessControl.getRowFilters(
                    session.getTransactionId().get(),
                    session.getIdentity(),
                    session.getAccessControlContext(),
                    baseTable);

            if (!rowFilters.isEmpty()) {
                return false;
            }

            Optional<TableHandle> tableHandle = metadataResolver.getTableHandle(baseTable);
            if (!tableHandle.isPresent()) {
                return false;
            }

            // Check for column masks on this base table
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
                return false;
            }
        }

        // No row filters or column masks found on base tables, safe to use data table
        return true;
    }
}
