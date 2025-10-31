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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.MaterializedViewScanNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;

import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.isLegacyMaterializedViews;
import static com.facebook.presto.SystemSessionProperties.isMaterializedViewDataConsistencyEnabled;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.planner.plan.Patterns.materializedViewScan;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MaterializedViewRewrite
        implements Rule<MaterializedViewScanNode>
{
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
        checkState(!isLegacyMaterializedViews(session), "Materialized view rewrite rule should not fire when legacy materialized views are enabled");
        checkState(isMaterializedViewDataConsistencyEnabled(session), "Materialized view rewrite rule should not fire when materialized view data consistency is disabled");

        MetadataResolver metadataResolver = metadata.getMetadataResolver(session);

        MaterializedViewStatus status = metadataResolver.getMaterializedViewStatus(node.getMaterializedViewName());

        boolean useDataTable = status.isFullyMaterialized();
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
}
