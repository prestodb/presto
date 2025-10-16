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

import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.MaterializedViewScanNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import java.util.Map;

import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static java.util.Objects.requireNonNull;

/**
 * Simple MaterializedViewOptimizer that makes a binary decision:
 * - If the materialized view is fully fresh, return the data table plan
 * - If the materialized view is stale, return the view query plan (recompute everything)
 *
 * NO UNION stitching - this is a simple binary choice only!
 */
public class MaterializedViewOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;

    public MaterializedViewOptimizer(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanOptimizerResult optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        MetadataResolver metadataResolver = metadata.getMetadataResolver(session);
        Rewriter rewriter = new Rewriter(metadataResolver, idAllocator);
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan);
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final MetadataResolver metadataResolver;
        private final PlanNodeIdAllocator idAllocator;
        private boolean planChanged;

        public Rewriter(MetadataResolver metadataResolver, PlanNodeIdAllocator idAllocator)
        {
            this.metadataResolver = requireNonNull(metadataResolver, "metadataResolver is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitMaterializedViewScan(MaterializedViewScanNode node, RewriteContext<Void> context)
        {
            // First, let the default rewriter optimize both child plans
            // This ensures that dataTablePlan and viewQueryPlan are fully optimized
            MaterializedViewScanNode rewrittenNode = (MaterializedViewScanNode) context.defaultRewrite(node);

            // Now make the freshness-based decision on the optimized children
            MaterializedViewStatus status = metadataResolver.getMaterializedViewStatus(
                    rewrittenNode.getMaterializedViewName(),
                    TupleDomain.all());

            planChanged = true;
            boolean useFreshData = status.isFullyMaterialized();

            // Choose the optimized plan based on freshness
            PlanNode chosenPlan = useFreshData ? rewrittenNode.getDataTablePlan() : rewrittenNode.getViewQueryPlan();
            Map<VariableReferenceExpression, VariableReferenceExpression> chosenMappings =
                    useFreshData ? rewrittenNode.getDataTableMappings() : rewrittenNode.getViewQueryMappings();

            requireNonNull(chosenMappings, "chosenMappings is null (useFreshData=" + useFreshData +
                    ", dataTableMappings=" + rewrittenNode.getDataTableMappings() +
                    ", viewQueryMappings=" + rewrittenNode.getViewQueryMappings() + ")");

            // Build assignments to remap variables
            Assignments.Builder assignments = Assignments.builder();
            for (VariableReferenceExpression outputVariable : rewrittenNode.getOutputVariables()) {
                VariableReferenceExpression sourceVariable = chosenMappings.get(outputVariable);
                if (sourceVariable != null) {
                    assignments.put(outputVariable, sourceVariable);
                }
                else {
                    // If no mapping exists, use identity (shouldn't happen in well-formed plans)
                    assignments.put(outputVariable, outputVariable);
                }
            }

            return new ProjectNode(
                    rewrittenNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    chosenPlan,
                    assignments.build(),
                    LOCAL);
        }
    }
}
