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
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getDistributedDynamicFilterCardinalityRatioThreshold;
import static com.facebook.presto.SystemSessionProperties.getDistributedDynamicFilterDiscreteValuesLimit;
import static com.facebook.presto.SystemSessionProperties.getDistributedDynamicFilterStrategy;
import static com.facebook.presto.SystemSessionProperties.isDistributedDynamicFilterEnabled;
import static com.facebook.presto.SystemSessionProperties.isDistributedDynamicFilterExtendedMetrics;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_CREATED_FAVORABLE_RATIO;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_CREATED_LOW_NDV;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_CREATED_PARTITION_FALLBACK;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_SKIPPED_BUILD_COVERS_PROBE;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_SKIPPED_HIGH_CARDINALITY;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_SKIPPED_NOT_PARTITION_COLUMN;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.DistributedDynamicFilterStrategy.COST_BASED;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static java.lang.Double.isFinite;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Populates JoinNode.dynamicFilters for distributed dynamic partition pruning.
 * When strategy is COST_BASED, filters are only created for clauses that meet
 * NDV or cardinality ratio criteria.
 */
public class AddDynamicFilterRule
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(node -> (node.getType() == JoinType.INNER || node.getType() == JoinType.RIGHT)
                    && node.getDynamicFilters().isEmpty()
                    && !node.getCriteria().isEmpty());

    private final Metadata metadata;

    public AddDynamicFilterRule(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isDistributedDynamicFilterEnabled(session);
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        boolean costBased = getDistributedDynamicFilterStrategy(context.getSession()) == COST_BASED;

        Map<String, VariableReferenceExpression> dynamicFilters = new LinkedHashMap<>();
        for (EquiJoinClause clause : node.getCriteria()) {
            if (costBased && !shouldCreateFilter(clause, node, context)) {
                continue;
            }
            String filterId = context.getIdAllocator().getNextId().toString();
            dynamicFilters.put(filterId, clause.getRight());
        }

        if (dynamicFilters.isEmpty()) {
            return Result.empty();
        }

        return Result.ofPlanNode(new JoinNode(
                node.getSourceLocation(),
                node.getId(),
                node.getStatsEquivalentPlanNode(),
                node.getType(),
                node.getLeft(),
                node.getRight(),
                node.getCriteria(),
                node.getOutputVariables(),
                node.getFilter(),
                node.getLeftHashVariable(),
                node.getRightHashVariable(),
                node.getDistributionType(),
                dynamicFilters));
    }

    boolean shouldCreateFilter(EquiJoinClause clause, JoinNode node, Context context)
    {
        Session session = context.getSession();
        boolean extendedMetrics = isDistributedDynamicFilterExtendedMetrics(session);
        String columnName = clause.getLeft().getName();
        double cardinalityRatioThreshold = getDistributedDynamicFilterCardinalityRatioThreshold(session);
        long discreteValuesLimit = getDistributedDynamicFilterDiscreteValuesLimit(session);

        PlanNodeStatsEstimate buildStats = context.getStatsProvider().getStats(node.getRight());
        PlanNodeStatsEstimate probeStats = context.getStatsProvider().getStats(node.getLeft());

        double buildRowCount = buildStats.getOutputRowCount();
        double probeRowCount = probeStats.getOutputRowCount();

        if (!isFinite(buildRowCount) || !isFinite(probeRowCount)) {
            boolean create = isSplitFilteringColumn(clause.getLeft(), node.getLeft(), context);
            if (extendedMetrics) {
                emitPlanDecisionMetric(session, create
                        ? DYNAMIC_FILTER_PLAN_CREATED_PARTITION_FALLBACK
                        : DYNAMIC_FILTER_PLAN_SKIPPED_NOT_PARTITION_COLUMN, columnName);
            }
            return create;
        }

        VariableStatsEstimate buildVarStats = buildStats.getVariableStatistics(clause.getRight());
        double buildNdv = buildVarStats.getDistinctValuesCount();

        // If the build-side domain fully covers the probe-side domain,
        // the filter cannot prune anything
        VariableStatsEstimate probeVarStats = probeStats.getVariableStatistics(clause.getLeft());
        double buildLow = buildVarStats.getLowValue();
        double buildHigh = buildVarStats.getHighValue();
        double probeLow = probeVarStats.getLowValue();
        double probeHigh = probeVarStats.getHighValue();
        double probeNdv = probeVarStats.getDistinctValuesCount();
        if (isFinite(buildLow) && isFinite(buildHigh) && isFinite(buildNdv)
                && isFinite(probeLow) && isFinite(probeHigh) && isFinite(probeNdv)
                && buildLow <= probeLow && buildHigh >= probeHigh && buildNdv >= probeNdv) {
            if (extendedMetrics) {
                emitPlanDecisionMetric(session, DYNAMIC_FILTER_PLAN_SKIPPED_BUILD_COVERS_PROBE, columnName);
            }
            return false;
        }

        if (isFinite(buildNdv) && buildNdv <= discreteValuesLimit) {
            if (extendedMetrics) {
                emitPlanDecisionMetric(session, DYNAMIC_FILTER_PLAN_CREATED_LOW_NDV, columnName);
            }
            return true;
        }

        boolean create = probeRowCount > 0 && (buildRowCount / probeRowCount) < cardinalityRatioThreshold;
        if (extendedMetrics) {
            emitPlanDecisionMetric(session, create
                    ? DYNAMIC_FILTER_PLAN_CREATED_FAVORABLE_RATIO
                    : DYNAMIC_FILTER_PLAN_SKIPPED_HIGH_CARDINALITY, columnName);
        }
        return create;
    }

    private static void emitPlanDecisionMetric(Session session, String metricName, String columnName)
    {
        session.getRuntimeStats().addMetricValue(format("%s[%s]", metricName, columnName), NONE, 1);
    }

    private boolean isSplitFilteringColumn(VariableReferenceExpression variable, PlanNode probeNode, Context context)
    {
        Optional<ProbeColumnMapping> mapping = resolveProbeColumnMapping(variable, probeNode, context.getLookup());
        if (!mapping.isPresent()) {
            return false;
        }

        TableScanNode tableScan = mapping.get().getTableScanNode();
        ColumnHandle columnHandle = mapping.get().getColumnHandle();

        if (!tableScan.getTable().getLayout().isPresent()) {
            return false;
        }

        TableLayout layout = metadata.getLayout(context.getSession(), tableScan.getTable());
        return layout.getDiscretePredicates()
                .map(dp -> dp.getColumns().contains(columnHandle))
                .orElse(false);
    }

    static Optional<ProbeColumnMapping> resolveProbeColumnMapping(
            VariableReferenceExpression variable,
            PlanNode node,
            Lookup lookup)
    {
        PlanNode resolved = lookup.resolve(node);

        if (resolved instanceof TableScanNode) {
            TableScanNode tableScan = (TableScanNode) resolved;
            ColumnHandle columnHandle = tableScan.getAssignments().get(variable);
            if (columnHandle != null) {
                return Optional.of(new ProbeColumnMapping(tableScan, columnHandle));
            }
            return Optional.empty();
        }

        if (resolved instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) resolved;
            Assignments assignments = project.getAssignments();
            RowExpression expression = assignments.getMap().get(variable);
            if (expression instanceof VariableReferenceExpression) {
                return resolveProbeColumnMapping((VariableReferenceExpression) expression, project.getSource(), lookup);
            }
            return Optional.empty();
        }

        if (resolved.getSources().size() == 1) {
            return resolveProbeColumnMapping(variable, resolved.getSources().get(0), lookup);
        }

        return Optional.empty();
    }

    static class ProbeColumnMapping
    {
        private final TableScanNode tableScanNode;
        private final ColumnHandle columnHandle;

        ProbeColumnMapping(TableScanNode tableScanNode, ColumnHandle columnHandle)
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
}
