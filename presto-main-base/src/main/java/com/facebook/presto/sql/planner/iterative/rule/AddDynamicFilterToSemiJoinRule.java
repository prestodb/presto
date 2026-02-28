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

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getDistributedDynamicFilterCardinalityRatioThreshold;
import static com.facebook.presto.SystemSessionProperties.getDistributedDynamicFilterDiscreteValuesLimit;
import static com.facebook.presto.SystemSessionProperties.getDistributedDynamicFilterMinProbeSize;
import static com.facebook.presto.SystemSessionProperties.getDistributedDynamicFilterStrategy;
import static com.facebook.presto.SystemSessionProperties.getJoinMaxBroadcastTableSize;
import static com.facebook.presto.SystemSessionProperties.isDistributedDynamicFilterEnabled;
import static com.facebook.presto.SystemSessionProperties.isDistributedDynamicFilterExtendedMetrics;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_CREATED_FAVORABLE_RATIO;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_CREATED_LOW_NDV;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_CREATED_PARTITION_FALLBACK;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_SKIPPED_BROADCAST_JOIN;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_SKIPPED_BUILD_COVERS_PROBE;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_SKIPPED_HIGH_CARDINALITY;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_SKIPPED_NOT_PARTITION_COLUMN;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_SKIPPED_SMALL_PROBE;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.DistributedDynamicFilterStrategy.COST_BASED;
import static com.facebook.presto.sql.planner.iterative.rule.AddDynamicFilterRule.resolveProbeColumnMapping;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;
import static java.lang.Double.isFinite;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Populates SemiJoinNode.dynamicFilters for distributed dynamic partition pruning.
 * When strategy is COST_BASED, evaluates the clause against the same criteria
 * as {@link AddDynamicFilterRule}.
 */
public class AddDynamicFilterToSemiJoinRule
        implements Rule<SemiJoinNode>
{
    private static final Pattern<SemiJoinNode> PATTERN = semiJoin()
            .matching(node -> node.getDynamicFilters().isEmpty());

    private final Metadata metadata;
    private final TaskCountEstimator taskCountEstimator;

    public AddDynamicFilterToSemiJoinRule(Metadata metadata, TaskCountEstimator taskCountEstimator)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isDistributedDynamicFilterEnabled(session);
    }

    @Override
    public Result apply(SemiJoinNode node, Captures captures, Context context)
    {
        boolean costBased = getDistributedDynamicFilterStrategy(context.getSession()) == COST_BASED;

        if (costBased && !shouldCreateFilter(node, context)) {
            return Result.empty();
        }

        String filterId = context.getIdAllocator().getNextId().toString();
        Map<String, VariableReferenceExpression> dynamicFilters = ImmutableMap.of(
                filterId, node.getFilteringSourceJoinVariable());

        return Result.ofPlanNode(new SemiJoinNode(
                node.getSourceLocation(),
                node.getId(),
                node.getStatsEquivalentPlanNode(),
                node.getSource(),
                node.getFilteringSource(),
                node.getSourceJoinVariable(),
                node.getFilteringSourceJoinVariable(),
                node.getSemiJoinOutput(),
                node.getSourceHashVariable(),
                node.getFilteringSourceHashVariable(),
                node.getDistributionType(),
                dynamicFilters));
    }

    private boolean shouldCreateFilter(SemiJoinNode node, Context context)
    {
        Session session = context.getSession();
        boolean extendedMetrics = isDistributedDynamicFilterExtendedMetrics(session);
        String columnName = node.getSourceJoinVariable().getName();
        double cardinalityRatioThreshold = getDistributedDynamicFilterCardinalityRatioThreshold(session);
        long discreteValuesLimit = getDistributedDynamicFilterDiscreteValuesLimit(session);

        PlanNodeStatsEstimate buildStats = context.getStatsProvider().getStats(node.getFilteringSource());
        PlanNodeStatsEstimate probeStats = context.getStatsProvider().getStats(node.getSource());

        double buildRowCount = buildStats.getOutputRowCount();
        double probeRowCount = probeStats.getOutputRowCount();

        if (!isFinite(buildRowCount) || !isFinite(probeRowCount)) {
            boolean create = isSplitFilteringColumn(node.getSourceJoinVariable(), node.getSource(), context);
            if (extendedMetrics) {
                emitPlanDecisionMetric(session, create
                        ? DYNAMIC_FILTER_PLAN_CREATED_PARTITION_FALLBACK
                        : DYNAMIC_FILTER_PLAN_SKIPPED_NOT_PARTITION_COLUMN, columnName);
            }
            return create;
        }

        // Skip DPP for joins likely to be broadcast — Velox handles same-fragment filtering
        double buildSizeBytes = buildStats.getOutputSizeInBytes(node.getFilteringSource());
        DataSize broadcastThreshold = getJoinMaxBroadcastTableSize(session);
        if (isFinite(buildSizeBytes) && buildSizeBytes <= broadcastThreshold.toBytes()) {
            if (extendedMetrics) {
                emitPlanDecisionMetric(session, DYNAMIC_FILTER_PLAN_SKIPPED_BROADCAST_JOIN, columnName);
            }
            return false;
        }

        // Skip DPP for small probe-side scans — overhead exceeds possible savings
        // Threshold is per-node: divide total probe size by cluster size
        double probeSizeBytes = probeStats.getOutputSizeInBytes(node.getSource());
        DataSize minProbeSize = getDistributedDynamicFilterMinProbeSize(session);
        int taskCount = Math.max(1, taskCountEstimator.estimateSourceDistributedTaskCount());
        double perNodeProbeSizeBytes = probeSizeBytes / taskCount;
        if (isFinite(perNodeProbeSizeBytes) && perNodeProbeSizeBytes <= minProbeSize.toBytes()) {
            if (extendedMetrics) {
                emitPlanDecisionMetric(session, DYNAMIC_FILTER_PLAN_SKIPPED_SMALL_PROBE, columnName);
            }
            return false;
        }

        VariableStatsEstimate buildVarStats = buildStats.getVariableStatistics(node.getFilteringSourceJoinVariable());
        double buildNdv = buildVarStats.getDistinctValuesCount();

        // If the build-side domain fully covers the probe-side domain,
        // the filter cannot prune anything
        VariableStatsEstimate probeVarStats = probeStats.getVariableStatistics(node.getSourceJoinVariable());
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
        Optional<AddDynamicFilterRule.ProbeColumnMapping> mapping = resolveProbeColumnMapping(variable, probeNode, context.getLookup());
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
}
