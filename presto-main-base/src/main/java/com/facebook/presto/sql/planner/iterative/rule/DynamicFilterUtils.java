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
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getDistributedDynamicFilterCardinalityRatioThreshold;
import static com.facebook.presto.SystemSessionProperties.getDistributedDynamicFilterStrategy;
import static com.facebook.presto.SystemSessionProperties.isDistributedDynamicFilterEnabled;
import static com.facebook.presto.SystemSessionProperties.isDistributedDynamicFilterOnReplicatedJoins;
import static com.facebook.presto.SystemSessionProperties.isVerboseRuntimeStatsEnabled;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_CREATED_FAVORABLE_RATIO;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_SKIPPED_HIGH_CARDINALITY;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.DistributedDynamicFilterStrategy.COST_BASED;
import static com.google.common.base.Verify.verify;
import static java.lang.Double.isFinite;
import static java.lang.String.format;

public final class DynamicFilterUtils
{
    private DynamicFilterUtils() {}

    /**
     * If distributed DPP is enabled, adds a dynamic filter entry per equi-join
     * clause to the given JoinNode. Returns the original node unchanged when DPP
     * is disabled, the join type is unsupported, the join is REPLICATED and
     * {@code distributed_dynamic_filter_on_replicated_joins} is false, or
     * cost-based gating rejects all clauses.
     */
    public static JoinNode addApplicableDynamicFilters(Session session, JoinNode node, StatsProvider statsProvider, PlanNodeIdAllocator idAllocator)
    {
        if (!isDistributedDynamicFilterEnabled(session)) {
            return node;
        }
        if (node.getType() != INNER && node.getType() != RIGHT) {
            return node;
        }
        verify(node.getDynamicFilters().isEmpty(), "JoinNode already has dynamic filters");
        if (node.getCriteria().isEmpty()) {
            return node;
        }
        // Velox handles REPLICATED joins in-fragment; coordinator DPP only applies to PARTITIONED joins.
        if (!isDistributedDynamicFilterOnReplicatedJoins(session) && node.getDistributionType().equals(Optional.of(REPLICATED))) {
            return node;
        }

        boolean costBased = getDistributedDynamicFilterStrategy(session) == COST_BASED;
        boolean extendedMetrics = isVerboseRuntimeStatsEnabled(session);
        PlanNodeStatsEstimate buildStats = statsProvider.getStats(node.getRight());
        PlanNodeStatsEstimate probeStats = statsProvider.getStats(node.getLeft());
        Map<String, VariableReferenceExpression> dynamicFilters = new HashMap<>();
        for (EquiJoinClause clause : node.getCriteria()) {
            if (costBased && !shouldCreateFilter(clause.getLeft().getName(), buildStats, probeStats, session, extendedMetrics)) {
                continue;
            }
            String filterId = idAllocator.getNextId().toString();
            dynamicFilters.put(filterId, clause.getRight());
        }

        if (dynamicFilters.isEmpty()) {
            return node;
        }

        return new JoinNode(
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
                dynamicFilters);
    }

    static boolean shouldCreateFilter(String columnName, PlanNodeStatsEstimate buildStats, PlanNodeStatsEstimate probeStats, Session session, boolean extendedMetrics)
    {
        double buildRowCount = buildStats.getOutputRowCount();
        double probeRowCount = probeStats.getOutputRowCount();

        if (!isFinite(buildRowCount) || !isFinite(probeRowCount)) {
            return true;
        }

        double threshold = getDistributedDynamicFilterCardinalityRatioThreshold(session);
        // Zero probe rows means the build can be fully pruned — the filter is maximally selective.
        boolean create = probeRowCount == 0 || (buildRowCount / probeRowCount) < threshold;
        if (extendedMetrics) {
            String metricName = create ? DYNAMIC_FILTER_PLAN_CREATED_FAVORABLE_RATIO : DYNAMIC_FILTER_PLAN_SKIPPED_HIGH_CARDINALITY;
            session.getRuntimeStats().addMetricValue(
                    format("%s[%s]", metricName, columnName),
                    NONE, 1);
        }
        return create;
    }
}
