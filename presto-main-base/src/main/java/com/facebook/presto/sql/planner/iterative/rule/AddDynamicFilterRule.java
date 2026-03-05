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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.getDistributedDynamicFilterCardinalityRatioThreshold;
import static com.facebook.presto.SystemSessionProperties.getDistributedDynamicFilterStrategy;
import static com.facebook.presto.SystemSessionProperties.isDistributedDynamicFilterEnabled;
import static com.facebook.presto.SystemSessionProperties.isDistributedDynamicFilterExtendedMetrics;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_CREATED_FAVORABLE_RATIO;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_SKIPPED_HIGH_CARDINALITY;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.DistributedDynamicFilterStrategy.COST_BASED;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static java.lang.Double.isFinite;
import static java.lang.String.format;

/**
 * Populates JoinNode.dynamicFilters for distributed dynamic partition pruning.
 * When strategy is COST_BASED, filters are created for clauses where the
 * build/probe cardinality ratio is below the configured threshold.
 */
public class AddDynamicFilterRule
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(node -> (node.getType() == JoinType.INNER || node.getType() == JoinType.RIGHT)
                    && node.getDynamicFilters().isEmpty()
                    && !node.getCriteria().isEmpty());

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

        PlanNodeStatsEstimate buildStats = context.getStatsProvider().getStats(node.getRight());
        PlanNodeStatsEstimate probeStats = context.getStatsProvider().getStats(node.getLeft());
        double buildRowCount = buildStats.getOutputRowCount();
        double probeRowCount = probeStats.getOutputRowCount();

        // If stats are unavailable, create the filter — the connector decides usefulness
        if (!isFinite(buildRowCount) || !isFinite(probeRowCount)) {
            return true;
        }

        double threshold = getDistributedDynamicFilterCardinalityRatioThreshold(session);
        boolean create = probeRowCount > 0 && (buildRowCount / probeRowCount) < threshold;
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
}
