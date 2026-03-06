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
import com.facebook.presto.cost.CachingStatsProvider;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableMap;

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
import static java.lang.Double.isFinite;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Populates JoinNode.dynamicFilters and SemiJoinNode.dynamicFilters for
 * distributed dynamic partition pruning.
 *
 * <p>This is implemented as a simple plan rewriter (single top-down pass)
 * rather than an IterativeOptimizer rule because it needs no memo and no
 * exploration loop. For COST_BASED mode, stats are computed once per node
 * via a {@link CachingStatsProvider} — no eviction cascades.
 */
public class AddDynamicFilters
        implements PlanOptimizer
{
    private final StatsCalculator statsCalculator;

    public AddDynamicFilters(StatsCalculator statsCalculator)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
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
        if (!isDistributedDynamicFilterEnabled(session)) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }

        boolean costBased = getDistributedDynamicFilterStrategy(session) == COST_BASED;
        StatsProvider statsProvider = costBased
                ? new CachingStatsProvider(statsCalculator, session, types)
                : null;
        Rewriter rewriter = new Rewriter(session, idAllocator, costBased, statsProvider);
        PlanNode rewritten = SimplePlanRewriter.rewriteWith(rewriter, plan);
        return PlanOptimizerResult.optimizerResult(rewritten, rewriter.isPlanChanged());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;
        private final PlanNodeIdAllocator idAllocator;
        private final boolean costBased;
        private final boolean extendedMetrics;
        private final StatsProvider statsProvider;
        private boolean planChanged;

        private Rewriter(Session session, PlanNodeIdAllocator idAllocator, boolean costBased, StatsProvider statsProvider)
        {
            this.session = session;
            this.idAllocator = idAllocator;
            this.costBased = costBased;
            this.extendedMetrics = isDistributedDynamicFilterExtendedMetrics(session);
            this.statsProvider = statsProvider;
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode left = context.rewrite(node.getLeft());
            PlanNode right = context.rewrite(node.getRight());

            if ((node.getType() != JoinType.INNER && node.getType() != JoinType.RIGHT)
                    || !node.getDynamicFilters().isEmpty()
                    || node.getCriteria().isEmpty()) {
                return replaceChildren(node, left, right);
            }

            Map<String, VariableReferenceExpression> dynamicFilters = new LinkedHashMap<>();
            for (EquiJoinClause clause : node.getCriteria()) {
                if (costBased && !shouldCreateFilterForJoin(clause, node)) {
                    continue;
                }
                String filterId = idAllocator.getNextId().toString();
                dynamicFilters.put(filterId, clause.getRight());
            }

            if (dynamicFilters.isEmpty()) {
                return replaceChildren(node, left, right);
            }

            planChanged = true;
            return new JoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    node.getType(),
                    left,
                    right,
                    node.getCriteria(),
                    node.getOutputVariables(),
                    node.getFilter(),
                    node.getLeftHashVariable(),
                    node.getRightHashVariable(),
                    node.getDistributionType(),
                    dynamicFilters);
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            PlanNode filteringSource = context.rewrite(node.getFilteringSource());

            if (!node.getDynamicFilters().isEmpty()) {
                return replaceChildren(node, source, filteringSource);
            }

            if (costBased && !shouldCreateFilterForSemiJoin(node)) {
                return replaceChildren(node, source, filteringSource);
            }

            String filterId = idAllocator.getNextId().toString();
            Map<String, VariableReferenceExpression> dynamicFilters = ImmutableMap.of(
                    filterId, node.getFilteringSourceJoinVariable());

            planChanged = true;
            return new SemiJoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    source,
                    filteringSource,
                    node.getSourceJoinVariable(),
                    node.getFilteringSourceJoinVariable(),
                    node.getSemiJoinOutput(),
                    node.getSourceHashVariable(),
                    node.getFilteringSourceHashVariable(),
                    node.getDistributionType(),
                    dynamicFilters);
        }

        private boolean shouldCreateFilterForJoin(EquiJoinClause clause, JoinNode node)
        {
            String columnName = clause.getLeft().getName();
            PlanNodeStatsEstimate buildStats = statsProvider.getStats(node.getRight());
            PlanNodeStatsEstimate probeStats = statsProvider.getStats(node.getLeft());
            return shouldCreateFilter(columnName, buildStats, probeStats);
        }

        private boolean shouldCreateFilterForSemiJoin(SemiJoinNode node)
        {
            String columnName = node.getSourceJoinVariable().getName();
            PlanNodeStatsEstimate buildStats = statsProvider.getStats(node.getFilteringSource());
            PlanNodeStatsEstimate probeStats = statsProvider.getStats(node.getSource());
            return shouldCreateFilter(columnName, buildStats, probeStats);
        }

        private boolean shouldCreateFilter(String columnName, PlanNodeStatsEstimate buildStats, PlanNodeStatsEstimate probeStats)
        {
            double buildRowCount = buildStats.getOutputRowCount();
            double probeRowCount = probeStats.getOutputRowCount();

            if (!isFinite(buildRowCount) || !isFinite(probeRowCount)) {
                return true;
            }

            double threshold = getDistributedDynamicFilterCardinalityRatioThreshold(session);
            boolean create = probeRowCount > 0 && (buildRowCount / probeRowCount) < threshold;
            if (extendedMetrics) {
                emitPlanDecisionMetric(create
                        ? DYNAMIC_FILTER_PLAN_CREATED_FAVORABLE_RATIO
                        : DYNAMIC_FILTER_PLAN_SKIPPED_HIGH_CARDINALITY, columnName);
            }
            return create;
        }

        private void emitPlanDecisionMetric(String metricName, String columnName)
        {
            session.getRuntimeStats().addMetricValue(format("%s[%s]", metricName, columnName), NONE, 1);
        }

        private static PlanNode replaceChildren(JoinNode node, PlanNode left, PlanNode right)
        {
            if (left == node.getLeft() && right == node.getRight()) {
                return node;
            }
            return new JoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    node.getType(),
                    left,
                    right,
                    node.getCriteria(),
                    node.getOutputVariables(),
                    node.getFilter(),
                    node.getLeftHashVariable(),
                    node.getRightHashVariable(),
                    node.getDistributionType(),
                    node.getDynamicFilters());
        }

        private static PlanNode replaceChildren(SemiJoinNode node, PlanNode source, PlanNode filteringSource)
        {
            if (source == node.getSource() && filteringSource == node.getFilteringSource()) {
                return node;
            }
            return new SemiJoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    source,
                    filteringSource,
                    node.getSourceJoinVariable(),
                    node.getFilteringSourceJoinVariable(),
                    node.getSemiJoinOutput(),
                    node.getSourceHashVariable(),
                    node.getFilteringSourceHashVariable(),
                    node.getDistributionType(),
                    node.getDynamicFilters());
        }
    }
}
