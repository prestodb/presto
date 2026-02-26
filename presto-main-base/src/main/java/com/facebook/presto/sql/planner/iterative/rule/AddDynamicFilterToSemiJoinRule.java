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
import static com.facebook.presto.SystemSessionProperties.getDistributedDynamicFilterStrategy;
import static com.facebook.presto.SystemSessionProperties.isDistributedDynamicFilterEnabled;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.DistributedDynamicFilterStrategy.COST_BASED;
import static com.facebook.presto.sql.planner.iterative.rule.AddDynamicFilterRule.resolveProbeColumnMapping;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;
import static java.lang.Double.isFinite;
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

    public AddDynamicFilterToSemiJoinRule(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
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
        double cardinalityRatioThreshold = getDistributedDynamicFilterCardinalityRatioThreshold(session);
        long discreteValuesLimit = getDistributedDynamicFilterDiscreteValuesLimit(session);

        PlanNodeStatsEstimate buildStats = context.getStatsProvider().getStats(node.getFilteringSource());
        PlanNodeStatsEstimate probeStats = context.getStatsProvider().getStats(node.getSource());

        double buildRowCount = buildStats.getOutputRowCount();
        double probeRowCount = probeStats.getOutputRowCount();

        if (!isFinite(buildRowCount) || !isFinite(probeRowCount)) {
            return isSplitFilteringColumn(node.getSourceJoinVariable(), node.getSource(), context);
        }

        VariableStatsEstimate buildVarStats = buildStats.getVariableStatistics(node.getFilteringSourceJoinVariable());
        double buildNdv = buildVarStats.getDistinctValuesCount();
        if (isFinite(buildNdv) && buildNdv <= discreteValuesLimit) {
            return true;
        }

        return probeRowCount > 0 && (buildRowCount / probeRowCount) < cardinalityRatioThreshold;
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
