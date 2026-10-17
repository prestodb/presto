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
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.MVRewriteCandidatesNode;
import com.facebook.presto.spi.plan.MVRewriteCandidatesNode.MVRewriteCandidate;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.getMaterializedViewIncrementalRefreshStrategy;
import static com.facebook.presto.SystemSessionProperties.getMaterializedViewStitchingStrategy;
import static com.facebook.presto.SystemSessionProperties.isMaterializedViewQueryRewriteCostBasedSelectionEnabled;
import static com.facebook.presto.common.RuntimeMetricName.MATERIALIZED_VIEW_ROW_LEVEL_STITCHING_APPLIED;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.spi.StandardWarningCode.MATERIALIZED_VIEW_ROW_LEVEL_REJECTED_ON_COST;
import static com.facebook.presto.sql.planner.iterative.rule.materializedview.MaterializedViewRewriteStrategy.AUTOMATIC;
import static com.facebook.presto.sql.planner.plan.Patterns.mvRewriteCandidates;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Selects the lowest-cost plan among the original query and
 * all materialized view rewrite candidates.
 */
public class SelectLowestCostMVRewrite
        implements Rule<MVRewriteCandidatesNode>
{
    private static final Pattern<MVRewriteCandidatesNode> PATTERN = mvRewriteCandidates();

    private final CostComparator costComparator;

    public SelectLowestCostMVRewrite(CostComparator costComparator)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
    }

    @Override
    public Pattern<MVRewriteCandidatesNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isMaterializedViewQueryRewriteCostBasedSelectionEnabled(session)
                || getMaterializedViewStitchingStrategy(session) == AUTOMATIC
                || getMaterializedViewIncrementalRefreshStrategy(session) == AUTOMATIC;
    }

    @Override
    public boolean isCostBased(Session session)
    {
        return true;
    }

    @Override
    public Result apply(MVRewriteCandidatesNode node, Captures captures, Context context)
    {
        CostProvider costProvider = context.getCostProvider();
        StatsProvider statsProvider = context.getStatsProvider();
        Session session = context.getSession();
        Lookup lookup = context.getLookup();

        PlanNode selectedPlan = node.getOriginalPlan();
        PlanCostEstimate lowestCost = costProvider.getCost(selectedPlan);
        // Null while the original plan is still winning, i.e. no candidate has beaten a full
        // recompute yet.
        MVRewriteCandidate selectedCandidate = null;

        for (MVRewriteCandidate candidate : node.getCandidates()) {
            PlanCostEstimate candidateCost = costProvider.getCost(candidate.getPlan());

            if (!candidateCost.hasUnknownComponents() && !lowestCost.hasUnknownComponents()) {
                if (costComparator.compare(session, candidateCost, lowestCost) < 0) {
                    lowestCost = candidateCost;
                    selectedPlan = candidate.getPlan();
                    selectedCandidate = candidate;
                }
            }
            else {
                // Fall back to row count when cost has unknown components (e.g. partial aggregation)
                double candidateRows = statsProvider.getStats(candidate.getPlan()).getOutputRowCount();
                double selectedRows = statsProvider.getStats(selectedPlan).getOutputRowCount();
                if (!Double.isNaN(candidateRows) && (Double.isNaN(selectedRows) || candidateRows < selectedRows)) {
                    lowestCost = candidateCost;
                    selectedPlan = candidate.getPlan();
                    selectedCandidate = candidate;
                }
            }
        }

        // A row-level candidate that was offered and not chosen is the cost picker working, not a
        // capability gap, so it gets its own code: without one, a view that never refreshes
        // row-level looks the same whether it was ineligible or merely more expensive.
        boolean rowLevelOffered = node.getCandidates().stream().anyMatch(MVRewriteCandidate::isRowLevel);
        if (selectedCandidate != null && selectedCandidate.isRowLevel()) {
            context.getSession().getRuntimeStats()
                    .addMetricValue(MATERIALIZED_VIEW_ROW_LEVEL_STITCHING_APPLIED, NONE, 1);
        }
        if (rowLevelOffered && (selectedCandidate == null || !selectedCandidate.isRowLevel())) {
            context.getWarningCollector().add(new PrestoWarning(
                    MATERIALIZED_VIEW_ROW_LEVEL_REJECTED_ON_COST,
                    "Row-level incremental refresh was available for materialized view " +
                            node.getCandidates().stream()
                                    .filter(MVRewriteCandidate::isRowLevel)
                                    .map(MVRewriteCandidate::getFullyQualifiedName)
                                    .findFirst()
                                    .orElse("<unknown>") +
                            " and was not chosen because another plan cost less."));
        }

        // Resolve GroupReference since Memo wraps children as GroupReferences
        if (selectedPlan instanceof GroupReference) {
            List<PlanNode> resolved = lookup.resolveGroup(selectedPlan).collect(toList());
            checkArgument(resolved.size() == 1, "Expected exactly one resolved plan node, got %s", resolved.size());
            selectedPlan = resolved.get(0);
        }

        List<VariableReferenceExpression> expectedOutputs = node.getOutputVariables();
        List<VariableReferenceExpression> selectedOutputs = selectedPlan.getOutputVariables();

        if (expectedOutputs.equals(selectedOutputs)) {
            return Result.ofPlanNode(selectedPlan);
        }

        // Add projection to remap output variables
        checkArgument(expectedOutputs.size() == selectedOutputs.size(),
                "Expected %s output variables but selected plan has %s",
                expectedOutputs.size(), selectedOutputs.size());

        Assignments.Builder assignments = Assignments.builder();
        for (int i = 0; i < expectedOutputs.size(); i++) {
            VariableReferenceExpression expectedVar = expectedOutputs.get(i);
            VariableReferenceExpression selectedVar = selectedOutputs.get(i);
            assignments.put(expectedVar, selectedVar);
        }

        ProjectNode projectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                selectedPlan,
                assignments.build());

        return Result.ofPlanNode(projectNode);
    }
}
