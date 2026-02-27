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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.MVRewriteCandidatesNode;
import com.facebook.presto.spi.plan.MVRewriteCandidatesNode.MVRewriteCandidate;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.isMaterializedViewQueryRewriteCostBasedSelectionEnabled;
import static com.facebook.presto.sql.planner.plan.Patterns.mvRewriteCandidates;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A rule that selects the lowest-cost plan from the original query and
 * all materialized view rewrite candidates. This enables cost-based
 * materialized view selection.
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
        return isMaterializedViewQueryRewriteCostBasedSelectionEnabled(session);
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
        Session session = context.getSession();

        // Calculate cost for original plan
        PlanCostEstimate originalCost = costProvider.getCost(node.getOriginalPlan());

        // Start with original plan as the selected plan
        PlanNode selectedPlan = node.getOriginalPlan();
        PlanCostEstimate lowestCost = originalCost;

        // Find lowest cost among all candidates
        for (MVRewriteCandidate candidate : node.getCandidates()) {
            PlanCostEstimate candidateCost = costProvider.getCost(candidate.getPlan());

            // Skip candidates with unknown costs
            if (candidateCost.hasUnknownComponents()) {
                continue;
            }

            // Compare costs: if candidate is cheaper, select it
            if (lowestCost.hasUnknownComponents() ||
                    costComparator.compare(session, candidateCost, lowestCost) < 0) {
                lowestCost = candidateCost;
                selectedPlan = candidate.getPlan();
            }
        }

        // If selected plan's output variables match the expected outputs, return it directly
        List<VariableReferenceExpression> expectedOutputs = node.getOutputVariables();
        List<VariableReferenceExpression> selectedOutputs = selectedPlan.getOutputVariables();

        if (expectedOutputs.equals(selectedOutputs)) {
            return Result.ofPlanNode(selectedPlan);
        }

        // Add a projection to map selected plan's outputs to expected outputs
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
