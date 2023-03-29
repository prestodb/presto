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

package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.eventlistener.PlanOptimizerInformation;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.MergeJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.jsonFragmentPlan;

public class MergeJoinWithGroupedExecutionOptimizer
{
    private MergeJoinWithGroupedExecutionOptimizer() {}

    public static SubPlan optimizeMergeJoinWithGroupedExecution(Session session, SubPlan subPlan, Metadata metadata)
    {
        if (!subPlan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution()) {
            return subPlan;
        }
        PlanNode original = subPlan.getFragment().getRoot();

        PlanNode rewritten = SimplePlanRewriter.rewriteWith(new Rewriter(), original);
        if (rewritten == original) {
            return subPlan;
        }

        session.getOptimizerInformationCollector().addInformation(new PlanOptimizerInformation(MergeJoinWithGroupedExecutionOptimizer.class.getSimpleName(), true, Optional.empty()));
        PlanFragment originalFragment = subPlan.getFragment();

        return new SubPlan(
                new PlanFragment(
                        originalFragment.getId(),
                        rewritten,
                        originalFragment.getVariables(),
                        originalFragment.getPartitioning(),
                        originalFragment.getTableScanSchedulingOrder(),
                        originalFragment.getPartitioningScheme(),
                        originalFragment.getStageExecutionDescriptor(),
                        originalFragment.isOutputTableWriterFragment(),
                        originalFragment.getStatsAndCosts(),
                        Optional.of(jsonFragmentPlan(rewritten, originalFragment.getVariables(), originalFragment.getStatsAndCosts(), metadata.getFunctionAndTypeManager(), session))),
                subPlan.getChildren());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        @Override
        public PlanNode visitMergeJoin(MergeJoinNode node, RewriteContext<Void> rewriteContext)
        {
            if (canRemoveMergingExchange(node.getLeft()) && canRemoveMergingExchange(node.getRight())) {
                return node.replaceChildren(ImmutableList.of(node.getLeft().getSources().get(0), node.getRight().getSources().get(0)));
            }
            return node;
        }

        private boolean canRemoveMergingExchange(PlanNode node)
        {
            return isMergingExchange(node) && node.getSources().size() == 1 && !containsSortOrExchange(node.getSources().get(0));
        }

        private boolean containsSortOrExchange(PlanNode planNode)
        {
            return PlanNodeSearcher.searchFrom(planNode)
                    .whereIsInstanceOfAny(ImmutableList.of(SortNode.class, ExchangeNode.class))
                    .findFirst()
                    .isPresent();
        }

        private boolean isMergingExchange(PlanNode node)
        {
            return node instanceof ExchangeNode && ((ExchangeNode) node).getType() == ExchangeNode.Type.GATHER && ((ExchangeNode) node).getOrderingScheme().isPresent();
        }
    }
}
