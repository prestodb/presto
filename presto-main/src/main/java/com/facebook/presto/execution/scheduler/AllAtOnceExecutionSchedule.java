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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageState;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.execution.StageState.RUNNING;
import static com.facebook.presto.execution.StageState.SCHEDULED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class AllAtOnceExecutionSchedule
        implements ExecutionSchedule
{
    private final Set<SqlStageExecution> schedulingStages;

    public AllAtOnceExecutionSchedule(Collection<SqlStageExecution> stages)
    {
        requireNonNull(stages, "stages is null");
        List<PlanFragmentId> preferredScheduleOrder = getPreferredScheduleOrder(stages.stream()
                .map(SqlStageExecution::getFragment)
                .collect(toImmutableList()));

        Ordering<SqlStageExecution> ordering = Ordering.explicit(preferredScheduleOrder)
                .onResultOf(PlanFragment::getId)
                .onResultOf(SqlStageExecution::getFragment);
        schedulingStages = new LinkedHashSet<>(ordering.sortedCopy(stages));
    }

    @Override
    public Set<SqlStageExecution> getStagesToSchedule()
    {
        for (Iterator<SqlStageExecution> iterator = schedulingStages.iterator(); iterator.hasNext(); ) {
            StageState state = iterator.next().getState();
            if (state == SCHEDULED || state == RUNNING || state.isDone()) {
                iterator.remove();
            }
        }
        return schedulingStages;
    }

    @Override
    public boolean isFinished()
    {
        return schedulingStages.isEmpty();
    }

    @VisibleForTesting
    static List<PlanFragmentId> getPreferredScheduleOrder(Collection<PlanFragment> fragments)
    {
        // determine output fragment
        Set<PlanFragmentId> remoteSources = fragments.stream()
                .map(PlanFragment::getRemoteSourceNodes)
                .flatMap(Collection::stream)
                .map(RemoteSourceNode::getSourceFragmentIds)
                .flatMap(Collection::stream)
                .collect(toImmutableSet());

        Set<PlanFragment> rootFragments = fragments.stream()
                .filter(fragment -> !remoteSources.contains(fragment.getId()))
                .collect(toImmutableSet());
        checkArgument(rootFragments.size() == 1, "Expected one root fragment, but found: " + rootFragments);

        Visitor visitor = new Visitor(fragments);
        visitor.processFragment(getOnlyElement(rootFragments).getId());

        return visitor.getSchedulerOrder();
    }

    private static class Visitor
            extends PlanVisitor<Void, Void>
    {
        private final Map<PlanFragmentId, PlanFragment> fragments;
        private final ImmutableSet.Builder<PlanFragmentId> schedulerOrder = ImmutableSet.builder();

        public Visitor(Collection<PlanFragment> fragments)
        {
            this.fragments = fragments.stream()
                    .collect(toImmutableMap(PlanFragment::getId, identity()));
        }

        public List<PlanFragmentId> getSchedulerOrder()
        {
            return ImmutableList.copyOf(schedulerOrder.build());
        }

        public void processFragment(PlanFragmentId planFragmentId)
        {
            PlanFragment planFragment = fragments.get(planFragmentId);
            checkArgument(planFragment != null, "Fragment not found: " + planFragmentId);

            planFragment.getRoot().accept(this, null);
            schedulerOrder.add(planFragmentId);
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            node.getRight().accept(this, context);
            node.getLeft().accept(this, context);
            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Void context)
        {
            node.getFilteringSource().accept(this, context);
            node.getSource().accept(this, context);
            return null;
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Void context)
        {
            node.getProbeSource().accept(this, context);
            node.getIndexSource().accept(this, context);
            return null;
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Void context)
        {
            node.getSourceFragmentIds()
                    .forEach(this::processFragment);
            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Void context)
        {
            for (PlanNode subPlanNode : node.getSources()) {
                subPlanNode.accept(this, context);
            }

            return null;
        }

        @Override
        public Void visitExchange(ExchangeNode node, Void context)
        {
            for (PlanNode subPlanNode : node.getSources()) {
                subPlanNode.accept(this, context);
            }
            return null;
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            List<PlanNode> sources = node.getSources();
            if (sources.isEmpty()) {
                return null;
            }
            if (sources.size() == 1) {
                sources.get(0).accept(this, context);
                return null;
            }
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }
    }
}
